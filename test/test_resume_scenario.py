
import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Add src to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from pyobs.core import StreamUploader, UploadContext

class TestResumeScenario(unittest.TestCase):

    @patch('pyobs.core.ObsClient')
    def test_wb_interrupted_then_ab_resumes(self, MockObsClient):
        """
        Scenario:
        1. User runs with mode="wb".
           - Should abort any existing upload.
           - Should initiate a new upload.
           - Uploads some parts.
           - Interrupted.
        2. User runs with mode="ab".
           - Should find the existing upload created in step 1.
           - Should resume from offset.
           - Should NOT create a new upload ID.
        """
        mock_client = MockObsClient.return_value

        # Shared state between calls
        obs_state = {
            "uploads": {}, # upload_id -> {part_num: etag}
            "upload_meta": {} # upload_id -> key
        }
        id_counter = [0] # List to be mutable in closures

        # --- Mock Implementations ---

        def initiate_multipart_upload(bucket, objectKey):
            id_counter[0] += 1
            new_id = f"upload_id_{id_counter[0]}"
            obs_state["uploads"][new_id] = {}
            obs_state["upload_meta"][new_id] = objectKey

            resp = MagicMock()
            resp.status = 200
            resp.body.uploadId = new_id
            return resp

        def list_multipart_uploads(bucket, multipart):
            # multipart is the request object
            prefix = multipart.prefix
            uploads_list = []

            # Find uploads matching prefix (key)
            for uid, key in obs_state["upload_meta"].items():
                if key == prefix:
                    upload_obj = MagicMock()
                    upload_obj.key = key
                    upload_obj.uploadId = uid
                    upload_obj.initiated = "time"
                    uploads_list.append(upload_obj)

            resp = MagicMock()
            resp.status = 200
            resp.body.upload = uploads_list
            return resp

        def abort_multipart_upload(bucket, objectKey, uploadId):
            if uploadId in obs_state["uploads"]:
                del obs_state["uploads"][uploadId]
                del obs_state["upload_meta"][uploadId]
            resp = MagicMock()
            resp.status = 204
            return resp

        def upload_part(bucketName, objectKey, partNumber, uploadId, content, partSize):
            if uploadId not in obs_state["uploads"]:
                resp = MagicMock()
                resp.status = 404
                return resp

            obs_state["uploads"][uploadId][partNumber] = "etag"
            resp = MagicMock()
            resp.status = 200
            resp.body.etag = "etag"
            return resp

        def list_parts(bucket, key, uploadId, partNumberMarker=None):
            parts_dict = obs_state["uploads"].get(uploadId, {})
            parts_list = []
            for pnum in sorted(parts_dict.keys()):
                part = MagicMock()
                part.partNumber = pnum
                # We assume 512KB part size as per our configuration below
                part.size = 512 * 1024
                part.etag = parts_dict[pnum]
                parts_list.append(part)

            resp = MagicMock()
            resp.status = 200
            resp.body.parts = parts_list
            resp.body.isTruncated = False
            resp.body.nextPartNumberMarker = None
            return resp

        def complete_multipart_upload(bucket, key, uploadId, completeMultipartUploadRequest):
             resp = MagicMock()
             resp.status = 200
             return resp

        # Attach side effects
        mock_client.initiateMultipartUpload.side_effect = initiate_multipart_upload
        mock_client.listMultipartUploads.side_effect = list_multipart_uploads
        mock_client.abortMultipartUpload.side_effect = abort_multipart_upload
        mock_client.uploadPart.side_effect = upload_part
        mock_client.listParts.side_effect = list_parts
        mock_client.completeMultipartUpload.side_effect = complete_multipart_upload

        # Initialize Uploader with small part size for testing
        uploader = StreamUploader(
            ak="ak", sk="sk", server="server", bucket_name="test_bucket",
            part_size=512 * 1024 # 512KB for testing
        )
        # Manually reduce MAX_WORKERS to ensure sequential submission if needed,
        # or just rely on thread pool.

        key = "test.file"

        # --- PHASE 1: WB + Interrupt ---

        # 1. Init
        context1 = uploader.init_upload(key)
        # Should start new upload (id_1)
        self.assertEqual(context1.offset, 0)
        initial_id = context1.upload_id

        # 2. Upload with WB and interrupt
        def stream_gen_interrupt():
            # Send enough for 1 part (512KB) + some buffer
            yield b"x" * (600 * 1024)
            raise RuntimeError("Interrupt!")

        try:
            # Must provide total_size to avoid auto-bump to 150MB
            uploader.upload_stream(
                context1,
                stream_gen_interrupt(),
                total_size=10 * 1024 * 1024,
                mode="wb"
            )
        except RuntimeError:
            pass

        # Verify Phase 1 outcome
        # initial_id should be aborted because we used WB (it starts FRESH)
        # The logic: init_upload -> id_1. upload_stream(wb) -> abort(id_1) -> init -> id_2.
        self.assertNotIn(initial_id, obs_state["uploads"])

        # There should be ONE active upload (id_2)
        active_ids = list(obs_state["uploads"].keys())
        self.assertEqual(len(active_ids), 1)
        wb_id = active_ids[0]
        self.assertNotEqual(wb_id, initial_id)

        # It should have parts uploaded (at least part 1)
        self.assertTrue(len(obs_state["uploads"][wb_id]) > 0)

        # --- PHASE 2: AB (Resume) ---

        # 1. Init
        context2 = uploader.init_upload(key)

        # Should find wb_id (the one active on OBS)
        self.assertEqual(context2.upload_id, wb_id)
        # Should have offset > 0 (size of part 1)
        self.assertGreater(context2.offset, 0)

        # 2. Upload with AB
        def stream_gen_resume():
            yield b"y" * 100

        final_size = uploader.upload_stream(
            context2,
            stream_gen_resume(),
            total_size=10 * 1024 * 1024,
            mode="ab"
        )

        # Verify completed called with wb_id
        mock_client.completeMultipartUpload.assert_called()
        # Check arguments of the LAST call
        call_args = mock_client.completeMultipartUpload.call_args
        # args: (bucket, key, uploadId, request)
        self.assertEqual(call_args[0][2], wb_id)

if __name__ == '__main__':
    unittest.main()
