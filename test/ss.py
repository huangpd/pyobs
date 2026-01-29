from pyobs import StreamUploader
import requests
import time
# 1. 初始化 SDK
uploader = StreamUploader(
    server="obs.cn-east-3.myhuaweicloud.com",
    bucket_name="sais-craw-v2"
)
t = time.time()
context = uploader.init_upload(object_key="test/female_spleen.1.bam.bw")

print(f"-> SDK 建议从 {context.offset} 字节处开始下载")
uploader.abort_upload("test/female_spleen.1.bam.bw")
# 2. 根据 offset 发起下载
headers = {
    "Range": f"bytes={context.offset}-"  # <--- 关键
}

# stream=True 流式下载
response = requests.get("https://ftp.ebi.ac.uk/ensemblorg/pub/grch37/release-110/data_files/oncorhynchus_mykiss/Omyk_1.0/rnaseq/Omyk_1.0.ENA.female_spleen.1.bam.bw", headers=headers, stream=True)


# 3. 把流扔回给 SDK
uploader.upload_stream(
    mode='ab',
    context=context,
    stream_iterator=response.iter_content(chunk_size=10*1024*1024),
    total_size = int(response.headers.get("Content-Length",306184192))
)
print(time.time()-t)