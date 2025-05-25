import gzip

import requests
from tqdm import tqdm


def ungzip(zip_fn, unzip_fn):
    """解压gz文件"""
    g = gzip.GzipFile(mode="rb", fileobj=open(zip_fn, 'rb'))
    with open(unzip_fn, "wb") as f:
        f.write(g.read())


def download(url, local_fn):
    """下载url到本地文件"""
    hd = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36"}
    response = requests.request('GET', url, headers=hd)
    if response.ok:
        open(local_fn, 'wb').write(response.content)
        return True
    else:
        return False


def download_progress(url, filename):
    """带进度条的下载"""
    # 发起GET请求，以流式方式获取数据
    response = requests.get(url, stream=True)

    # 获取文件总大小，用于设置进度条
    total_size = int(response.headers.get('content-length', 0))

    # 使用tqdm创建进度条
    with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as bar:
        # 以二进制写入模式打开文件
        with open(filename, 'wb') as file:
            # 按块读取数据
            for data in response.iter_content(chunk_size=1024):
                # 写入数据
                file.write(data)
                # 更新进度条
                bar.update(len(data))
