import os

from warcio import ArchiveIterator

from utils import download, ungzip, download_progress

cc_data_url = "https://data.commoncrawl.org/crawl-data/"
cc_base_url = "https://data.commoncrawl.org/"

cc_wet_paths_gz = "wet.paths.gz"  # WET文件路径列表文件
cc_wet_paths = "wet.paths"  # 解压WET文件路径列表文件


def get_wet_paths(cc_archive_id, out_dir=None):
    """获得所有WET文件链接"""
    wet_paths_url = cc_data_url + cc_archive_id + "/" + cc_wet_paths_gz
    if out_dir is None:
        out_dir = "./" + cc_archive_id + "/"
    else:
        out_dir = os.path.join(out_dir, cc_archive_id)
    if not os.path.exists(out_dir):
        print("创建目录: ", out_dir)
        os.makedirs(out_dir, exist_ok=True)

    # download
    cc_wet_paths_gz_path = os.path.join(out_dir, cc_wet_paths_gz)
    print("下载WET路径列表文件...")
    if not download(wet_paths_url, cc_wet_paths_gz_path):
        print("ERROR: Fail to download", wet_paths_url)
        return []

    # unzip
    print("解压WET路径列表文件...")
    cc_wet_paths_path = os.path.join(out_dir, cc_wet_paths)
    ungzip(cc_wet_paths_gz_path, cc_wet_paths_path)


def get_wet_name(wet_url):
    parts = wet_url.split("/")
    gz_path = parts[-1]
    idx = gz_path.find(".gz")
    return gz_path, gz_path[:idx]


def get_text(wet_url, lang):
    wet_gz_name, wet_name = get_wet_name(wet_url)
    print("下载WET文件...")
    download_progress(wet_url, wet_gz_name)

    print("解压WET文件...")
    ungzip(wet_gz_name, wet_name)

    with open(wet_name, 'rb') as stream:
        total_pages = 0
        found = 0
        docs = {}
        for record in ArchiveIterator(stream):
            if record.rec_type == 'conversion' and record.rec_headers.get_header('Content-Type').find('text/')==0:
                total_pages += 1
                langs = record.rec_headers.get_header("WARC-Identified-Content-Language")
                url = record.rec_headers.get_header("WARC-Target-URI")
                content_len = int(record.rec_headers.get_header("Content-Length"))
                if langs is not None:
                    langs = langs.split(",")
                    content = record.content_stream().read().decode("utf-8")
                    if lang==langs[0]:
                        found += 1
                        print("{}/{}".format(found, total_pages))
                        docs[url] = content

        return docs


if __name__ == "__main__":
    # cc_id = "CC-MAIN-2025-05"
    # get_wet_paths(cc_id)

    wet_url = cc_base_url + "crawl-data/CC-MAIN-2025-05/segments/1736703361941.29/wet/CC-MAIN-20250126135402-20250126165402-00000.warc.wet.gz"
    docs = get_text(wet_url, "zho")
    print(docs)
