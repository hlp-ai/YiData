import os

from utils import download, ungzip

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

    # read WET files
    wetf = open(cc_wet_paths_path, encoding="utf-8")
    wet_paths = [cc_base_url+line.strip() for line in wetf]  # 绝对化URL

    return wet_paths


if __name__ == "__main__":
    cc_id = "CC-MAIN-2025-05"
    get_wet_paths(cc_id)
