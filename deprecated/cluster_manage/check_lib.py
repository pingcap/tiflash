if __name__ == '__main__':
    try:
        import dns
        import requests
        import uri
        import urllib3
        import toml
        import pybind11
        import setuptools
        import etcd3
    except Exception as e:
        print(e)
        exit(-1)
    exit(0)
