import requests


class Downloader:
    """download stock data from alphavantage.co"""

    def __init__(self, url='https://www.alphavantage.co/query'):
        self.url = url
        self.session = requests.Session()
        self.params = {}

    def addParams(self, params={}):
        self.payload = params
        return self

    def stream(self):
        self.payload['outputsize'] = 'compact'
        r = self.session.get(self.url, params=self.payload)
        return r.text

    def bulk(self):
        self.payload['outputsize'] = 'full'
        r = self.session.get(self.url, params=self.payload)
        return r.text

    def close(self):
        self.session.close()

    def raise_error(self):
        if self.params == {}:
            raise AttributeError("Downloader object has no attribute 'params'")
