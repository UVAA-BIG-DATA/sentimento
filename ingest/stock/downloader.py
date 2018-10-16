import requests


class Downloader:
    """download stock data from alphavantage.co"""

    def __init__(self, url='https://www.alphavantage.co/query', params={}):
        self.url = url
        self.payload = params
        self.session = requests.Session()

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
