import yaml
from downloader import Downloader
from pipe import Pipe

if __name__ == '__main__':
    yml = ''
    with open('company.yaml', 'r') as f:
        yml = f.read()
    obj = yaml.load(yml)

    for company in obj['company']:
        payload = {
            'function': 'TIME_SERIES_INTRADAY',
            'interval': obj['interval'],
            'symbol': company,
            'apikey': 'M1PAJKCE6DZUZAUS',
            'datatype': obj['datatype']
        }

        dl = Downloader(params=payload)
        pipe = Pipe()
        pipe.read_from_downloader(text=dl.stream())
        pipe.write_to_file(filename=company, ext=obj['datatype'])
        pipe.clear()

    dl.close()
