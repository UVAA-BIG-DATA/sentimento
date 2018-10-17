import os
import getopt
import yaml
from downloader import Downloader
from pipe import Pipe
from utils import dedup, clean


def loadConfig():
    yml = ''
    with open('company.yaml', 'r') as f:
        yml = f.read()
    return yaml.load(yml)

if __name__ == '__main__':
    obj = loadConfig()
    dl = Downloader()

    for company in obj['company']:
        payload = {
            'function': 'TIME_SERIES_INTRADAY',
            'interval': obj['interval'],
            'symbol': company,
            'apikey': 'M1PAJKCE6DZUZAUS',
            'datatype': obj['datatype']
        }
        pipe = Pipe()

        new_text = pipe.read_from_downloader(text=dl.addParams(payload).bulk())
        old_text = pipe.read_from_file(filename=company, ext=obj['datatype'])

        new_text = dedup(clean(new_text + old_text))

        pipe.read_from_text(new_text)
        pipe.write_to_file(filename=company, ext=obj['datatype'])

        pipe.clear()

    dl.close()
