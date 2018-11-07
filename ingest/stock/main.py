import os
import getopt
import yaml
from downloader import Downloader
from pipe import Pipe
from producer import produce
from utils import dedup, clean, appendCol


def loadConfig():
    yml = ''
    with open('../company.yaml', 'r') as f:
        yml = f.read()
    return yaml.load(yml)

if __name__ == '__main__':
    obj = loadConfig()
    dl = Downloader()

    for company in obj['company']:
        print('...', end=' ')
        payload = {
            'function': 'TIME_SERIES_INTRADAY',
            'interval': obj['interval'],
            'symbol': company[0],
            'apikey': 'M1PAJKCE6DZUZAUS',
            'datatype': obj['datatype']
        }
        pipe = Pipe()

        new_text = appendCol(
            clean(
                pipe.read_from_downloader(text=dl.addParams(payload).bulk())
            ),
            colname=company[1]
        )

        old_text = pipe.read_from_file(
            filename=company[1],
            ext=obj['datatype']
        )

        pipe.read_from_text(new_text + old_text)
        pipe.write_to_file(filename=company[1], ext=obj['datatype'])

        pipe.clear()
        print('Finish pulling stock data: %s' % company[1])

    dl.close()