import os


class Pipe:
    """pipe results to stdout, files or Kafka"""

    def __init__(self):
        self.text = []

    def read_from_file(self, path=os.getcwd(), filename='default', ext='csv'):
        path = path + '/__data__/' + filename + '.' + ext
        if not os.path.isfile(path):
            return []

        with open(path, 'r') as f:
            self.text = f.readlines()
        return self.text

    def read_from_text(self, text):
        if isinstance(text, list) is False:
            text = text.split('\n')
        self.text = text
        return self.text

    def read_from_downloader(self, text=''):
        self.text = text.split('\n')
        return self.text

    def write_to_stdout(self):
        self.raise_error()
        print(self.text)

    def write_to_file(self, path=os.getcwd(), filename='default', ext='csv'):
        self.raise_error()
        path = path + '/__data__/' + filename + '.' + ext

        with open(path, 'a') as f:
            for line in self.text[:]:
                f.write(line)

    def raise_error(self):
        if self.text == []:
            raise AttributeError("Pipe object has no attribute 'text'")

    def clear(self):
        self.text = []
