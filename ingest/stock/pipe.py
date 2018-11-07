import os


class Pipe:
    """pipe results to stdout, files or Kafka"""

    def __init__(self):
        self.fields = ['company']
        self.text = []

    def read_from_file(self, path=os.getcwd(), filename='default', ext='csv'):
        path = path + '/__data__/' + filename + '.' + ext
        if not os.path.isfile(path):
            return []

        with open(path, 'r') as f:
            self.text = f.readlines()
        self.fields = self.text[0].split(',')
        self.text.remove(self.text[0])
        return self.text

    def read_from_text(self, text):
        if isinstance(text, list) is False:
            text = text.strip().split('\n')
        self.text = text
        return self.text

    def read_from_downloader(self, text=''):
        self.text = text.strip().split('\n')
        self.fields = self.fields[0:1]
        self.fields += self.text[0].split(',')
        self.text.remove(self.text[0])
        return self.text

    def write_to_stdout(self):
        self.raise_error()
        print(self.text)

    def write_to_file(self, path=os.getcwd(), filename='default', ext='csv'):
        self.raise_error()
        path = path + '/__data__/' + filename + '.' + ext

        with open(path, 'w') as f:
            f.write(','.join(self.fields))
            for line in self.text[:]:
                f.write(line + '\n')

    def raise_error(self):
        if self.text == []:
            raise AttributeError("Pipe object has no attribute 'text'")

    def clear(self):
        self.text = []
