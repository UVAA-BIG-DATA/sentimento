def dedup(text):
    if isinstance(text, list) is False:
        text = text.split('\n')
    return list(set(text))


def clean(text):
    if isinstance(text, list) is False:
        text = text.split('\n')
    text = filter(lambda line: 'Information' not in line and '{' not in line, text)
    return [line.strip() for line in list(text)]


def sort(text):
    if isinstance(text, list) is False:
        text = text.split('\n')


def appendCol(text, colname=''):
    if isinstance(text, list) is False:
        text = text.split('\n')
    return ['%s,%s' % (colname, line) for line in text[:]]