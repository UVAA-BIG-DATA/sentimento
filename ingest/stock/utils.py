def dedup(text):
    if isinstance(text, list) is False:
        text = text.split('\n')
    final_list = []
    for line in text[:]:
        if line not in final_list:
            final_list.append(line)
    return final_list


def clean(text):
    if isinstance(text, list) is False:
        text = text.split('\n')
    text = filter(lambda line: '{' not in line and '}' not in line, text)
    return list(text)