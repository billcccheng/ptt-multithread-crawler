#!/usr/bin/python
# coding=utf-8
import json
import re
import requests
from bs4 import BeautifulSoup
from six import u

def parse(link , data_to_store):
    print link
    resp = requests.get(url=link, cookies={'over18': '1'}, verify=False)
    if resp.status_code != 200:
        print('invalid url:', resp.url)
        return json.dumps({"error": "invalid url"}, sort_keys=True, ensure_ascii=False)
    soup = BeautifulSoup(resp.text, 'html.parser')
    main_content = soup.find(id="main-content")
    metas = main_content.select('div.article-metaline')
    author, title, date = '', '', ''
    if metas:
        _author=  metas[0].select('span.article-meta-value')[0]
        _title =  metas[1].select('span.article-meta-value')[0]
        _date  =  metas[2].select('span.article-meta-value')[0]
        author =  _author.string if _author else author
        title  =  _title.string if _title else title
        date   =  _date.string if _date else date

        # remove meta nodes
        for meta in metas:
            meta.extract()
        for meta in main_content.select('div.article-metaline-right'):
            meta.extract()

    # remove and keep push nodes
    pushes = main_content.find_all('div', class_='push')
    for push in pushes:
        push.extract()

    try:
        ip = main_content.find(text=re.compile(u'※ 發信站:'))
        ip = re.search('[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*', ip).group()
    except:
        ip = ""

    filtered = [ v for v in main_content.stripped_strings if v[0] not in [u'※', u'◆'] and v[:2] not in [u'--'] ]
    expr = re.compile(u(r'[^\u4e00-\u9fa5\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\s\w:/-_.?~%()]'))
    for i in range(len(filtered)):
        filtered[i] = re.sub(expr, '', filtered[i])

    filtered = [_f for _f in filtered if _f]  # remove empty strings
    content = ' '.join(filtered)
    content = re.sub(r'(\s)+', ' ', content)
    messages = []
    for push in pushes:
        if not push.find('span', 'push-tag'):
            continue
        push_userid = push.find('span', 'push-userid').string.strip(' \t\n\r')
        # if find is None: find().strings -> list -> ' '.join; else the current way
        push_content = push.find('span', 'push-content').strings
        push_content = ' '.join(push_content)[1:].strip(' \t\n\r')  # remove ':'
        push_ipdatetime = push.find('span', 'push-ipdatetime').string.strip(' \t\n\r')
        messages.append(push_userid + ":" + push_content)

    data = {
        'title': title,
        'link': link,
        'author': author,
        'date': date,
        'content': content,
        'ip': ip,
        'messages': " ".join(messages)
    }
    data_to_store.append(json.dumps(data, sort_keys=False, ensure_ascii=False)+",")

