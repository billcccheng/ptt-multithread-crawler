#coding=utf-8 
import re
import sys
import json
import requests
import threading
import os
import random
import codecs
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup  
from six import u
requests.packages.urllib3.disable_warnings()
rs=requests.session()

def page_count(PTT_board):
    res = rs.get('https://www.ptt.cc/bbs/'+PTT_board+'/index.html',verify=False)
    soup = BeautifulSoup(res.text,'html.parser')
    all_page_url = soup.select('.btn.wide')[1]['href']
    all_page=int(get_page_num(all_page_url))+1
    return  all_page 

def parse_link(link , data_to_store):
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
        'messages': messages
    }
    data_to_store.append(json.dumps(data, sort_keys=False, ensure_ascii=False)+",")

def crawler(PTT_board, begin, end, thread_number, data):
    for number in range(begin, end, -1):
        _url = 'https://www.ptt.cc/bbs/'+PTT_board+'/index'+str(number)+'.html'
        res=rs.get(_url,verify=False)
        soup = BeautifulSoup(res.text,'html.parser')
        data = []
        for tag in soup.select('div.title'):
            try:
                atag = tag.find('a')
                time = random.uniform(1, 10)/5
                sleep(time)
                if(atag):
                    URL=atag['href'].strip()   
                    link='https://www.ptt.cc'+URL
                    parse_link(link, data)                     
            except Exception, err:
                print '\033[91m'+ str(err) + '\033[0m'
        store_file(data, thread_number, PTT_board)

def store_file(data, thread_number, PTT_board):
    print '\033[93m' + "Storing Thread-" + thread_number + '\033[0m'
    if not os.path.exists(PTT_board):
        os.makedirs(PTT_board)
    FILENAME = PTT_board + '/data-' + thread_number + '.json'
    with codecs.open(FILENAME, 'a', encoding='utf-8') as f:
        if os.stat(FILENAME).st_size == 0:
            f.write("[")
        f.write("\n".join(data))

def remove(value, deletechars):
    for c in deletechars:
        value = value.replace(c,'')
    return value.rstrip();

def get_page_num(content) :
    start_index = content.find('index')
    end_index = content.find('.html')
    page_num = content[start_index+5 : end_index]
    return page_num

def group_by(n_list):
    grouped_list = []
    for i in range(len(n_list) - 1):
        start, end = n_list[i], n_list[i+1]
        grouped_list.append([start, end])

    # Last divided pages
    grouped_list.append([grouped_list[-1][-1], 0])
    return grouped_list

def add_brackets(PTT_board):
    cwd = os.getcwd()
    for _file in os.listdir(cwd + '/' + PTT_board):
        if(_file == ".DS_Store" or _file == "*.swp"):
            continue
        _file = cwd + '/' + PTT_board + '/' + _file
        with open(_file, 'a') as my_file:
            if os.stat(_file).st_size == 0:
                my_file.write("[")
            my_file.seek(-1, os.SEEK_END)
            my_file.truncate()
            my_file.write("]") 

class myThread(threading.Thread):
    def __init__(self, PTT_board, begin, end, thread_number):
        threading.Thread.__init__(self)
        self.PTT_board = PTT_board
        self.begin = begin
        self.end = end
        self.thread_number = thread_number
        self.data = list()
    def run(self):
        crawler(self.PTT_board, self.begin, self.end, self.thread_number, self.data)

if __name__ == "__main__":  
    PTT_board = str(sys.argv[1]).lower() 
    thread_num = int(sys.argv[2])
    print 'Start parsing [',PTT_board,']....'
    all_page = page_count(PTT_board)
    divide_pages = [x for x in range(all_page, 0, -all_page/thread_num)]
    divide_pages_grouped = group_by(divide_pages)
    print divide_pages_grouped

    # Create new threads
    threads = []
    for i, divide_pages in enumerate(divide_pages_grouped):
      thread = "thread" + str(i)
      thread = myThread(PTT_board, divide_pages[0], divide_pages[1], str(i))
      threads.append(thread)
    for x in threads:
        print "Thread-" + x.thread_number + " Starting"
        x.start()
    for x in threads:
        x.join()
        print "Thread-" + x.thread_number + " Finished"
    add_brackets(PTT_board) 
    print "Finished Crawling " + PTT_board

