#!/usr/bin/python
# coding=utf-8 
import sys
import requests
import io
import threading
import os
import random
import re
import codecs
import parse_link
import get_doc_id
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup  
requests.packages.urllib3.disable_warnings()

rs=requests.session()

def page_count(PTT_board):
    res = rs.get('https://www.ptt.cc/bbs/'+PTT_board+'/index.html',verify=False)
    soup = BeautifulSoup(res.text,'html.parser')
    all_page_url = soup.select('.btn.wide')[1]['href']
    all_page=int(get_page_num(all_page_url))+1
    return  all_page 

def crawler(PTT_board, begin, end, thread_number, last_id, data):
    for number in range(begin, end, -1):
        _url = 'https://www.ptt.cc/bbs/'+PTT_board+'/index'+str(number)+'.html'
        res=rs.get(_url,verify=False)
        soup = BeautifulSoup(res.text,'html.parser')
        data = []
        for tag in soup.select('div.title'):
            try:
                atag = tag.find('a')
                time = random.uniform(1, 5)/5
                sleep(time)
                if(atag):
                    URL=atag['href'].strip()   
                    link='https://www.ptt.cc'+URL
                    if not link_exists(URL, last_id):
                        parse_link.parse(link, data)                     
            except Exception, err:
                print('\033[91m'+ str(err) + '\033[0m')
        if data:
            store_file(data, thread_number, PTT_board)

def link_exists(URL, last_id):
    regex = '.(\d+).\w{1}'
    p = re.compile(regex)
    doc_id = p.findall(URL) 
    return int(doc_id[0] ) <= last_id


def store_file(data, thread_number, PTT_board):
    print('\033[91m' + "Storing Thread-" + thread_number + '\033[0m')
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
    def __init__(self, PTT_board, begin, end, thread_number, latest_doc_id):
        threading.Thread.__init__(self)
        self.PTT_board = PTT_board
        self.begin = begin
        self.end = end
        self.thread_number = thread_number
        self.last_id = latest_doc_id 
        self.data = list()
    def run(self):
        crawler(self.PTT_board, self.begin, self.end, self.thread_number, self.last_id, self.data)

if __name__ == "__main__":  
    PTT_board = str(sys.argv[1]).lower() 
    thread_num = int(sys.argv[2])
    print('Start parsing [',PTT_board,']....')
    all_page = page_count(PTT_board)
    divide_pages = [x for x in range(all_page, 0, -all_page/thread_num)]
    divide_pages_grouped = group_by(divide_pages)
    latest_doc_id, num_of_files = get_doc_id.latest(PTT_board)

    # Create new threads
    threads = []
    for i, divide_pages in enumerate(divide_pages_grouped):
      thread_name = "thread-" + str(num_of_files + i)
      thread = myThread(PTT_board, divide_pages[0], divide_pages[1], thread_name, latest_doc_id)
      threads.append(thread)
    for x in threads:
        print("Thread-" + x.thread_number + " Starting")
        x.start()
    for x in threads:
        x.join()
        print("Thread-" + x.thread_number + " Finished")
    add_brackets(PTT_board) 
    print("Exited Thread")

