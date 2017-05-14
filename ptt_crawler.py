#coding=utf-8 
#Last Parsing: page 1312 Dec 5
import re
import sys
import json
import requests
import io
import threading
import os
import random
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup  
requests.packages.urllib3.disable_warnings()

PTT_board=""
load={
'from':'/bbs/'+PTT_board+'/index.html',
'yes':'yes' 
}

rs=requests.session()
res=rs.post('https://www.ptt.cc/ask/over18',verify=False,data=load)

LAST_PAGED_PARSED = 1312

def PageCount(PTT_board):
    res=rs.get('https://www.ptt.cc/bbs/'+PTT_board+'/index.html',verify=False)
    soup=BeautifulSoup(res.text,'html.parser')
    ALLpageURL = soup.select('.btn.wide')[1]['href']
    ALLpage=int(getPageNumber(ALLpageURL))+1
    return  ALLpage 

def parseGos(link , g_id, data):
    res=rs.get(link,verify=False)
    soup = BeautifulSoup(res.text,'html.parser')
    # author
    author  = soup.select('.article-meta-value')[0].text
    # title
    title = soup.select('.article-meta-value')[2].text
    # date
    date = soup.select('.article-meta-value')[3].text
    # content
    content = soup.find(id="main-content").text
    target_content=u'※ 發信站: 批踢踢實業坊(ptt.cc),'
    content = content.split(target_content)
    content = content[0].split(date)
    main_content = content[1].replace('\n', '  ').replace('\t', '  ')
    
    # message
    num, message = 0, [] 
    for tag in soup.select('div.push'):
        num += 1
        push_userid = tag.find("span", {'class': 'push-userid'}).text       
        push_content = tag.find("span", {'class': 'push-content'}).text   
        push_content = push_content[1:]
        message.append(push_userid.encode('utf-8')+":"+push_content.encode('utf-8'))
    d={"ID":g_id , "日期":date.encode('utf-8'), "標題":title.encode('utf-8'),"作者":author.encode('utf-8'),
            "內文":main_content.encode('utf-8'), "推文":" ".join(message), "link":str(link) }
    json_data = json.dumps(d,ensure_ascii=False,indent=2,sort_keys=True)+','
    data.append(json_data)

def remove(value, deletechars):
    for c in deletechars:
        value = value.replace(c,'')
    return value.rstrip();
   

def getPageNumber(content) :
    startIndex = content.find('index')
    endIndex = content.find('.html')
    pageNumber = content[startIndex+5 : endIndex]
    return pageNumber

def groupby(n_list):
    grouped_list = []
    for i in range(len(n_list) - 1):
        start, end = n_list[i], n_list[i+1]
        grouped_list.append([start, end])

    # Last divided pages
    grouped_list.append([grouped_list[-1][-1], 0])
    return grouped_list

def crawler(PTT_board, begin, end, threadname, g_id, data):
    for number in range(begin, end, -1):
        seen = {}
        _url = 'https://www.ptt.cc/bbs/'+PTT_board+'/index'+str(number)+'.html'
        res=rs.get(_url,verify=False)
        soup = BeautifulSoup(res.text,'html.parser')
        data = []
        for tag in soup.select('div.title'):
            try:
                atag=tag.find('a')
                time=random.uniform(1, 10)/5
                sleep(time)
                if(atag):
                    URL=atag['href'].strip()   
                    link='https://www.ptt.cc'+URL
                g_id = g_id+1
                parseGos(link, g_id, data)                     
            except:
                pass
        store(data, threadname, PTT_board)

def store(data, threadname, PTT_board):
    if not os.path.exists(PTT_board):
        os.makedirs(PTT_board)
    FILENAME = PTT_board + '/data-' + threadname + '.json'
    with open(FILENAME, 'a') as f:
        if os.stat(FILENAME).st_size == 0:
            f.write("[")
        f.write("\n".join(data))

def addBrackets(PTT_board):
    cwd = os.getcwd()
    for _file in os.listdir(cwd + '/' + PTT_board):
        if(_file == ".DS_Store" or _file == "*.swp"):
            continue
        _file = cwd + '/' + PTT_board + '/' + _file
        with open(_file, 'a') as myFile:
            if os.stat(_file).st_size == 0:
                myFile.write("[")
            myFile.seek(-1, os.SEEK_END)
            myFile.truncate()
            myFile.write("]") 

class myThread(threading.Thread):
    def __init__(self, PTT_board, begin, end, threadname):
        threading.Thread.__init__(self)
        self.PTT_board = PTT_board
        self.begin = begin
        self.end = end
        self.threadname = threadname
        self.data = list()
        self.g_id = 0
    def run(self):
        crawler(self.PTT_board, self.begin, self.end, self.threadname, self.g_id, self.data)
if __name__ == "__main__":  
    PTT_board = str(sys.argv[1]) 
    print 'Start parsing [',PTT_board,']....'
    all_page = PageCount(PTT_board)
    divide_pages = [x for x in range(all_page, 0, -all_page/60)]
    divide_pages_grouped = groupby(divide_pages)
    print divide_pages_grouped

    # Create new threads
    threads = []
    for i, divide_pages in enumerate(divide_pages_grouped):
      thread = "thread" + str(i)
      thread = myThread(PTT_board, divide_pages[0], divide_pages[1], str(i))
      threads.append(thread)
    for x in threads:
        print x.threadname + " Starting"
        x.start()
    for x in threads:
        x.join()
        print x.threadname + " Finished"   
    addBrackets(PTT_board) 
    print "Exited Thread"

