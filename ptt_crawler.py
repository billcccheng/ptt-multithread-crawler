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

PttName=""
load={
'from':'/bbs/'+PttName+'/index.html',
'yes':'yes' 
}

rs=requests.session()
res=rs.post('https://www.ptt.cc/ask/over18',verify=False,data=load)

LAST_PAGED_PARSED = 1312

def PageCount(PttName):
    res=rs.get('https://www.ptt.cc/bbs/'+PttName+'/index.html',verify=False)
    soup=BeautifulSoup(res.text,'html.parser')
    ALLpageURL = soup.select('.btn.wide')[1]['href']
    ALLpage=int(getPageNumber(ALLpageURL))+1
    return  ALLpage 

<<<<<<< Updated upstream
=======
def crawler(PttName):
    ALLpage=PageCount(PttName)
    g_id = 0;
    data = []
    for number in range(ALLpage, 0,-1):
        res=rs.get('https://www.ptt.cc/bbs/'+PttName+'/index'+str(number)+'.html',verify=False)
        soup = BeautifulSoup(res.text,'html.parser')
        for tag in soup.select('div.title'):
            try:
                atag=tag.find('a')
                time=random.uniform(0, 1)/5
                #print 'time:',time
                sleep(time)
                if(atag):
                   URL=atag['href']   
                   link='https://www.ptt.cc'+URL
                   #print link
                g_id = g_id+1
                parseGos(link, g_id, data)                     
            except:
                print 'error:',URL
        store(data)
        data = []
>>>>>>> Stashed changes
def parseGos(link , g_id, data):
        res=rs.get(link,verify=False)
        soup = BeautifulSoup(res.text,'html.parser')
        # author
        author  = soup.select('.article-meta-value')[0].text
        #author = soup.find("span", {'class': 'article-meta-value'}).text              
        #print 'author:',author
        # title
        title = soup.select('.article-meta-value')[2].text
        #print 'title:',title
        # date
        date = soup.select('.article-meta-value')[3].text
        #print 'date:',date
        

        # content
        content = soup.find(id="main-content").text
        target_content=u'※ 發信站: 批踢踢實業坊(ptt.cc),'
        content = content.split(target_content)
        content = content[0].split(date)
        main_content = content[1].replace('\n', '  ').replace('\t', '  ')
        #print 'content:',main_content
        
        # message
        num , g , b , n ,message = 0,0,0,0,{}
        
        for tag in soup.select('div.push'):
            num += 1
            push_tag = tag.find("span", {'class': 'push-tag'}).text
            #print "push_tag:",push_tag
            push_userid = tag.find("span", {'class': 'push-userid'}).text       
            #print "push_userid:",push_userid
            push_content = tag.find("span", {'class': 'push-content'}).text   
            push_content = push_content[1:]
            #print "push_content:",push_content
            push_ipdatetime = tag.find("span", {'class': 'push-ipdatetime'}).text   
            push_ipdatetime = remove(push_ipdatetime, '\n')
            #print "push-ipdatetime:",push_ipdatetime 
                
        # json-data  type(d) dict
          
        d={ "ID":g_id , "標題":title.encode('utf-8'), "日期":date.encode('utf-8'),
            "內文":main_content.encode('utf-8'), "link":str(link) }
        json_data = json.dumps(d,ensure_ascii=False,indent=4,sort_keys=True)+','
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
    sublist = []
    for i in range(len(n_list) - 1):
        sublist.append(n_list[i])
        sublist.append(n_list[i+1])
        grouped_list.append(sublist)
        sublist = []

    grouped_list.append([grouped_list[-1][-1], 0])
    # print n_list
    return grouped_list

def crawler(PttName, begin, end, threadname, data, g_id):
    for number in range(begin, end, -1):
        _url = 'https://www.ptt.cc/bbs/'+PttName+'/index'+str(number)+'.html'
        res=rs.get(_url,verify=False)
        soup = BeautifulSoup(res.text,'html.parser')
        for tag in soup.select('div.title'):
            try:
                atag=tag.find('a')
                time=random.uniform(1, 10)/5
                #print 'time:',time
                sleep(time)
                if(atag):
                   URL=atag['href']   
                   link='https://www.ptt.cc'+URL
                   #print link
                g_id = g_id+1
                parseGos(link, g_id, data)                     
            except:
#                print 'error:',URL
                pass
        store(data, threadname)
        data = []

def store(data, threadname):
#    print "Storing "+ threadname
    FILENAME = 'data/data-' + threadname + '.json'
    with open(FILENAME, 'a') as f:
        if os.stat(FILENAME).st_size == 0:
            f.write("[")
        f.write("\n".join(data))

def addBrackets():
    cwd = os.getcwd()
    for _file in os.listdir(cwd + '/data'):
        if(_file == ".DS_Store" or _file == "*.swp"):
            continue
        _file = cwd + '/data/' + _file
        with open(_file, 'a') as myFile:
            if os.stat(_file).st_size == 0:
                myFile.write("[")
            myFile.seek(-1, os.SEEK_END)
            myFile.truncate()
            myFile.write("]") 

class myThread(threading.Thread):
    def __init__(self, PttName, begin, end, threadname):
        threading.Thread.__init__(self)
        self.PttName = PttName
        self.begin = begin
        self.end = end
        self.threadname = threadname
        self.data = list()
        self.g_id = 0
    def run(self):
        crawler(self.PttName, self.begin, self.end, self.threadname, self.data, self.g_id)
if __name__ == "__main__":  
    PttName = str(sys.argv[1])
    print 'Start parsing [',PttName,']....'
    all_page = PageCount(PttName)
    # print all_page
    # sys.exit()
    # for number in range(len(all_page)):
    divide_pages = [x for x in range(all_page, 0, -all_page/10)]
    divide_pages_grouped = groupby(divide_pages)
    print divide_pages_grouped
    # Create new threads
    threads = []
    for i in range(len(divide_pages_grouped)):
      thread = "thread"+str(i)
      thread = myThread(PttName, divide_pages_grouped[i][0], divide_pages_grouped[i][1], str(i))
      #print thread.threadname+" started"
      threads.append(thread)
    for x in threads:
        print x.threadname + " Starting"
        x.start()
    for x in threads:
        x.join()
        print x.threadname + " Finished"   
    addBrackets() 
    print "Exited Thread"

