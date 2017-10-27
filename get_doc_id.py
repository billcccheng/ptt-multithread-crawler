import json
import os
import re


def latest(board_name):
    dir_name = '../ptt-search-server/boards/'+ board_name + '/'
    files = os.listdir(dir_name)
    num_of_files = len(files)
    latest_board_id = float('-inf') 
    for file in files:
        with open(dir_name + file) as data_file:    
            data = json.load(data_file)
            for datum in data:
                regex = '.(\d+).\w{1}'
                p = re.compile(regex)
                doc_id = p.findall(datum['link']) 
                if doc_id:
                    latest_board_id = max(latest_board_id, int(doc_id[0])) 
    return latest_board_id, num_of_files

