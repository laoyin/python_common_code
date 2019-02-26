from multiprocessing import Process
import threading
import time
import os
import datetime
from elasticsearch.helpers import bulk

import os
import sys
import json

SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.extend([SRC_DIR])

from plugin.tasks import async_data_to_mysql
from utils.db import get_es

def deal_file(dir_name):
    dir_path = dir_name
    file_abs_name_list = []
    dir_lists = []
    if os.path.exists(dir_path):
        dir_lists = os.listdir(dir_path)
    for dir_name in dir_lists:
        file_base_path = os.path.join(dir_path, dir_name)
        # async_data_to_mysql.delay(file_base_path)
        file_abs_name_list.append(file_base_path)
    return file_abs_name_list

def insert_into_es(file_path):
    index_name =  os.path.basename(file_path).split(".")[0]
    count = 0
    with open(file_path, 'r') as f:
        for file_data in f:
            data = json.loads(file_data)
            data['fulltext'] = data["paragraphs"]
            id =  data["case_id"]
            del data["paragraphs"]
            if not data["decide_date"]:
                continue
            yield {
                        '_op_type':'update',
                        'doc': data,
                        '_index': index_name,
                        '_type': '_doc',
                        '_id':id
                    }
            #yield {
            #            '_op_type':'index',
            #            '_source': data,
            #            '_index': index_name,
            #            '_type': '_doc',
            #            '_id':id
            #        }
            count += 1
            print(count)
             


def bulk_data(es_conn, file_path):
    bulk(es_conn, insert_into_es(file_path), refresh=True)


if __name__ == '__main__':
    es = get_es("es")
    p_list = []
    file_name_list = deal_file("/data/xcases/2019-01-25")
    #file_name_list = ["/data/xcases/2019-01-25/xcases_41_0_v2.txt"]
    for i, query_name in enumerate(file_name_list):
        p = Process(target=bulk_data, args=(es, query_name ))
        p.daemon = True
        p_list.append(p)

    for p in p_list:
        p.start()
    for p in p_list:
        p.join()

