# -*- encoding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import csv
import re


def get_host_info(conn):
    match = re.findall(r'[0-9]+(?:\.[0-9]+){3}(:[0-9]+)?', conn)
    if match.__len__() == 0:
        return None
    try:
        return conn
    except Exception as e:
        print(str(e))
        return None


def read_from_csv(file_path):
    try:
        # remove \ufeff character pattern usiing utf-8-sig
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        return rows
    except FileNotFoundError as e:
        print(str(e))
        return False


def insert_bulk_to_es(con_info, list_data, index, doc_type):
    try:
        if get_host_info(con_info) is not None:
            es = Elasticsearch(host_info)
            helpers.bulk(es, list_data, index=index, doc_type=doc_type)
            return True
        else:
            return False
    except Exception as e:
        return False


if __name__ == '__main__':
    csv_file = "csv_file"
    es_index = 'my_index'
    es_doc_type = 'my_type'
    host_info = "127.0.0.1:9200"
    data = read_from_csv(csv_file)

    if data is False:
        print("Not found {} OR Can't read this file".format(csv_file))
        exit(1)
    result = insert_bulk_to_es(host_info, data, es_index, es_doc_type)
    if result:
        print('Success save data')
    else:
        print('Fail save data')
