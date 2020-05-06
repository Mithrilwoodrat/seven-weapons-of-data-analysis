# -*- coding: utf-8 -*-
import json
import sys
import os
import time
import logging


ABSPATH = os.path.abspath(os.path.realpath(os.path.dirname(__file__)))


MAXSIZE = 10000


OUTFileName = 'dump.json'

dumps = []

def process_resp_docs(fout, resp_docs):
    for item in resp_docs:
        index = item['_index']
        _type = 0
        source = item['_source']
        jsonvalue = json.dumps(source)
        fout.write(jsonvalue+'\n')

def fetch_from_es(es_client, index, body):
    fout = open(OUTFileName, 'w')
    total_es_time = 0
    total_process_time = 0
    if body is None:
        logging.error("body empty!")
        return
    logging.info("using filter : {0}".format(body))
    logging.info("searching on indexs: {0}".format(index))
    resp = es_client.search(
        index=index, body=body, scroll="3m", size=MAXSIZE, timeout='30s')
    scroll_id = resp['_scroll_id']
    total = resp['hits']['total']
    resp_docs = resp['hits']['hits']
    process_resp_docs(fout, resp_docs)
    count = len(resp_docs)
    logging.info("filter match {0}".format(total))
    if type(total) == dict: # es 7
        total = total['value']
    logging.info("filter match {0}".format(total))
    if count > 20000000:
        logging.error("filter match more than 20000000, job stoped")
        return

    while len(resp_docs) > 0:
        t1 = time.time()
        resp = es_client.scroll(scroll_id=scroll_id, scroll="3m")
        t2 = time.time()
        total_es_time += t2 - t1
        scroll_id = resp['_scroll_id']
        resp_docs = resp['hits']['hits']
        count += len(resp_docs)
        logging.info("comming events: {0}".format(len(resp_docs)))
        t1 = time.time()
        process_resp_docs(fout, resp_docs)
        t2 = time.time()
        total_process_time += t2 - t1
        if count >= total:
            break


    logging.info('request es time: {}'.format(total_es_time))
    logging.info('process resp_docs time: {}'.format(total_process_time))
    # return result
    fout.close()
    return


if __name__ == "__main__":
    logging.basicConfig(
        format='[%(asctime)s] (%(module)s:%(funcName)s:%(lineno)s): <%(levelname)s> %(message)s', level=logging.INFO)
    es_host = 'http://elastic:changeme@127.0.0.1:9200'
    try:
        from elasticsearch import Elasticsearch
        es_client = Elasticsearch(
            es_host, timeout=30, max_retries=10, retry_on_timeout=True)
        logging.info("Init ES client..")
    except Exception as err:
        logging.error(err)
        sys.exit(-1)
    index = 'nginx_log'
    body = '''
    {
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "remote_ip": {
              "value": "217.168.17.5"
            }
          }
        }
      ],
      "adjust_pure_negative": true,
      "boost": 1
    }
  },
  "sort": [
    {
      "timestamp": {
        "order": "desc"
      }
    }
  ]
}
     '''
    fetch_from_es(es_client, index, body)
