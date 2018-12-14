# -*- coding: utf-8 -*- 

import sys 

import os 

import json 

import base64 

from elasticsearch import Elasticsearch 

from elasticsearch.helpers import bulk, streaming_bulk 
  

  

def load_data(filename, index_name, doc_type): 
    for line in open(filename): 
        data = json.loads(line)
        yield { 
            "_index": index_name, 
            "_type": doc_type, 
            "_source": data 
        }

def load(es, filename, index_name, doc_type): 
    try:
        actions = load_data(filename ,index_name, doc_type) 
        errors = [] 
        success = 0 
        for ok, item in streaming_bulk(es, actions, raise_on_exception=False, raise_on_error=False): 
            if not ok: 
                errors.append(item) 
            else: 
                success += 1 
        print 'success: ', success 
        return errors 

    except Exception as err:
        print 'error', err 
        print len(errors) 

  

if __name__ == "__main__": 

    es_host = 'http://elastic:changeme@127.0.0.1:9200' 

    es_client = Elasticsearch(es_host) 
  
    if len(sys.argv) >= 4: 
        print load(es_client, sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        print 'Use ', sys.argv[0], 'filename, index_name, doc_type'