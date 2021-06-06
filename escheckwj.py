from io import StringIO
from os import close
import json
from elasticsearch import Elasticsearch
from collections import defaultdict
import argparse
import datetime
import sys

#sys.setrecursionlimit(2000)

parser = argparse.ArgumentParser(description='Argparse for ES')
parser.add_argument('--es_url_source', '-u1', help='源ES URL，必要参数', required=True)
parser.add_argument('--es_url_target', '-u2', help='目标ES URL，必要参数', required=True)
parser.add_argument('--user_source', '-us', help='源ES用户名，非必要参数',default='')
parser.add_argument('--password_source', '-ps', help='源ES密码，非必要参数',default='')
parser.add_argument('--user_target', '-ut', help='目标ES用户名，非必要参数',default='')
parser.add_argument('--password_target', '-pt', help='目标ES密码，非必要参数',default='')
parser.add_argument('--index_name_list', '-i', help='Index列表，非必要参数',nargs='+')
parser.add_argument('--check_mode', '-m', help='校验模式，data或id校验。非必要参数',default="data",type=str)
parser.add_argument('--percentage','-p', help='随机校验百分比，非必要参数，默认10%', default=10,type=int)
args = parser.parse_args()

def simple_diff(es_source_indices, es_target_indices):
    es_source_indices = es_source.cat.indices(format="json")
    es_target_indices = es_target.cat.indices(format="json")

    for selem in es_source_indices:
        flg = False
        for telem in es_target_indices:
            if selem['index'] == telem['index']:
                check(selem, telem)
                flg = True          
        if flg == False: f.write(selem['index']+" is not in target ES"+ "\n")
        
def check(selem,telem):
    if selem['uuid'] != telem['uuid']:
        f.write(selem['index'] + " UUID is not identical:" + selem['uuid'] + "vs" + telem['uuid'] + "\n")
    elif selem['docs.count']!=telem['docs.count']:
        f.write(selem['index'] + " docs.count is not identical: " + selem['docs.count'] + "vs" + telem['docs.count']+ "\n")
    elif selem['store.size']!=telem['store.size']:
        f.write(selem['index'] + " store.size is not identical:" + selem['store.size'] + "vs" + telem['store.size']+ "\n")
    else: print(selem['index'] + " check passed")

def diff(es_source,es_target,es_source_indices,percentage,mode):
    for elem in es_source_indices:
        f.write("------------------------------------------------------------\n")
        f.write(str(datetime.datetime.now()) + " 【Check starts】: "+elem + "\n")
        doc_count = int(es_source.cat.count(index=elem,format="json")[0]['count'])

        if mode != 'data' :
            filter = ['hits.hits._id','hits.hits._type','_scroll_id','hits.total']
            query = json.dumps({"query": {"match_all": {}}})
        else:
            filter = ['hits.hits._id', 'hits.hits._type','hits.hits._source','_scroll_id','hits.total']
            doc_count = int(doc_count*(percentage/100))
            query = parsequery(doc_count)

        if doc_count > 10000 : 
            scroll_size = 10000
            scroll_flg = True
        else:
            scroll_size = doc_count
            scroll_flg = False

        print("doc_count:" + str(doc_count))

        res = es_source.search(index=elem, 
                            body=query, 
                            request_timeout=20, 
                            scroll='10m',
                            size=scroll_size,
                            filter_path=filter)
        scroll_size = doc_count - scroll_size

        sid = res['_scroll_id']
        if mode != 'data' :
            checkid(elem,res["hits"]["hits"],es_target)
        else:
            checkdata(elem,res["hits"]["hits"],es_target)  

        while(res and scroll_size >0):
            print ("Scrolling...")  
            res = es_source.scroll(scroll_id=sid,
                                scroll='1m')
            sid = res['_scroll_id']
            print ("scroll size: "+ str(scroll_size))
            scroll_size = scroll_size - len(res['hits']['hits'])

            if mode != 'data' :
                checkid(elem,res["hits"]["hits"],es_target)
            else:
                checkdata(elem,res["hits"]["hits"],es_target) 
        
        f.write(str(datetime.datetime.now()) + " 【Check ends】: "+elem + " 【Total checked】: " + str(doc_count) + "\n")

def checkid(inx,source_data,es_target):
    types_id = defaultdict(list)
    for data in source_data:
        if(data['_type'] in types_id.keys()):
            types_id[data['_type']].append(data['_id'])
        else:
            types_id[data['_type']]=[]
            types_id[data['_type']].append(data['_id'])
    for key in types_id.keys():       
        res_target = es_target.mget(index=inx,doc_type=key,body = {'ids': types_id[key]})
        for mgetRst in res_target['docs']:
            if mgetRst['found'] == "False":
                f.write("Check failed【NOT FOUND】: "+"/{}/{}/{}".format(inx, key,mgetRst['_id'])+"\n")

def checkdata(inx,source_data,es_target):
    types_id = defaultdict(list)
    types_source = {}
    for data in source_data:
        if(data['_type'] in types_id.keys()):
            types_id[data['_type']].append(data['_id'])
        else:
            types_id[data['_type']]=[]
            types_id[data['_type']].append(data['_id'])
        types_source[data['_id']] = data['_source']

    for key in types_id.keys(): 
        res_target = es_target.mget(index=inx,doc_type=key,body = {'ids': types_id[key]})
        for mgetRst in res_target['docs']:
            if mgetRst['found'] == "False":
                f.write("Check failed【NOT FOUND】: "+"/{}/{}/{}".format(inx, key, mgetRst['_id'])+"\n")
            if mgetRst['_source'] != types_source[mgetRst['_id']]:
                f.write("Check failed【NOT Equal】: "+"/{}/{}/{}".format(inx, key, mgetRst['_id'])+"\n")
            else:
                continue

def mySort(e):
  return e['_id']

def parsequery(size):
    qdata = {
        "query": {
            "function_score": {
                "query": {
                    "match_all": {

                    }
                },
                "functions": [
                    {
                        "random_score": {

                        }
                    }
                ]
            }
        },
        "sort": {

        },
        "aggs": {

        },
        "from": 0,
        "size": size
    }
    query=json.dumps(qdata)
    return query

if __name__ == '__main__':
    try:
        es_source = Elasticsearch(args.es_url_source, http_auth=(args.user_source,args.password_source))
        es_target = Elasticsearch(args.es_url_target,http_auth=(args.user_target,args.password_target))

        f = open("checkRes.txt", "w")
        #simple_diff check indices, doc.count, doc.size, UUID
        simple_diff(es_source, es_target)

        #Get indices
        if args.index_name_list:
            es_source_indices = args.index_name_list
        else:     
            es_source_indices_json = es_source.cat.indices(format="json")
            es_source_indices = []
            for elem in es_source_indices_json:
                es_source_indices.append(elem['index'])
        
        diff(es_source,es_target,es_source_indices,args.percentage,args.check_mode)

        f.close()
    except Exception as e:
        print(e)
