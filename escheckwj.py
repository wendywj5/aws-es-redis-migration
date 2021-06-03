from io import StringIO
from os import close
import json
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import argparse

parser = argparse.ArgumentParser(description='Argparse for ES')
parser.add_argument('--es_url_source', '-u1', help='源ES URL，必要参数', required=True)
parser.add_argument('--es_url_target', '-u2', help='目标ES URL，必要参数', required=True)
parser.add_argument('--user_source', '-us', help='源ES用户名，非必要参数',default='')
parser.add_argument('--password_source', '-ps', help='源ES密码，非必要参数',default='')
parser.add_argument('--user_target', '-ut', help='目标ES用户名，非必要参数',default='')
parser.add_argument('--password_target', '-pt', help='目标ES密码，非必要参数',default='')
parser.add_argument('--index_name_list', '-i', help='Index列表，非必要参数',nargs='+')
parser.add_argument('--check_mode', '-m', help='校验模式，data活id校验。非必要参数',default="data",type=str)
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
        if flg == False: print(selem['index']+"is not in target ES")
        
def check(selem,telem):
    chk = False
    if selem['uuid'] != telem['uuid']:
        f.write(selem['index'] + " UUID is not identical:" + selem['uuid'] + "vs" + telem['uuid'] + "\n")
    elif selem['docs.count']!=telem['docs.count']:
        f.write(selem['index'] + " docs.count is not identical: " + selem['docs.count'] + "vs" + telem['docs.count']+ "\n")
    elif selem['store.size']!=telem['store.size']:
        f.write(selem['index'] + " store.size is not identical:" + selem['store.size'] + "vs" + telem['store.size']+ "\n")
    else: print(selem['index'] + " check passed")

def ramdom_diff(es_source,es_target,es_source_indices,percentage):
    for elem in es_source_indices:
        f.write("------------------------------------------------------------\n")
        f.write("【Check starts】: "+elem + "\n")
        doc_count = int(es_source.cat.count(index=elem,format="json")[0]['count'])
        size = int(doc_count*(percentage/100))
        query = parsequery(size)
        res = es_source.search(index=elem, 
                            body=query, 
                            request_timeout=20, 
                            scroll='1m',
#                            size=100,
                            filter_path=['hits.hits._id', 'hits.hits._type','hits.hits._source','_scroll_id'])
        res_data = res["hits"]["hits"]

        for i in range(0,int(size/100)):
            query_scroll = es_source.scroll(scroll_id=res['_scroll_id'],
                                            scroll='1m',
                                            filter_path=['hits.hits._id', 'hits.hits._type','hits.hits._source','_scroll_id'])
            res_data += query_scroll["hits"]["hits"]
        for item in res_data:
                print("item:" + item['_id'])
        print("len check_data:" + str(len(res_data)))
        checkdata(elem,res_data,es_target) 
        f.write("【Check ends】: "+elem + " 【Total checked】: " + str(size) + "\n")

def checkdata(inx,source_data,es_target):
    print("checkdata starts")
    for data in source_data:
        try:
            res_target = es_target.get(index=inx, doc_type=data['_type'], id=data['_id'])['_source']
            if res_target != data['_source']:
                f.write("Check failed【NOT Indetical:: "+"/{}/{}/{}".format(inx, data['_type'], data['_id'])+"\n")
        except:
            f.write("Check failed【NOT FOUND】: "+"/{}/{}/{}".format(inx, data['_type'], data['_id'])+"\n")
    print("checkdata ends")
    
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

def id_diff(es_source,es_target,es_source_indices):
    for elem in es_source_indices:
        f.write("------------------------------------------------------------\n")
        f.write("【ID Check starts】: "+elem + "\n")
        queryd = {"query": {"match_all": {}}}
        doc_count = int(es_source.cat.count(index=elem,format="json")[0]['count'])

        res = elasticsearch.helpers.scan(es_source,
                                        index=elem,
                                        request_timeout=20,
                                        preserve_order=False,
                                        query=queryd,
                                        size=1000,
                                        clear_scroll=True,)

        checkid(elem,res,es_target) 
        f.write("【Check ends】: "+elem + " 【Total checked】: " + str(doc_count) + "\n")

def checkid(inx,source_data,es_target):
    print("checkid starts")
    for data in source_data:
        try:
            res_target = es_target.exists(index=inx, id=data['_id'])
            if not res_target:
                f.write("Check failed【NOT FOUND】: "+"/{}/{}/{}".format(inx, data['_type'], data['_id'])+"\n")
        except:
            f.write("Check failed【NOT FOUND】: "+"/{}/{}/{}".format(inx, data['_type'], data['_id'])+"\n")
    print("checkid ends")

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
        
        if(args.check_mode == "data"):
            #ramdom_diff for ramdom check based on args.percentage or entire doc [_source] check
            ramdom_diff(es_source,es_target,es_source_indices,int(args.percentage))
        else:
            #check document_id only - full check!
            id_diff(es_source,es_target,es_source_indices)

        f.close()
    except Exception as e:
        print(e)



