#from es_client import es_client
from elasticsearch1 import helpers
import elasticsearch1 as ES
import random

def search_interval(time_s, time_e, host, index, host_t):
    time_start=time_s
    time_end=time_e
    #print time_start,"   ",time_end
    host_all = "20.0.8."+str(host_t)
    ES_SERVERS = [{
                    'host': host_all,
                        'port': 9200
                  }]
    global es_client
    es_client = ES.Elasticsearch(
            hosts=ES_SERVERS
                        )
            
    index_all = "logstash-" + index

    es_search_options = set_search_optional_interval(time_start, time_end, host)
    es_result = get_search_result(es_search_options, index_all)
    final_result_host, final_result_message, final_result_time \
    = get_result_list(es_result)
    return final_result_host, final_result_message, final_result_time

def get_search_result(es_search_options, index, scroll='3m', raise_on_error=True, preserve_order=False, doc_type='redis-input', timeout="1m"):                                           
    es_result = helpers.scan(
            client = es_client,
            query = es_search_options,
            scroll = scroll,
            index = index,
            doc_type = doc_type,
            timeout=timeout
            )
    return es_result

def get_result_list(es_result):
    final_result_host = []
    final_result_message = []
    final_result_time = []
    index = 0
    for item in es_result:
        index += 1
        final_result_message.append(str(item['_source']['message']))
        final_result_host.append(str(item['_source']['host']))
        final_result_time.append(str(item['_source']['@timestamp']))
    return final_result_host, final_result_message, final_result_time

def set_search_optional_interval(time_start, time_end, host):
    match_query = []
    for i in xrange(len(host)):
        match_query.append({"match":{"message": host[i]}})
    es_search_options = {
        "query":{
            "bool":{
                "must":[
                    {"bool":
                        {"should":[
                            match_query
                        ]
                        }
                    },
                    {"range":{
                        "@timestamp":{
                            "gt":time_start,
                            "lt":time_end
                        }
                    }
                    }   
                ],
            }
            }
        }

    return es_search_options

if __name__ == '__main__':
    time_s='2017-10-10 08:00:00'
    time_e='2017-12-20 08:00:00'
    host=[]
    for i in range(17,144):
        if i<>90:
            host.append('20.0.2.'+str(i))
    for i in range(1,90):
        host.append('20.0.208.'+str(i))
    index='2017.11.17'
    host_t=87
    final_results = search(time_s, time_e, host, index, host_t)
    #print final_results[0] 
    #print final_results[1] 
    print len(final_results[0])
    #for item in final_results[0]:
    #    print item
