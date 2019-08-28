#!/usr/bin/env python

'''
This script is for correcting SciHub acquisition
metadata. For discrepencies where endtime is less than
startime.
'''

import os
from datetime import datetime
import re
from hysds.celery import app
import requests
import json
import elasticsearch
import dateutil.parser

BASE_PATH = os.path.dirname(__file__)

es_url = app.conf["GRQ_ES_URL"]
_index = "grq_v2.0_acquisition-s1-iw_slc"
_type = "acquisition-S1-IW_SLC"
ES = elasticsearch.Elasticsearch(es_url)


def get_index_and_type():
    try:
        dataset = json.loads(open("dataset.json", "r").read())
        ipath = dataset[0].get("ipath")
        typ = ipath[ipath.rfind("/"):]
        version = dataset[0].get("version")
        index = "grq_v{}_acquisition-s1-iw_slc".format(version)
    except:
        index = "grq_{}_acquisition-s1-iw_slc".format("v2.0")
        typ = "acquisition-S1-IW_SLC"
    return index, typ


def update_document_by_id(acq_id, body, index=_index, typ=_type):
    """
    Delete documents in ES by ID
    :param index:
    :param typ:
    :param acq_id:
    :return:
    """
    ES.update(index=index, doc_type=typ, id=acq_id, body=body)


def query_ES_acqs(query, es_index=_index):
    """
    This function creates a list of acquisition IDs that are in the past.
    :param query:
    :param es_index:
    :return:
    """
    print "Querying ES"
    acq_list = []
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/_search".format(rest_url)
    if es_index:
        url = "{}/{}/_search".format(rest_url, es_index)
    print(url)
    print(query)
    hits = query_es(url, query)
    print('completed query')
    count = 0
    for item in hits:
        acq_id = item.get("_id")
        # print acq_id
        acq_info = dict()
        try:
            start_time = dateutil.parser.parse(item.get("fields").get("starttime")[0])
        except:
            start_time = dateutil.parser.parse(item.get("fields").get("starttime")[0])

        try:
            end_time = dateutil.parser.parse(item.get("fields").get("endtime")[0])
        except:
            end_time = dateutil.parser.parse(item.get("fields").get("endtime")[0])

        if end_time < start_time:
            count += 1
            print("ID: {}  Start time: {}, End time: {}".format(item.get("fields").get("metadata.title")[0], start_time,
                                                                end_time))
            acq_info["id"] = acq_id
            acq_info["start_time"] = start_time
            acq_info["end_time"] = end_time
            acq_info["file_name"] = item.get("fields").get("metadata.title")[0]
            acq_list.append(acq_info)

    print "Count of incorrect acqs: {}".format(count)
    return acq_list

def query_es(grq_url, es_query):
    '''
        Runs the query through Elasticsearch, iterates until
        all results are generated, & returns the compiled result
        '''
    # make sure the fields from & size are in the es_query
    if 'size' in es_query.keys():
        iterator_size = es_query['size']
    else:
        iterator_size = 10000
        es_query['size'] = iterator_size
    if 'from' in es_query.keys():
        from_position = es_query['from']
    else:
        from_position = 0
        es_query['from'] = from_position
    # run the query and iterate until all the results have been returned
    print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    # print('status code: {}'.format(response.status_code))
    # print('response text: {}'.format(response.text))
    response.raise_for_status()
    results = json.loads(response.text, encoding='ascii')
    results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    for i in range(iterator_size, total_count, iterator_size):
        es_query['from'] = i
        print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
        response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
        response.raise_for_status()
        results = json.loads(response.text, encoding='ascii')
        results_list.extend(results.get('hits', {}).get('hits', []))
    return results_list


def acqs_to_update(update_list):
    """
    find acquisitions with a specific status
    :param index:
    :param status:
    :return:
    """
    for item in update_list:
        doc_id = item.get("id")
        acq_id = item.get("file_name")
        match_pattern = "(?P<spacecraft>S1\w)_IW_SLC__(?P<misc>.*?)_(?P<s_year>\d{4})(?P<s_month>\d{2})(?P<s_day>\d{2})T(?P<s_hour>\d{2})(?P<s_minute>\d{2})(?P<s_seconds>\d{2})_(?P<e_year>\d{4})(?P<e_month>\d{2})(?P<e_day>\d{2})T(?P<e_hour>\d{2})(?P<e_minute>\d{2})(?P<e_seconds>\d{2})(?P<misc2>.*?)$"
        m = re.match(match_pattern, acq_id)
        file_starttime = "{}-{}-{}T{}:{}:{}Z".format(m.group("s_year"), m.group("s_month"), m.group("s_day"),
                                                     m.group("s_hour"), m.group("s_minute"), m.group("s_seconds"))
        s_time = datetime.strptime(file_starttime, "%Y-%m-%dT%H:%M:%SZ")
        file_endtime = "{}-{}-{}T{}:{}:{}Z".format(m.group("e_year"), m.group("e_month"), m.group("e_day"),
                                                   m.group("e_hour"), m.group("e_minute"), m.group("e_seconds"))
        e_time = datetime.strptime(file_endtime, "%Y-%m-%dT%H:%M:%SZ")
        if s_time < e_time:
            print "For ID: {}".format(acq_id)
            print "File Start Time: {}, File End Time: {}".format(file_starttime, file_endtime)
            print "Metadata Start Time : {}, Metadata End Time: {}".format(item.get("start_time"), item.get("end_time"))
            print "Submitting for update"
            doc = dict()
            body = dict()
            body["starttime"] = file_starttime
            body["endtime"] = file_endtime
            metadata = dict()
            metadata["sensingStart"] = file_starttime
            metadata["sensingStop"] = file_endtime
            body["metadata"] = metadata
            doc["doc"] = body
            print json.dumps(doc)
            update_document_by_id(doc_id, body=doc)
        else:
            print "Inconsistency in filename too. Aborting correction"

    return


if __name__ == "__main__":
    '''
    Main program to delete  planned or predicted
    acquisitions from bos sarcat acq index
    '''

    # find and delete past planned acquisitions
    query = {
        "query": {
        "match_all": {}
          },
          "fields": [
            "_id"
          ]
    }
    acq_list = query_ES_acqs(query=query)
    if len(acq_list) != 0:
        print("Updating following acquisitions :::")
        acqs_to_update(update_list= acq_list)
    else:
        print("No acquisitions to correct")