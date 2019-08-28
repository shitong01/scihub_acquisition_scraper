#!/usr/bin/env python
from __future__ import print_function

'''
This lambda function submits a job via MOZART API
to update ES doc, for book keeping purposes.
When an SNS message is recieved from ASF reporting
successful receipt and ingestion of product delivered by our system,
we want to capture this acknowledgement by stamping the product
with delivery and ingestion time.
'''

import os
import json
import requests

print ('Loading function')

MOZART_URL = os.environ['MOZART_URL']
QUEUE = os.environ['QUEUE']
JOB_TYPE = os.environ['JOB_TYPE'] #job-AOI_based_ipf_submitter
JOB_RELEASE = os.environ['JOB_RELEASE'] #master
ES_URL = os.environ["GRQ_ES_URL"]


def submit_job(job_type, release, product_id, tag, job_params):
    """
    submits a job to mozart
    :param job_type:
    :param release:
    :param product_id:
    :param tag:
    :param job_params:
    :return:
    """

    # submit mozart job
    job_submit_url = '%s/mozart/api/v0.1/job/submit' % MOZART_URL
    params = {
        'queue': QUEUE,
        'priority': '5',
        'job_name': 'job-%s-%s-%s' % ("aoi_ipf_submitter", product_id, release),
        'tags': '["%s"]' % tag,
        'type': '%s:%s' % (job_type, release),
        'params': job_params,
        'enable_dedup': False
    }

    print ('submitting jobs with params:')
    print (json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    req = requests.post(job_submit_url, params=params, verify=False)
    if req.status_code != 200:
        req.raise_for_status()
    result = req.json()
    if 'result' in result.keys() and 'success' in result.keys():
        if result['success'] is True:
            job_id = result['result']
            print ('submitted job: %s:%s job_id: %s' % (job_type, release, job_id))
        else:
            print ('job not submitted successfully: %s' % result)
            raise Exception('job not submitted successfully: %s' % result)
    else:
        raise Exception('job not submitted successfully: %s' % result)


def get_aois():
    """
    This function would query for all the acquisitions that
    temporally and spatially overlap with the AOI
    :param location:
    :param start_time:
    :param end_time:
    :return:
    """
    index = "grq_v3.0_area_of_interest"
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "dataset_type.raw": "area_of_interest"
                        }
                    }
                ]
            }
        }
    }

    aoi_list = []
    rest_url = ES_URL[:-1] if ES_URL.endswith('/') else ES_URL
    url = "{}/{}/_search?search_type=scan&scroll=60&size=10000".format(rest_url, index)
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    if count == 0:
        return []
    if '_scroll_id' not in scan_result:
        print("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" % query)
        return []
    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0:
            break
        hits.extend(res['hits']['hits'])

    for item in hits:
        aoi_info = dict()
        aoi_info["id"] = item.get("_id")
        aoi_info["location"] = item.get("_source").get("location")
        aoi_info["starttime"] = item.get("_source").get("starttime")
        aoi_info["endtime"] = item.get("_source").get("endtime")
        aoi_list.append(aoi_info)

    return aoi_list


def submit_aoi_ipf(aoi):
    job_params = {
            "AOI_name": aoi.get("id"),
            "spatial_extent": aoi.get("location"),
            "start_time": aoi.get("starttime"),
            "end_time": aoi.get("endtime")
        }

    rule = {
        "rule_name": "{}_ipf_scraper".format(aoi.get("id")),
        "queue": "factotum-job_worker-apihub_scraper_throttled",
        "priority": '5',
        "kwargs": '{}'
    }

    print('submitting jobs with params:')
    print(json.dumps(job_params, sort_keys=True, indent=4, separators=(',', ': ')))
    job_id = submit_job(job_type=JOB_TYPE, release=JOB_RELEASE, product_id= aoi.get("id"), tag = "aoi_ipf_submitter-{}"
                        .format(aoi.get("id")), job_params=job_params)

    print("For {} , AOI IPF Submitter Job ID: {}".format(aoi.get("_id"), job_id))


if __name__ == "__main__":
    """
    This script will find all active AOIs. It will then submit AOI IPF scraper
    jobs for each AOI.
    """
    aoi_list = get_aois()
    for aoi in aoi_list:
        submit_aoi_ipf(aoi)


def lambda_handler(context):
    """
    This lambda handler calls submit_job
    to submit IPF scraping job for AOIs.
    :param event:
    :param context:
    :return:
    """
    print ("Got context: %s" % context)

    # submit mozart jobs to update ES
    job_type = JOB_TYPE
    job_release = JOB_RELEASE
    for aoi in get_aois():
        submit_aoi_ipf(aoi)
