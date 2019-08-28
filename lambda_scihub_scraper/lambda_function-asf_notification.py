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
JOB_TYPE = os.environ['JOB_TYPE']
JOB_RELEASE = os.environ['JOB_RELEASE']
TAGGING = os.environ['PRODUCT_TAG']  # set to True if product in HySDS catalog should be tagged as delivered.


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
        'job_name': 'job_%s-%s' % ('es_update', product_id),
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
            print ('submitted upate ES:%s job: %s job_id: %s' % (job_type, release, job_id))
        else:
            print ('job not submitted successfully: %s' % result)
            raise Exception('job not submitted successfully: %s' % result)
    else:
        raise Exception('job not submitted successfully: %s' % result)


def lambda_handler(event, context):
    """
    This lambda handler calls submit_job with the job type info
    and product id from the sns message
    :param event:
    :param context:
    :return:
    """
    print ("Got event of type: %s" % type(event))
    print ("Got event: %s" % json.dumps(event, indent=2))
    print ("Got context: %s" % context)

    sns_message = json.loads(event["Records"][0]["Sns"]["Message"])
    product = sns_message["ProductName"]
    print ("From SNS product key: %s" % product)

    # submit mozart jobs to update ES
    job_type = JOB_TYPE
    job_release = JOB_RELEASE
    job_params = {"sns_message": sns_message}  # pass the whole SNS message
    if TAGGING is True:
        job_params["product_tagging"] = True
    else:
        job_params["product_tagging"] = False

    job_tag = "asf_delivered"
    submit_job(job_type, job_release, product, job_tag, json.dumps(job_params))
