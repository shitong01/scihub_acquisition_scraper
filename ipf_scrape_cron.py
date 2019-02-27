import json
import os
import requests
import elasticsearch
from hysds.celery import app
from hysds_commons.job_utils import submit_mozart_job

BASE_PATH = os.path.dirname(__file__)

es_url = app.conf["GRQ_ES_URL"]
_type = "area_of_interest"
ES = elasticsearch.Elasticsearch(es_url)


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
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
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
    params = [
        {
            "name": "AOI_name",
            "from": "value",
            "value": aoi.get("id")
        },
        {
            "name": "spatial_extent",
            "from": "value",
            "value": aoi.get("location")
        },
        {
            "name": "start_time",
            "from": "value",
            "value": aoi.get("starttime")
        },
        {
            "name": "end_time",
            "from": "value",
            "value": aoi.get("endtime")
        }
    ]

    rule = {
        "rule_name": "{}_ipf_scraper".format(aoi.get("id")),
        "queue": "factotum-job_worker-apihub_scraper_throttled",
        "priority": '4',
        "kwargs": '{}'
    }

    print('submitting jobs with params:')
    print(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    mozart_job_id = submit_mozart_job({}, rule, hysdsio={"id": "internal-temporary-wiring", "params": params,
                                                         "job-specification": "job-AOI_based_ipf_submitter:master"},
                                      job_name='job-%s-%s-%s' % ("aoi_ipf_submitter", aoi.get("id"), "master"),
                                      enable_dedup=False)
    print("For {} , AOI IPF Submitter Job ID: {}".format(aoi.get("_id"), mozart_job_id))


if __name__ == "__main__":
    """
    This script will find all active AOIs. It will then submit AOI IPF scraper
    jobs for each AOI.
    """
    aoi_list = get_aois()
    for aoi in aoi_list:
        submit_aoi_ipf(aoi)

