import json
import os
import requests
import dateutil.parser
from datetime import datetime, timedelta
import elasticsearch
from hysds.celery import app
from hysds_commons.job_utils import submit_mozart_job

BASE_PATH = os.path.dirname(__file__)

es_url = app.conf["GRQ_ES_URL"]
ES = elasticsearch.Elasticsearch(es_url)

job_types = {
    "asf": "job-ipf-scraper-asf",
    "scihub": "job-ipf-scraper-scihub"
}

job_queues = {
    "asf": "ipf-scraper-asf",
    "scihub": "ipf-scraper-scihub"
}


def get_non_ipf_acquisitions(location, start_time, end_time):
    """
    This function would query for all the acquisitions that
    temporally and spatially overlap with the AOI
    :param location:
    :param start_time:
    :param end_time:
    :return:
    """
    index = "grq_v2.0_acquisition-s1-iw_slc"
    query = {
        "query": {
            "filtered": {
                "filter": {
                    "geo_shape": {
                        "location": {
                            "shape": location
                        }
                    }
                },
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "metadata.sensingStart": {
                                        "to": end_time,
                                        "from": start_time
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    acq_list = []
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
        ipf_version = item.get("_source").get("metadata").get("processing_version")
        if ipf_version is None:
            acq_info = dict()
            acq_info["id"] = item.get("_id")
            acq_info["metadata"] = item.get("_source").get("metadata")
            acq_list.append(acq_info)

    return acq_list


def submit_ipf_scraper(acq, tag, endpoint):
    params = [
        {
            "name": "acq_id",
            "from": "value",
            "value": acq.get("id")
        },
        {
            "name": "acq_met",
            "from": "value",
            "value": acq.get("metadata")
        },
        {
            "name": "index",
            "from": "value",
            "value": "grq_v2.0_acquisition-s1-iw_slc"
        },
        {
            "name": "dataset_type",
            "from": "value",
            "value": "acquisition-S1-IW_SLC"
        },
        {
            "name": "endpoint",
            "from": "value",
            "value": endpoint
        },
        {
            "name": "ds_cfg",
            "from": "value",
            "value": "datasets.json"
        }
    ]

    rule = {
        "rule_name": "ipf_scraper_{}".format(endpoint),
        "queue": job_queues.get(endpoint),
        "priority": '5',
        "kwargs": '{}'
    }

    # based on the

    print('submitting jobs with params:')
    print(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    mozart_job_id = submit_mozart_job({}, rule, hysdsio={"id": "internal-temporary-wiring", "params": params,
                                                         "job-specification": "{}:{}".format(job_types.get(endpoint),tag)},
                                      job_name='%s-%s-%s' % (job_types.get(endpoint), acq.get("id"), tag))
    print("For {} , IPF scrapper Job ID: {}".format(acq.get("id"), mozart_job_id))


if __name__ == "__main__":
    """
    This script will find all acquisitions without IPF versions
    overlapping with a specific AOI. It will then submit IPF scraper
    jobs for each acquisition.
    """

    ctx = json.loads(open("_context.json", "r").read())
    location = ctx.get("spatial_extent")
    start_time = ctx.get("start_time")
    end_time = ctx.get("end_time")
    tag = ctx.get("container_specification").get("version")
    acqs_list = get_non_ipf_acquisitions(location, start_time, end_time)

    for acq in acqs_list:
        print(json.dumps(acq))
        print("Date:" + acq.get("metadata").get("sensingStart"))
        acq_date = acq.get("metadata").get("sensingStart")
        start_time = dateutil.parser.parse(acq_date)
        if start_time.replace(tzinfo=None) < datetime.now() - timedelta(days=1):
            endpoint = "asf"
        else:
            endpoint = "scihub"
        submit_ipf_scraper(acq, tag, endpoint)
