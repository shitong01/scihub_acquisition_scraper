#!/usr/bin/env python
"""
Cron script to submit scihub scraper jobs.
"""

from __future__ import print_function
from datetime import datetime, timedelta
import argparse
from hysds_commons.job_utils import submit_mozart_job


if __name__ == "__main__":
    '''
    Main program that is run by cron to submit a scraper job
    '''

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("qtype", help="query endpoint, e.g. (opensearch|odata|stub)")
    parser.add_argument("ds_es_url", help="ElasticSearch URL for acquisition dataset, e.g. " +
                         "http://aria-products.jpl.nasa.gov:9200/grq_v1.1_acquisition-s1-iw_slc/acquisition-S1-IW_SLC")
    parser.add_argument("starttime", help="Start time in ISO8601 format", nargs='?',
                        default="%sZ" % (datetime.utcnow()-timedelta(days=2)).isoformat())
    parser.add_argument("endtime", help="End time in ISO8601 format", nargs='?',
                        default="%sZ" % datetime.utcnow().isoformat())
    parser.add_argument("--tag", help="PGE docker image tag (release, version, " +
                                      "or branch) to propagate",
                        default="master", required=False)
    args = parser.parse_args()

    qtype = args.qtype
    ds_es_url = args.ds_es_url
    starttime = args.starttime
    endtime = args.endtime
    job_spec = "job-acquisition_ingest-scihub:{}".format(args.tag)

    rtime = datetime.utcnow()
    job_name = "%s-%s-%s-%s" % (job_spec, starttime.replace("-", "").replace(":", ""),
                                endtime.replace("-", "").replace(":", ""),
                                rtime.strftime("%d_%b_%Y_%H:%M:%S"))
    job_name = job_name.lstrip('job-')

    #Setup input arguments here
    rule = {
        "rule_name": "acquistion_ingest-scihub",
        #"queue": "factotum-job_worker-apihub_%s_throttled" % qtype, # job submission queue
        "queue": "factotum-job_worker-apihub_scraper_throttled",
        "priority": 0,
        "kwargs":'{}'
    }
    params = [
        { 
            "name": "es_dataset_url",
            "from": "value",
            "value": ds_es_url,
        },
        { 
            "name": "ds_cfg",
            "from": "value",
            "value": "datasets.json"
        },
        { 
            "name": "starttime",
            "from": "value",
            "value": starttime
        },
        { 
            "name": "endtime",
            "from": "value",
            "value": endtime
        },
        { 
            "name": "ingest_flag",
            "from": "value",
            "value": "--ingest"
        }
    ]

    print("submitting scraper job for %s" % qtype)
    submit_mozart_job({}, rule,
        hysdsio={"id": "internal-temporary-wiring",
                 "params": params,
                 "job-specification": job_spec},
        job_name=job_name)
