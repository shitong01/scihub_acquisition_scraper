#!/usr/bin/env python
"""
Cron script to submit scihub scraper jobs.
"""

from __future__ import print_function
from builtins import str
from datetime import datetime
import json
import pandas as pd
import numpy as np
from hysds_commons.job_utils import submit_mozart_job


def get_time_segments(start_time, end_time):
    segments = np.arange(start_time, end_time, dtype='M8[M]')
    time_segments = list()
    time_segments.append(start_time)
    for date in segments[1:]:
        ts = pd.to_datetime(str(date))
        d = ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        time_segments.append(d)
    time_segments.append(end_time)

    ttl = len(time_segments)
    segment_pairs = list()
    num = 0
    while num < ttl - 1:
        segment_pairs.append([time_segments[num], time_segments[num + 1]])
        num += 1
    return segment_pairs


def get_job_params(aoi_name, job_type, starttime, endtime, polygon, dataset_version):

    rule = {
        "rule_name": "{}-{}".format(job_type.lstrip('job-'), aoi_name),
        "queue": "factotum-job_worker-apihub_scraper_throttled",
        "priority": 6,
        "kwargs": '{}'
    }
    params = [
        {
            "name": "aoi_name",
            "from": "value",
            "value": aoi_name
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
        },
        {
            "name": "report_flag",
            "from": "value",
            "value": "--report"
        },
        {
            "name": "polygon_flag",
            "from": "value",
            "value": "--polygon"
        },
        {
            "name": "polygon",
            "from": "value",
            "value": polygon
        },
        {
            "name": "ds_flag",
            "from": "value",
            "value": "--dataset_version"
        },
        {
            "name": "ds_version",
            "from": "value",
            "value": dataset_version
        },
        {
            "name": "ingest_flag",
            "from": "value",
            "value": "--ingest"
        },
        {
            "name": "purpose_flag",
            "from": "value",
            "value": "--purpose"
        },
        {
            "name": "purpose",
            "from": "value",
            "value": "aoi_scrape"
        },
        {
            "name": "report_flag",
            "from": "value",
            "value": "--report"
        }
    ]

    return rule, params


if __name__ == "__main__":
    '''
    Main program that is run by cron to submit a scraper job
    '''
    qtype = "opensearch"
    ctx = json.loads(open("_context.json", "r").read())
    aoi_name = ctx.get("AOI_name")
    dataset_version = ctx.get("dataset_version")
    starttime = ctx.get("start_time")
    endtime = ctx.get("end_time")
    polygon = ctx.get("spatial_extent")
    tag = ctx.get("container_specification").get("version")
    job_type = "job-acquisition_ingest-aoi"
    job_spec = "{}:{}".format(job_type, tag)

    segments = get_time_segments(starttime, endtime)
    for segment in segments:
        start_time = segment[0]
        end_time = segment[1]
        rtime = datetime.utcnow()
        job_name = "%s-%s-%s-%s-%s" % (job_spec, aoi_name,
                                       start_time.replace("-", "").replace(":", ""),
                                       end_time.replace("-", "").replace(":", ""),
                                       rtime.strftime("%d_%b_%Y_%H:%M:%S"))
        job_name = job_name.lstrip('job-')

        # Setup input arguments here
        rule, params = get_job_params(aoi_name=aoi_name,
                                      job_type=job_type,
                                      starttime=start_time,
                                      endtime=end_time,
                                      polygon=polygon,
                                      dataset_version=dataset_version)

        print("submitting job of type {} for {}".format(job_spec, qtype))
        print(json.dumps(params))

        submit_mozart_job({}, rule,
                          hysdsio={
                              "id": "internal-temporary-wiring",
                              "params": params,
                              "job-specification": job_spec
                          },
                          job_name=job_name)
