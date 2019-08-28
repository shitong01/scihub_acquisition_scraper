#!/usr/bin/env python
"""
Cron script to submit scihub scraper jobs.
"""

from __future__ import print_function
from datetime import datetime, timedelta
import argparse
from hysds.celery import app
from hysds_commons.job_utils import submit_mozart_job


def validate_temporal_input(starttime, hours_delta, days_delta):
    '''

    :param starttime:
    :param hours_delta:
    :param days_delta:
    :return:
    '''
    if isinstance(hours_delta, int) and isinstance(days_delta, int):
        raise Exception("Please make sure the delta specified is a number")

    if starttime is None and hours_delta is None and days_delta is not None:
        return "{}Z".format((datetime.utcnow()-timedelta(days=days_delta)).isoformat()), "daily"
    elif starttime is None and hours_delta is not None and days_delta is None:
        return "{}Z".format((datetime.utcnow() - timedelta(hours=hours_delta)).isoformat()), "hourly"
    elif starttime is not None and hours_delta is None and days_delta is None:
        return starttime, None
    elif starttime is None and hours_delta is None and days_delta is None:
        raise Exception("None of the time parameters were specified. Must specify either start time, delta of hours"
                        " or delta of days ")
    else:
        raise Exception("only one of the time parameters should be specified. "
                        "start time: {} delta of hours:{} delta of days: {}"
                        .format(starttime, hours_delta, days_delta))


def get_job_params(job_type, job_name, starttime, endtime):

    rule = {
        "rule_name": job_type.lstrip('job-'),
        "queue": "factotum-job_worker-apihub_scraper_throttled",
        "priority": 5,
        "kwargs": '{}'
    }
    params = [
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

    if "daily" in job_name:
        add_params = [
            {
                "name": "report_flag",
                "from": "value",
                "value": "--report"
            }
        ]

        params = params + add_params
    return rule, params


if __name__ == "__main__":
    '''
    Main program that is run by cron to submit a scraper job
    '''

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("qtype", help="query endpoint, e.g. (opensearch|odata|stub)")
    parser.add_argument("--dataset_version", help="version of acquisition dataset, e.g. v1.1")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--days", help="Delta in days", nargs='?',
                        type=int, required=False)
    group.add_argument("--hours", help="Delta in hours", nargs='?',
                        type=int, required=False)
    group.add_argument("--starttime", help="Start time in ISO8601 format", nargs='?', required=False)
    parser.add_argument("--endtime", help="End time in ISO8601 format", nargs='?',
                        default="%sZ" % datetime.utcnow().isoformat(), required=False)
    parser.add_argument("--tag", help="PGE docker image tag (release, version, " +
                                      "or branch) to propagate",
                        default="master", required=True)
    parser.add_argument("--polygon", required=False)

    args = parser.parse_args()
    qtype = args.qtype
    dataset_version = args.dataset_version
    days_delta = args.days
    hours_delta = args.hours
    starttime = args.starttime
    endtime = args.endtime
    tag = args.tag

    starttime, job_name = validate_temporal_input(starttime, hours_delta, days_delta)

    rtime = datetime.utcnow()

    if days_delta is not None:
        job_type = "job-acquisition_ingest-scihub_daily"
        job_spec = "{}:{}".format(job_type, tag)
        job_name = "%s-%s-%s-%s-%s" % (job_spec, starttime.replace("-", "").replace(":", ""),
                                       "daily", endtime.replace("-", "").replace(":", ""),
                                       rtime.strftime("%d_%b_%Y_%H:%M:%S"))
    elif hours_delta is not None:
        job_type = "job-acquisition_ingest-scihub"
        job_spec = "{}:{}".format(job_type, tag)
        job_name = "%s-%s-%s-%s-%s" % (job_spec, starttime.replace("-", "").replace(":", ""),
                                       "hourly",endtime.replace("-", "").replace(":", ""),
                                       rtime.strftime("%d_%b_%Y_%H:%M:%S"))
    else:
        job_type = "job-acquisition_ingest-scihub"
        job_spec = "{}:{}".format(job_type, tag)
        job_name = "%s-%s-%s-%s" % (job_spec, starttime.replace("-", "").replace(":", ""),
                                    endtime.replace("-", "").replace(":", ""),
                                    rtime.strftime("%d_%b_%Y_%H:%M:%S"))

    job_name = job_name.lstrip('job-')

    # Setup input arguments here
    rule, params = get_job_params(job_type, job_name, starttime, endtime)

    print("submitting job of type {} for {}".format(job_spec, qtype))
    submit_mozart_job({}, rule,
        hysdsio={"id": "internal-temporary-wiring",
                 "params": params,
                 "job-specification": job_spec},
        job_name=job_name)
