#!/usr/bin/env python
from __future__ import print_function
import os, sys, re, json, requests, logging, traceback, argparse, hashlib
from datetime import datetime

from hysds_commons.job_utils import submit_mozart_job
from hysds.celery import app


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('sling_acquisition')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


def get_date(dt):
    """Return datetime from string."""

    return datetime.strptime(dt[:-1] if dt.endswith('Z') else dt,
                             "%Y-%m-%dT%H:%M:%S.%f")
    

def submit_sling(ctx_file):
    """Submit sling for S1 SLC from acquisition."""

    # get context
    with open(ctx_file) as f:
        ctx = json.load(f)
    logger.info("ctx: {}".format(json.dumps(ctx, indent=2)))

    # filter non acquisitions
    if ctx.get('source_dataset', None) != "acquisition-S1-IW_SLC":
        raise RuntimeError("Skipping invalid acquisition dataset.")

    # build payload items for job submission
    qtype = "scihub"
    archive_fname = ctx['archive_filename']
    title, ext = archive_fname.split('.')
    start_dt = get_date(ctx['starttime'])
    yr = start_dt.year
    mo = start_dt.month
    dy = start_dt.day
    logger.info("starttime: {}".format(start_dt))
    md5 = hashlib.md5("{}\n".format(archive_fname)).hexdigest()
    repo_url = "{}/{}/{}/{}/{}/{}".format(ctx['repo_url'], md5[0:8], md5[8:16],
                                          md5[16:24], md5[24:32], archive_fname)
    logger.info("repo_url: {}".format(repo_url))
    prod_met = {}
    prod_met['source'] = qtype
    prod_met['dataset_type'] = title[0:3]
    prod_met['spatial_extent'] = {
        'type': 'polygon',
        'aoi': None,
        'coordinates': ctx['prod_met']['location']['coordinates'],
    }
    prod_met['tag'] = []

    #required params for job submission
    job_type = "job:spyddder-sling_%s" % qtype
    oauth_url = None
    queue = "factotum-job_worker-%s_throttled" % qtype # job submission queue

    #set sling job spec release/branch
    #job_spec = "job-sling:release-20170619"
    job_spec = "job-sling:{}".format(ctx['sling_release'])
    rtime = datetime.utcnow()
    job_name = "%s-%s-%s-%s" % (job_spec, queue, archive_fname, rtime.strftime("%d_%b_%Y_%H:%M:%S"))
    job_name = job_name.lstrip('job-')

    #Setup input arguments here
    rule = {
        "rule_name": job_spec,
        "queue": queue,
        "priority": ctx.get('job_priority', 0),
        "kwargs":'{}'
    }
    params = [
        { "name": "download_url",
          "from": "value",
          "value": ctx['download_url'],
        },
        { "name": "repo_url",
          "from": "value",
          "value": repo_url,
        },
        { "name": "prod_name",
          "from": "value",
          "value": title,
        },
        { "name": "file_type",
          "from": "value",
          "value": ext,
        },
        { "name": "prod_date",
          "from": "value",
          "value": "{}".format("%04d-%02d-%02d" % (yr, mo, dy)),
        },
        { "name": "prod_met",
          "from": "value",
          "value": prod_met,
        },
        { "name": "options",
          "from": "value",
          "value": "--force_extract"
        }
    ]

    logger.info("rule: {}".format(json.dumps(rule, indent=2)))
    logger.info("params: {}".format(json.dumps(params, indent=2)))

    submit_mozart_job({}, rule,
        hysdsio={"id": "internal-temporary-wiring",
                 "params": params,
                 "job-specification": job_spec},
        job_name=job_name)



if __name__ == "__main__":
    parse = argparse.ArgumentParser(description="Sling S1 SLC from acquisition.")
    parse.add_argument("-c", "--ctx_file", help="context JSON file", default="_context.json")
    args = parse.parse_args()
    submit_sling(args.ctx_file)
