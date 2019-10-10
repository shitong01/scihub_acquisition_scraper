#!/usr/bin/env python
from __future__ import print_function
from __future__ import absolute_import
import os, sys, re, json, requests, logging, traceback, argparse, hashlib
from datetime import datetime

from hysds_commons.job_utils import submit_mozart_job
from hysds.celery import app

from .sling_acquisition import get_date


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('sling_acquisition_list')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


def submit_sling(ctx_file):
    """Submit sling for S1 SLC from acquisition."""

    # get context
    with open(ctx_file) as f:
        ctx = json.load(f)
    logger.info("ctx: {}".format(json.dumps(ctx, indent=2)))

    # get ES url
    es_url = app.conf.GRQ_ES_URL
    index = "grq"

    # get ids
    ids = ctx['ids']

    # build query
    query = {
        "query": {
            "ids" : {
                "type" : "acquisition-S1-IW_SLC",
                "values" : ids,
            }
        },
        "partial_fields" : {
            "partial" : {
                "exclude" : ["city", "context", "metadata.context"],
            }
        }
    }

    # query
    r = requests.post("%s/grq_*_acquisition-s1-iw_slc/_search?search_type=scan&scroll=60&size=100" % es_url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    scroll_id = scan_result['_scroll_id']
    matches = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        matches.extend([i['fields']['partial'][0] for i in res['hits']['hits']])
    #logger.info("matches: {}".format([m['_id'] for m in matches]))
    logger.info("matches: {}".format(len(matches)))
    #logger.info("matches[-1]: {}".format(json.dumps(matches[-1], indent=2)))

    #required params for job submission
    qtype = "scihub"
    job_type = "job:spyddder-sling_%s" % qtype
    oauth_url = None
    queue = "factotum-job_worker-%s_throttled" % qtype # job submission queue

    # loop over acquisitions and submit sling jobs
    for res in matches:
        id = res['id']

        # filter non acquisitions
        if res.get('dataset', None) != "acquisition-S1-IW_SLC":
            logger.info("Skipping invalid acquisition dataset: {}".format(id))

        # get metadata
        md = res['metadata']

        # build payload items for job submission
        archive_fname = md['archive_filename']
        title, ext = archive_fname.split('.')
        start_dt = get_date(res['starttime'])
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
            'coordinates': res['location']['coordinates'],
        }
        prod_met['tag'] = []


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
              "value": md['download_url'],
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
    parse = argparse.ArgumentParser(description="Sling S1 SLC from acquisition list.")
    parse.add_argument("-c", "--ctx_file", help="context JSON file", default="_context.json")
    args = parse.parse_args()
    submit_sling(args.ctx_file)
