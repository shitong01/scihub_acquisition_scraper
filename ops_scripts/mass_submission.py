import datetime
import time
from hysds_commons.job_utils import submit_mozart_job

"""
This script is meant to submit acq scrape jobs for a time period
"""
def dates(start_date, end_date):
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    dates = list()
    d = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    delta = datetime.timedelta(days=1)
    while d <= end_date:
        dates.append(datetime.datetime.strftime(d, "%Y-%m-%dT%H:%M:%S.%fZ"))
        d += delta
    return dates


def submit_job(start_time, end_time):

    tag = "master"
    ds_es_url = "http://128.149.127.152:9200/grq_v2.0_acquisition-s1-iw_slc/acquisition-S1-IW_SLC"
    job_spec = "job-acquisition_ingest-scihub:{}".format(tag) #job-acquisition_ingest-scihub:dev-malarout
    job_name = "%s-%s-%s" % (job_spec, start_time.replace("-", "").replace(":", ""),
                             end_time.replace("-", "").replace(":", ""))

    # Setup input arguments here
    rule = {
        "rule_name": "acquistion_ingest-scihub",
        "queue": "factotum-job_worker-apihub_scraper_throttled",
        "priority": 0,
        "kwargs": '{}'
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
            "value": start_time
        },
        {
            "name": "endtime",
            "from": "value",
            "value": end_time
        },
        {
            "name": "ingest_flag",
            "from": "value",
            "value": "--ingest"
        }
    ]

    id = submit_mozart_job({}, rule,
                      hysdsio={"id": "internal-temporary-wiring",
                               "params": params,
                               "job-specification": job_spec},
                      job_name=job_name)

    print("Submitted job for window {} to {}, JOB ID: {}".format(start_time, end_time, id))
    # print("Submitted job for window {} to {}".format(start_time, end_time, id))
7
if __name__ == "__main__":
    '''
    Main program that is run by to submit a scraper jobs
    '''

    start_date = "2015-05-02"
    end_date = "2016-11-01"

    dates = dates(start_date, end_date)
    for i in range(len(dates) - 1):
        time.sleep(60)
        submit_job(dates[i], dates[i + 1])










