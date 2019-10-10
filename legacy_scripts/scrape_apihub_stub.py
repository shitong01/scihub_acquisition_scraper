#!/usr/bin/env python
"""
Query ApiHub (stub) for all S1 SLC scenes globally and create
acquisition datasets.
"""

from future import standard_library
standard_library.install_aliases()
from builtins import str
import os, sys, time, re, requests, json, logging, traceback, argparse
import shutil, hashlib, getpass, tempfile, backoff
from subprocess import check_call
#import requests_cache
from datetime import datetime, timedelta
from urllib.parse import urlparse
from tabulate import tabulate
from requests.packages.urllib3.exceptions import (InsecureRequestWarning,
                                                  InsecurePlatformWarning)

import shapely.wkt
import geojson

import hysds.orchestrator
from hysds.celery import app
from hysds.dataset_ingest import ingest
from osaka.main import get

#from notify_by_email import send_email


# disable warnings for SSL verification
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)


# monkey patch and clean cache
#expire_after = timedelta(hours=1)
#requests_cache.install_cache('check_apihub', expire_after=expire_after)


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('scrape_apihub_stub')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


# global constants
url = "https://scihub.copernicus.eu/dhus/api/stub/products"
download_url = "https://scihub.copernicus.eu/apihub/odata/v1/Products('{}')/$value"
browse_url = "https://scihub.copernicus.eu/apihub/odata/v1/Products('{}')/Products('Quicklook')/$value"
dtreg = re.compile(r'S1\w_.+?_(\d{4})(\d{2})(\d{2})T.*')
QUERY_TEMPLATE = '( ingestionDate:[{0} TO {1}] ) AND ( platformname:Sentinel-1 AND producttype:SLC AND sensoroperationalmode:IW)'

# regexes
PLATFORM_RE = re.compile(r'S1(.+?)_')


def massage_result(res):
    """Massage result JSON into HySDS met json."""

    # loop over indexes
    for idx in res['indexes']:
        if idx['name'] == 'product':
            for i in idx['children']:
                if i['name'] == 'Orbit number (start)':
                    res['orbitNumber'] = int(i['value'])
                elif i['name'] == 'Relative orbit (start)':
                    res['trackNumber'] = int(i['value'])
                elif i['name'] == 'JTS footprint':
                    res['footprint'] = i['value']
                elif i['name'] == 'Pass direction':
                    if i['value'] == "DESCENDING": res['direction'] = "dsc"
                    elif i['value'] == "ASCENDING": res['direction'] = "asc"
                    else: raise RuntimeError("Failed to recognize orbit direction: %s" % i['value'])
                elif i['name'] == 'Sensing start':
                    res['sensingStart'] = i['value'].replace('Z', '')
                elif i['name'] == 'Sensing stop':
                    res['sensingStop'] = i['value'].replace('Z', '')
  
    # set links
    res['download_url'] = download_url.format(res['uuid'])
    res['icon'] = browse_url.format(res['uuid'])

    # set data_product_name and archive filename
    res['data_product_name'] = "acquisition-%s" % res['identifier']
    res['archive_filename'] = "%s.zip" % res['identifier']

    # extract footprint and save as bbox and geojson polygon
    g = shapely.wkt.loads(res['footprint'])
    res['location'] = geojson.Feature(geometry=g, properties={}).geometry
    res['bbox'] = geojson.Feature(geometry=g.envelope, properties={}).geometry.coordinates[0]

    # set platform
    match = PLATFORM_RE.search(res['identifier'])
    if not match:
        raise RuntimeError("Failed to extract iplatform: %s" % res['identifier'])
    res['platform'] = "Sentinel-1%s" % match.group(1)

    # verify track
    if res['platform'] == "Sentinel-1A":
        if res['trackNumber'] != (res['orbitNumber']-73)%175+1:
            raise RuntimeError("Failed to verify S1A relative orbit number and track number.")
    if res['platform'] == "Sentinel-1B":
        if res['trackNumber'] != (res['orbitNumber']-27)%175+1:
            raise RuntimeError("Failed to verify S1B relative orbit number and track number.")
    

def get_dataset_json(met, version):
    """Generated HySDS dataset JSON from met JSON."""

    return {
        "version": version,
        "label": met['data_product_name'],
        "location": met['location'],
        "starttime": met['sensingStart'],
        "endtime": met['sensingStop'],
    }


def create_acq_dataset(ds, met, root_ds_dir=".", browse=False):
    """Create acquisition dataset. Return tuple of (dataset ID, dataset dir)."""

    # create dataset dir
    id = met['data_product_name']
    root_ds_dir = os.path.abspath(root_ds_dir)
    ds_dir = os.path.join(root_ds_dir, id)
    if not os.path.isdir(ds_dir): os.makedirs(ds_dir, 0o755)

    # append source to met
    met['query_api'] = "stub"

    # dump dataset and met JSON
    ds_file = os.path.join(ds_dir, "%s.dataset.json" % id)
    met_file = os.path.join(ds_dir, "%s.met.json" % id)
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2, sort_keys=True)
    with open(met_file, 'w') as f:
        json.dump(met, f, indent=2, sort_keys=True)

    # create browse?
    if browse:
        browse_jpg = os.path.join(ds_dir, "browse.jpg")
        browse_png = os.path.join(ds_dir, "browse.png")
        browse_small_png = os.path.join(ds_dir, "browse_small.png")
        get(met['icon'], browse_jpg)
        check_call(["convert", browse_jpg, browse_png])
        os.unlink(browse_jpg)
        check_call(["convert", "-resize", "250x250", browse_png, browse_small_png])

    return id, ds_dir


def ingest_acq_dataset(ds, met, ds_cfg, browse=False):
    """Create acquisition dataset and ingest."""

    tmp_dir = tempfile.mkdtemp()
    id, ds_dir = create_acq_dataset(ds, met, tmp_dir, browse)
    ingest(id, ds_cfg, app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE, ds_dir, None)
    shutil.rmtree(tmp_dir)


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException,
                      max_tries=8, max_value=32)
def rhead(url):
    return requests.head(url)


def scrape(ds_es_url, ds_cfg, starttime, endtime, email_to, user=None, password=None,
           version="v1.1", ingest_missing=False, create_only=False, browse=False):
    """Query ApiHub (stub) for S1 SLC scenes and generate acquisition datasets."""

    # get session
    session = requests.session()
    if None not in (user, password): session.auth = (user, password)

    # set query
    query = QUERY_TEMPLATE.format(starttime, endtime)

    # get result count
    count_params = { 
        "filter": query, 
    }
    logger.info("count query: %s" % json.dumps(count_params, indent=2))
    response = session.get("%s/count?" % url, params=count_params, verify=False)
    logger.info("count_url: %s" % response.url)
    if response.status_code != 200:
        logger.error("Error: %s\n%s" % (response.status_code,response.text))
    response.raise_for_status()
    total_results_expected = response.json()
    logger.info("Total results expected: %s" % total_results_expected)

    # query
    prods_all = {}
    offset = 0
    loop = True
    ids_by_track = {}
    while loop:
        query_params = { 
            "filter": query, 
            "limit": 100,
            "offset": offset,
            "sortedby": "ingestiondate",
            "order": "desc"
        }
        logger.info("query: %s" % json.dumps(query_params, indent=2))
        response = session.get("%s?" % url, params=query_params, verify=False)
        logger.info("query_url: %s" % response.url)
        if response.status_code != 200:
            logger.error("Error: %s\n%s" % (response.status_code,response.text))
        response.raise_for_status()
        entries = response.json()
        if len(entries) == 0: break
        with open('res.json', 'w') as f:
            f.write(json.dumps(entries, indent=2))
        count = len(entries)
        offset += count
        loop = True if count > 0 else False
        logger.info("Found: {0} results".format(count))
        for met in entries:
            try: massage_result(met) 
            except Exception as e:
                logger.error("Failed to massage result: %s" % json.dumps(met, indent=2, sort_keys=True))
                logger.error("Extracted entries: %s" % json.dumps(entries, indent=2, sort_keys=True))
                raise
            #logger.info(json.dumps(met, indent=2, sort_keys=True))
            ds = get_dataset_json(met, version)
            #logger.info(json.dumps(ds, indent=2, sort_keys=True))
            prods_all[met['data_product_name']] = {
                'met': met,
                'ds': ds,
            }
            ids_by_track.setdefault(met['trackNumber'], []).append(met['data_product_name'])

        # don't clobber the connection
        time.sleep(3)

    # check if exists
    prods_missing = []
    prods_found = []
    for acq_id, info in prods_all.items():
        r = rhead('%s/%s' % (ds_es_url, acq_id))
        if r.status_code == 200:
            prods_found.append(acq_id)
        elif r.status_code == 404:
            #logger.info("missing %s" % acq_id)
            prods_missing.append(acq_id)
        else:
            r.raise_for_status()

    # print number of products missing
    msg = "Global data availability for %s through %s:\n" % (starttime, endtime)
    table_stats = [["total on apihub", len(prods_all)],
                   ["missing products", len(prods_missing)],
                  ]
    msg += tabulate(table_stats, tablefmt="grid")

    # print counts by track
    msg += "\n\nApiHub (stub) product count by track:\n"
    msg += tabulate([(i, len(ids_by_track[i])) for i in ids_by_track], tablefmt="grid")

    # print missing products
    msg += "\n\nMissing products:\n"
    msg += tabulate([("missing", i) for i in sorted(prods_missing)], tablefmt="grid")
    msg += "\nMissing %d in %s out of %d in ApiHub (stub)\n\n" % (len(prods_missing),
                                                           ds_es_url,
                                                           len(prods_all))
    logger.info(msg)

    # error check options
    if ingest_missing and create_only:
        raise RuntimeError("Cannot specify ingest_missing=True and create_only=True.")

    # create and ingest missing datasets for ingest
    if ingest_missing and not create_only:
        for acq_id in prods_missing:
            info = prods_all[acq_id]
            ingest_acq_dataset(info['ds'], info['met'], ds_cfg)
            logger.info("Created and ingested %s\n" % acq_id)

    # just create missing datasets
    if not ingest_missing and create_only:
        for acq_id in prods_missing:
            info = prods_all[acq_id]
            id, ds_dir = create_acq_dataset(info['ds'], info['met'], browse=browse)
            logger.info("Created %s\n" % acq_id)

    # email
    #if email_to is not None:
    #    subject = "[check_apihub] %s S1 SLC count" % aoi['data_product_name']
    #    send_email(getpass.getuser(), email_to, [], subject, msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ds_es_url", help="ElasticSearch URL for acquisition dataset, e.g. " +
                         "http://aria-products.jpl.nasa.gov:9200/grq_v1.1_acquisition-s1-iw_slc/acquisition-S1-IW_SLC")
    parser.add_argument("datasets_cfg", help="HySDS datasets.json file, e.g. " +
                         "/home/ops/verdi/etc/datasets.json")
    parser.add_argument("starttime", help="Start time in ISO8601 format", nargs='?',
                        default="%sZ" % (datetime.utcnow()-timedelta(days=1)).isoformat())
    parser.add_argument("endtime", help="End time in ISO8601 format", nargs='?',
                        default="%sZ" % datetime.utcnow().isoformat())
    parser.add_argument("--dataset_version", help="dataset version",
                        default="v1.1", required=False)
    parser.add_argument("--user", help="SciHub user", default=None, required=False)
    parser.add_argument("--password", help="SciHub password", default=None, required=False)
    parser.add_argument("--email", help="email addresses to send email to", 
                        nargs='+', required=False)
    parser.add_argument("--browse", help="create browse images", action='store_true')
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--ingest", help="create and ingest missing datasets",
                       action='store_true')
    group.add_argument("--create_only", help="only create missing datasets",
                       action='store_true')
    args = parser.parse_args()
    try:
        scrape(args.ds_es_url, args.datasets_cfg, args.starttime, args.endtime,
               args.email, args.user, args.password, args.dataset_version,
               args.ingest, args.create_only, args.browse)
    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
