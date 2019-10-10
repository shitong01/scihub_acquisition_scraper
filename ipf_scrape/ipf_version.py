from builtins import str
import json
from lxml.etree import fromstring, ElementTree
import re
import requests
import logging
import elasticsearch
import traceback
import sys
from hysds.celery import app

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True


logger = logging.getLogger('ipf_scrape')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

es_url = app.conf["GRQ_ES_URL"]
_index = None
_type = None
ES = elasticsearch.Elasticsearch(es_url)


def check_ipf_avail(id):
    result = ES.search(index="grq",body={"query": {"term": {"_id": id}}})
    ipf_version = result.get("hits").get("hits")[0].get("_source").get("metadata").get("processing_version", None)

    if ipf_version is not None:
        return True
    else:
        return False


def check_prod_avail(session, link):
    """
    check if product is currently available or in long time archive
    :param session:
    :param link:
    :return:
    """

    product_url = "{}$value".format(link)
    response = session.head(product_url, verify=False, timeout=180)

    return response.status_code


def get_scihub_manifest(session, info):
    """Get manifest information."""

    # disable extraction of manifest (takes too long); will be
    # extracted when needed during standard product pipeline

    # logger.info("info: {}".format(json.dumps(info, indent=2)))
    manifest_url = "{}Nodes('{}')/Nodes('manifest.safe')/$value".format(info['met']['alternative'],
                                                                             info['met']['filename'])
    manifest_url2 = manifest_url.replace('/apihub/', '/dhus/')
    for url in (manifest_url2, manifest_url):
        response = session.get(url, verify=False, timeout=180)
        logger.info("url: %s" % response.url)
        if response.status_code == 200:
            break
    response.raise_for_status()
    return response.content


def get_scihub_namespaces(xml):
    """Take an xml string and return a dict of namespace prefixes to
       namespaces mapping."""

    nss = {}
    matches = re.findall(r'\s+xmlns:?(\w*?)\s*=\s*[\'"](.*?)[\'"]', xml)
    for match in matches:
        prefix = match[0]; ns = match[1]
        if prefix == '': prefix = '_default'
        nss[prefix] = ns
    return nss


def get_scihub_ipf(manifest):
    # append processing version (ipf)
    ns = get_scihub_namespaces(manifest)
    x = fromstring(manifest)
    ipf = x.xpath('.//xmlData/safe:processing/safe:facility/safe:software/@version', namespaces=ns)[0]

    return ipf


def get_dataset_json(met, version):
    """Generated HySDS dataset JSON from met JSON."""

    return {
        "version": version,
        "label": met['id'],
        "location": met['location'],
        "starttime": met['sensingStart'],
        "endtime": met['sensingStop'],
    }


def extract_asf_ipf(id):
    ipf = None
    try:
        # query the asf search api to find the download url for the .iso.xml file
        request_string = 'https://api.daac.asf.alaska.edu/services/search/param?platform=SA,SB&processingLevel=METADATA_SLC' \
                         '&granule_list=%s&output=json' % id
        logger.info("ASF request URL: {}".format(request_string))
        response = requests.get(request_string)
        response.raise_for_status()
        results = json.loads(response.text)
        logger.info("Response from ASF: {}".format(response.text))
        # download the .iso.xml file, assumes earthdata login credentials are in your .netrc file
        if len(results[0]) == 0:
            raise Exception("Acquisition not found at ASF.")
        response = requests.get(results[0][0]['downloadUrl'])
        response.raise_for_status()
        if response.status_code != 200:
            raise Exception("Request to ASF failed with status {}.".format(response.status_code))
        # parse the xml file to extract the ipf version string
        root = fromstring(response.text.encode('utf-8'))
        ns = {'gmd': 'http://www.isotc211.org/2005/gmd', 'gmi': 'http://www.isotc211.org/2005/gmi',
              'gco': 'http://www.isotc211.org/2005/gco'}
        try:
            ipf_string = root.find(
                'gmd:composedOf/gmd:DS_DataSet/gmd:has/gmi:MI_Metadata/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:processStep/gmd:LI_ProcessStep/gmd:description/gco:CharacterString',
                ns).text
        except AttributeError:
            raise Exception("IPF not found in XML from download URL. Failed to extract IPF version from ASF.")
        if ipf_string:
            ipf = ipf_string.split('version')[1].split(')')[0].strip()
    except Exception as err:
        logger.info("get_processing_version_from_asf: %s" % str(err))
        raise Exception("{}".format(str(err)))

    return ipf


def update_ipf(id, ipf_version):
    logger.info("Updating IPF Version of {}. IPF Version: {}".format(id, ipf_version))
    ES.update(index=_index, doc_type=_type, id=id,
              body={"doc": {"metadata": {"processing_version": ipf_version}}})


def extract_scihub_ipf(met):
    user = None
    password = None

    # get session
    session = requests.session()
    if None not in (user, password): session.auth = (user, password)

    ds = get_dataset_json(met, version="v2.0")

    info = {
        'met': met,
        'ds': ds
    }

    prod_avail = check_prod_avail(session, info['met']['alternative'])
    if prod_avail == 200:
        manifest = get_scihub_manifest(session, info)
    elif prod_avail == 202:
        logger.info("Got 202 from SciHub. Product moved to long term archive.")
        raise Exception("Got 202. Product moved to long term archive.")
    elif prod_avail == 503:
        logger.info("Exceeding max concurrent SciHub connections.")
        raise Exception("Exceeding max concurrent SciHub connections.")
    else:
        logger.info("Got code {} from SciHub".format(prod_avail))
        raise Exception("Got code {}".format(prod_avail))

    ipf = get_scihub_ipf(manifest)
    return ipf


if __name__ == "__main__":
    '''
    Main program that find IPF version for acquisition
    '''
    ctx = json.loads(open("_context.json", "r").read())
    id = ctx["acq_id"]
    met = ctx["acq_met"]
    _index = ctx.get("index")
    _type = ctx.get("dataset_type")
    endpoint = ctx["endpoint"]

    if check_ipf_avail(id):
        logger.error("Acquisition already has IPF, not processing with scraping")
        with open('_alt_error.txt', 'w') as f:
            f.write("Acquisition already has IPF, not proceeding with scraping for {}".format(id))
            f.close()
        sys.exit(1)
    
    if endpoint == "asf":
        try:
            ipf = extract_asf_ipf(met.get("identifier"))
            if ipf is None:
                raise Exception("Found null IPF")
        except Exception as ex:
                with open('_alt_error.txt', 'w') as f:
                    f.write("{}".format(ex))
                with open('_alt_traceback.txt', 'w') as f:
                    f.write("Failed to get IPF for {}. \n{}. \n {}".format(id, ex, traceback.format_exc()))
                raise Exception("Failed to get IPF for {}. {}.".format(id, ex))
    else:
        try:
            ipf = extract_scihub_ipf(met)
            if ipf is None:
                raise Exception("Found null IPF")
        except Exception as ex:
            with open('_alt_error.txt', 'w') as f:
                f.write("{}".format(ex))
            with open('_alt_traceback.txt', 'w') as f:
                f.write("Failed to get IPF for {}. \n{}. \n {}".format(id, ex, traceback.format_exc()))
            raise Exception("Failed to get IPF for {}. {}.".format(id, ex))

    update_ipf(id, ipf)
