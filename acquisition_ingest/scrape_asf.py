from builtins import str
from builtins import range
import copy
import logging
import json
import geojson
from hysds.celery import app
from hysds.dataset_ingest import ingest
import requests
import re
import shapely
import os
import traceback
import shutil


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'):
            record.id = '--'
        return True


logger = logging.getLogger('scrape_asf')
logger.setLevel(logging.INFO, logging.DEBUG)
logger.addFilter(LogFilter())

DATASET_VERSION = "v2.0"
DEFAULT_ACQ_TYPE = "NOMINAL"
SCIHUB_URL_TEMPLATE = "https://scihub.copernicus.eu/apihub/odata/v1/Products('$id')/"
SCIHUB_DOWNLOAD_URL = "https://scihub.copernicus.eu/apihub/odata/v1/Products('$id')/$value"
ICON_URL = "https://scihub.copernicus.eu/apihub/odata/v1/Products('$id')/Products('Quicklook')/$value"
failed_publish = list()

PLATFORM_NAME = {
    "Sentinel-1A": "Sentinel-1",
    "Sentinel-1B": "Sentinel-1"
}

INSTRUMENT_NAME = {
    "SENTINEL-1 C-SAR": "Synthetic Aperture Radar (C-band)",
    "Sentinel-1B": "Synthetic Aperture Radar (C-band)",
    "TerraSAR-X-1": "Synthetic Aperture Radar (X-band)",
    "TanDEM-X-1": "Digital Elevation Model",
    "ALOS-2": "Phased-Array L-band Synthetic Aperture Radar - 2"
}

INSTRUMENT_SHORT_NAME = {
    "SENTINEL-1 C-SAR": "SAR-C SAR",
    "Sentinel-1B": "SAR-C SAR",
    "TerraSAR-X-1": "SAR-X SAR",
    "TanDEM-X-1": "TanDEM",
    "ALOS-2": "PALSAR-2"
}


def get_existing_acqs(start_time, end_time, location=False):
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

    if location:
        geo_shape = {
                    "geo_shape": {
                        "location": {
                            "shape": location
                        }
                    }
                }
        query["query"]["filtered"]["filter"] = geo_shape

    acq_ids = []
    rest_url = app.conf["GRQ_ES_URL"][:-1] if app.conf["GRQ_ES_URL"].endswith('/') else app.conf["GRQ_ES_URL"]
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
        acq_ids.append(item.get("_id"))

    return acq_ids


def not_RAW(product_name):
    match = re.search(r'([\w.-]+)_([\w.-]+)_([\w.-]+)__([\d])([\w])([\w.-]+)', product_name)
    if match:
        product_type = match.group(3)

        if product_type == "RAW":
            return False
        else: return True
    else:
        return True


def get_area(coords):
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    return area / 2.0


def make_clockwise(coords):
    '''returns the coordinates in clockwise direction. takes in a list of coords.'''
    if get_area(coords) > 0:
        coords = coords[::-1] # switch direction if not clockwise
    return coords


def get_platform_name(mission):
    if mission in PLATFORM_NAME:
        platform_name = PLATFORM_NAME[mission]
    else:
        platform_name = mission

    return platform_name


def get_platform_identifier(satellite_id):
    return satellite_id


def get_product_class(satellite_name, product_name):
    product_type = None
    processing_level = None
    product_class = None

    if satellite_name == "Sentinel-1A" or satellite_name == "Sentinel-1B":
        # sample name of S1 file : S1A_IW_SLC__1SDV_20150909T163711_20150909T163746_007640_00A97E_A69D
        match = re.search(r'([\w.-]+)_([\w.-]+)_([\w.-]+)__([\d])([\w])([\w.-]+)', product_name)
        if match:
            product_type = match.group(3)
            processing_level = match.group(4)
            product_class = match.group(5)

    return product_class, product_type, processing_level


def get_sensor_file_name(sensor):
    if "(" in sensor:
        sensor = sensor[sensor.find("(") + 1:sensor.find(")")]
    sensor = sensor.replace(" ", "_")

    return sensor


def get_start_timestamp(start_time):
    start_time = start_time.replace("-","")
    start_time = start_time.replace(":","")
    return start_time


def valid_es_geometry(geometry):
    es_json = copy.deepcopy(geometry)

    # now for any meridian crossing polygons, add 360 to longitude
    # less than 0, since ES can handle 0 to 360
    # if is_anti_meridian(es_json):
    #     index = 0
    #     for coord in es_json["coordinates"][0]:
    #         if float(coord[0]) < 0:
    #             es_json["coordinates"][0][index][0] = float(coord[0]) + 360
    #         index +=1

    size = len(es_json["coordinates"][0])

    if es_json["coordinates"][0][size - 1] == es_json["coordinates"][0][size - 2]:
        del es_json["coordinates"][0][size - 1]

    es_json["coordinates"][0] = make_clockwise(es_json["coordinates"][0])
    return es_json


def get_polygon(wkt_polygon):
    wkt_geom = shapely.wkt.loads(wkt_polygon)
    polygon = geojson.Feature(geometry=wkt_geom, properties={})
    return polygon.geometry


def get_gml(wkt_polygon):
    gml = "<gml:Polygon srsName=\"http://www.opengis.net/gml/srs/epsg.xml#4326\" " \
          "xmlns:gml=\"http://www.opengis.net/gml\">\n  " \
          " <gml:outerBoundaryIs>\n      <gml:LinearRing>\n         <gml:coordinates>"

    start_pos = wkt_polygon.find("POLYGON((") + len("POLYGON((")
    end_pos = wkt_polygon.find("))")
    gml = gml + wkt_polygon[start_pos: end_pos]
    gml = gml + "</gml:coordinates>\n      </gml:LinearRing>\n   </gml:outerBoundaryIs>\n</gml:Polygon>"
    return gml


def get_instrument_name(satellite):
    instrument_name = satellite
    instrument_short_name = satellite
    if satellite in INSTRUMENT_NAME:
        instrument_name = INSTRUMENT_NAME[satellite]
    if satellite in INSTRUMENT_SHORT_NAME:
        instrument_short_name = INSTRUMENT_SHORT_NAME[satellite]

    return instrument_name, instrument_short_name


def make_met_file(record):

    metadata = dict()

    metadata["acquisitiontype"] = DEFAULT_ACQ_TYPE
    metadata["archive_filename"] = record["granuleName"] + ".zip"
    metadata["location"] = valid_es_geometry(get_polygon(record["stringFootprint"]))
    # metadata["bbox"] = metadata["location"]["coordinates"][0]
    if record["flightDirection"] == "ASCENDING":
        direction = "asc"
    elif record["flightDirection"] == "DESCENDING":
        direction = "dsc"
    else:
        direction = "N/A"
    metadata["direction"] = direction
    metadata["look_direction"] = record["lookDirection"]
    metadata["browse_url"] = record["browse"]
    # metadata["satellite_id"] = record["properties"]["satellite_id"]
    # metadata["agency"] = record["properties"]["agency"]
    # metadata["launch_date"] = record["properties"]["launch_date"]
    # metadata["eol_date"] = record["properties"]["eol_date"]
    # metadata["repeat_cycle"] = record["properties"]["repeat_cycle"]
    metadata["download_url"] = record["downloadUrl"]
    metadata["filename"] = record["granuleName"] + ".SAFE"
    metadata["format"] = "SAFE"
    metadata["footprint"] = record["stringFootprint"]
    metadata["gmlfootprint"] = get_gml(record["stringFootprint"])
    # metadata["icon"] = Template(ICON_URL).safe_substitute(id=record["properties"]["alt_identifier"])
    metadata["id"] = record["properties"]["alt_identifier"]
    metadata["identifier"] = record["sceneId"]
    metadata["asf_ingestion_time"] = record["processingDate"]
    metadata["instrumentname"], metadata["instrumentshortname"] = \
        get_instrument_name(record["sensor"])
    metadata["lastorbitnumber"] = record["absoluteOrbit"]
    metadata["lastrelativeorbitnumber"] = record["relativeOrbit"]
    # metadata["missiondatatakeid"] = record["properties"]["scene_id"]
    metadata["orbitNumber"] = record["absoluteOrbit"]
    metadata["platform"] = record["platform"]
    # metadata["platformidentifier"] = get_platform_identifier(record["sensor"],
    #                                                          record["properties"]["satellite_id"])
    metadata["platformname"] = get_platform_name(record["platform"])
    metadata["polarisationmode"] = record["polarization"].replace('+', ' ')
    metadata["productclass"], metadata["producttype"], metadata["processing_level"] = \
        get_product_class(record["platform"], record["granuleName"])
    metadata["query_api"] = "asf_api_search"
    metadata["sensingStart"] = record["startTime"]

    metadata["sensingStop"] = record["stopTime"]
    metadata["sensoroperationalmode"] = record["beamMode"]
    metadata["slicenumber"] = record["frameNumber"]
    metadata["status"] = "ARCHIVED"
    metadata["summary"] = "Date: " + metadata["sensingStart"] + ", Instrument: " + metadata[
        "platformname"] + ", Mode: " + metadata["polarisationmode"] + ", Satellite: " + metadata[
                              "platform"] + ", Size (in MB): " + record["sizeMB"]
    metadata["size"] = record["sizeMB"] + " MB"
    metadata["swathidentifier"] = record["beamSwath"]
    metadata["title"] = record["granuleName"]
    metadata["track_number"] = record["track"]
    # metadata["uuid"] = record["properties"]["alt_identifier"]
    metadata["source"] = "asf"

    folder_name = "acquisition-" + str(metadata["platform"]) + "_" \
                  + str(get_start_timestamp(metadata["sensingStart"])) + "_" \
                  + str(metadata["track_number"]) + "_"\
                  + str(get_sensor_file_name(metadata["sensoroperationalmode"]))\
                  + "-asf"

    dataset_name = folder_name

    try:
        print("Creating Dataset for {}".format(folder_name))
        os.makedirs(folder_name, 493)
        met_file = open("%s/%s.met.json" % (folder_name, dataset_name), 'w')
        met_file.write(json.dumps(metadata))
        met_file.close()
    except Exception as ex:
        print("Failed to create dataset for {}. Because {}. {}".format(folder_name, ex.message, traceback.format_exc()))
        logger.warn("Failed to create dataset for {}. Because {}. {}".format(folder_name, ex.message, traceback.format_exc()))

    return folder_name


def make_dataset_file(product_name, record, starttime = None, endtime = None):
    folder_name = product_name
    dataset = dict()

    if starttime is None and endtime is None:
        dataset["endtime"] = record["startTime"]
        dataset["starttime"] = record["stopTime"]
    else:
        dataset["starttime"] = starttime
        dataset["endtime"] = endtime

    dataset["label"] = product_name
    dataset["location"] = valid_es_geometry(get_polygon(record["geometry"]))
    dataset["version"] = DATASET_VERSION

    dataset_file = open("%s/%s.dataset.json" % (folder_name, product_name), 'w')
    dataset_file.write(json.dumps(dataset))
    dataset_file.close()


def create_dataset_from_asf(record):
    product_name = record["granuleName"]
    if not_RAW(product_name):
        product_name = make_met_file(record)
        make_dataset_file(product_name, record)


def ingest_acq_dataset(starttime, endtime, ds_cfg ="/home/ops/verdi/etc/datasets.json"):
    """Ingest acquisition dataset."""
    existing = get_existing_acqs(starttime, endtime)
    for dir in os.listdir('.'):
        if os.path.isdir(dir):
            id = dir
            if id.startswith("acquisition-"):
                if id.replace("-asf", "-esa_scihub") not in existing:
                    try:
                        ingest(id, ds_cfg, app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE, dir, None)
                        shutil.rmtree(id)
                    except Exception as e:
                        print("Failed to ingest dataset {}".format(id))
                        failed_publish.append(id)
    return


def scrape_asf(start_time, end_time):
    try:
        # query the asf search api to find the download url for the .iso.xml file
        request_string = 'https://api.daac.asf.alaska.edu/services/search/param?platform=SA,SB&processingLevel=METADATA_SLC' \
                         '&start={}&end={}&output=json'.format(start_time, end_time)
        logger.info("ASF request URL: {}".format(request_string))
        response = requests.get(request_string)
        response.raise_for_status()
        if response.status_code != 200:
            raise Exception("Request to ASF failed with status {}. {}".format(response.status_code, request_string))
        results = json.loads(response.text)
        logger.debug("Response from ASF: {}".format(response.text))

        # parse the json and map to scihub fields
        for result in results[0]:
            st = result["startTime"]
            et = result["stopTime"]
            st_ms_pos = st.rfind(".")
            et_ms_pos = et.rfind(".")

            # changing datetimes to have 3 digits of ms
            if len(st) - st_ms_pos > 3:
                result["startTime"] = st[:st_ms_pos + 3]
            if len(et) - et_ms_pos > 3:
                result["stopTime"] = et[:et_ms_pos + 3]

            create_dataset_from_asf(result)
        ingest_acq_dataset(start_time, end_time)
    except Exception as err:
        logger.info("Failed to ingest acquisitions from ASF : %s. List of failed acquistions" % str(err))
        raise Exception("Failed to ingest acquisitions from ASF : %s" % str(err))
    return


def main():
    try:
        context = open("_context.json", "r")
        ctx = json.loads(context.read())
        start_time = ctx.get("starttime")
        end_time = ctx.get("endtime")
        scrape_asf(start_time, end_time)
    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    return


if __name__ == "__main__":
    main()
