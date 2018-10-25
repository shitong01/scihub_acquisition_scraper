# SciHub/ApiHub Scraper

## Usage
```
usage: scrape_apihub_opensearch.py [-h] [--dataset_version DATASET_VERSION]
                                   [--user USER] [--password PASSWORD]
                                   [--email EMAIL [EMAIL ...]] [--browse]
                                   [--ingest | --create_only]
                                   ds_es_url datasets_cfg [starttime]
                                   [endtime]

Query ApiHub (OpenSearch) for all S1 SLC scenes globally and create
acquisition datasets.

positional arguments:
  ds_es_url             ElasticSearch URL for acquisition dataset, e.g. http
                        ://aria-products.jpl.nasa.gov:9200/grq_v1.1_acquisitio
                        n-s1-iw_slc/acquisition-S1-IW_SLC
  datasets_cfg          HySDS datasets.json file, e.g.
                        /home/ops/verdi/etc/datasets.json
  starttime             Start time in ISO8601 format
  endtime               End time in ISO8601 format

optional arguments:
  -h, --help            show this help message and exit
  --dataset_version DATASET_VERSION
                        dataset version
  --user USER           SciHub user
  --password PASSWORD   SciHub password
  --email EMAIL [EMAIL ...]
                        email addresses to send email to
  --browse              create browse images
  --ingest              create and ingest missing datasets
  --create_only         only create missing datasets
```

## To run ingest of missing acquisitions for a certain date range::
```
./scrape_apihub_opensearch.py \
  http://datasets.grfn.hysds.net:9200/grq_v1.1_acquisition-s1-iw_slc/acquisition-S1-IW_SLC \
  ~/verdi/etc/datasets.json \
  2017-04-06T00:00:00.0Z 2017-04-06T01:00:00.0Z \
  --user <username> --password <password> --ingest
```

## To run creation of missing acquisitions (no ingest) for a certain date range::
```
./scrape_apihub_opensearch.py \
  http://datasets.grfn.hysds.net:9200/grq_v1.1_acquisition-s1-iw_slc/acquisition-S1-IW_SLC \
  ~/verdi/etc/datasets.json --create
```
