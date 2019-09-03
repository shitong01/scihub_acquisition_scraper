# SciHub/ApiHub Scraper
This repository contains all the infrastructure for 2 components of the Upstream Ancillary Ingest.
1. Acquisition Ingest
2. IPF Scrape

The set of scripts here provide functionality for:
- Keep up mode
- On demand
- Catch up

We are following the concept of having consolidated scripts and wrapper scripts to address all the use cases.
The consolidated scripts are :
- `acquisition_ingest/scrape_apihub_opensearch.py` : performs the acquisition ingest from ESA's scihub
- `ipf_scrape/ipf_version.py` : performs the IPF scrape from either ESA SciHub or ASF based on acquisition date. 


## Keep Up
All the keep up is done with cron jobs. Those can be found under `crons`.
The jobs used for keep up are `job-acquisition-ingest-scihub` and `job-AOI_based_ipf_submitter`.
All keep up jobs are best effort and global.


## On Demand
The job types that can be used on demand are:
- `job-acquisition_ingest-aoi` : Ingest missing acquisitions for an AOI (faceted on).
- `job-acquisition_ingest-scihub`: Ingest acquisitions from SciHub. Takes start time and end time as input.
- `job-acquisition_ingest_by_id-scihub`: Ingest an acquisition given an SLC Id (Input from user).
- `job-aoi_based_acq_submitter`: Wrapper script that submits `acquisition_ingest-aoi` type jobs for all faceted AOIs.
- `job-aoi_validate_acquisitions`: Validates if all acquisitions exist in SDS, given AOI. If missing then reports and ingests them.
- `job-ipf-scraper-asf`: Finds and fills IPF version for acquisitions missing it from ASF.
- `job-ipf-scraper-scihub`: Finds and fills IPF version for acquisitions missing it from SciHub.
- `job-acquisition_ingest-asf` : Ingest acquisitions from ASF. Takes start time and end time as input. Not used in current operational system.

## Catch Up
These scripts can be found under the `ops_scripts`.
