# Crons for Upstream Ancillary Ingest
Here you can find all the cron scripts being used for the ancillary upstream ingest keepup.
### acq_ingest_cron.py
This script submits job-acquisition-ingest-scihub jobs on a daily and hourly basis.


Hourly submits the job-acquisition-ingest-scihub job with the appropriate time segmentation params.
Hourlies are submitted as “best effort” jobs. If they fail, no need to auto-retry as they will be resubmitted within the next hour like the hourly and dailies
Passes params to Acquisitions Scraper:

- start and end timestamps: last 5-hours

- polygon: (global for keep-up or use AOI-specific bbox)

- publish report: disabled

Daily submits the job-acquisition-ingest-scihub job with the appropriate time segmentation params.
Dailies are submitted as “best effort” jobs. If they fail, no need to auto-retry as they will be resubmitted within the next hour like the hourly and dailies
Passes params to Acquisitions Scraper:

- start and end timestamps: last 5-days

- polygon: (global for keep-up or use AOI-specific bbox)

- publish report: enabled

### ipf_global_cron.py

This script submits job-AOI_based_ipf_submitter to find all acquisitions globally with  missing IPF versions and fill them in.

Passes params to IPF Scraper:

- start and end timestamps: last 5-hours

- polygon: (global for keep-up)


## Crontab setting
The crontab-settings.txt has the crontab settings for the acquisition ingest and ipf scrape jobs.
