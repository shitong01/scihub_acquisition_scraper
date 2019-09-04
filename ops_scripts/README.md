## Operator Script

The collection of scripts here have been used on occasion to fix something in this subsystem.

- `catchup.py`: In case we need to catch up on acquisitions, this script can be run. It submits a `job-acquisition-ingest-scihub` job per day. Update the `mis_date` and run. It'll back fill acquisitions from then till now.
- `correct_start_endtimes.py`: This script was used to update the discrepancy in the metadata start and end times of acquisitions. We found some acquisitions in 2016 and 2015, where ESA had incorrect metadata timestamps. This script extracts the timestamp from the filename, compares it to the metadata and corrects if they don't match.
- `mass_submission.py`: This script can be used to do a back fill. Given a start and end time it submits a `job-acquisition-ingest-scihub` job per day in the period provided.