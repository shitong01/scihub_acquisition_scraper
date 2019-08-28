# Acquisition ingest job submission lambda_funcs
This lambda was added for future replacement of the acquisition ingest cron settings on factotum.
The idea was to use scheduled lambdas.

## adding external libraries
```
cd lambda_scihub_scraper
pip install -t <lib> .
```

For example to install `requests`
```
cd lambda_scihub_scraper
pip install -t requests .
```

## create deployment package
```
cd lambda_scihub_scraper
ln -sf lambda_function-acq_ingest_cron.py lambda_function.py
zip -r -9 ../lambda_scihub_scraper.zip *
```

NOTE: This implementation is not complete
