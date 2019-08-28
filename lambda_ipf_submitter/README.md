# IPF job submission lambda_funcs
This lambda was added for future replacement of the IPF cron settings on factotum.
The idea was to use schedule lambdas.

## adding external libraries
```
cd lambda_ipf_submitter
pip install -t <lib> .
```

For example to install `requests`
```
cd lambda_ipf_submitter
pip install -t requests .
```

## create deployment package
```
cdlambda_ipf_submitter
ln -sf lambda_function-ipf_scrape_cron.py lambda_function.py
zip -r -9 ../lambda_ipf_submitter.zip *
```

NOTE: This implementation is not complete
