# hysds_lambda_funcs
This lambda was added for future replacement of the acquisitions ingest cron settings on factotum.

## adding external libraries
```
cd hysds_lambda_funcs
pip install -t <lib> .
```

For example to install `requests`
```
cd hysds_lambda_funcs
pip install -t requests .
```

## create deployment package
```
cd hysds_lambda_funcs
ln -sf lambda_function-proxy_mozart.py lambda_function.py
zip -r -9 ../hysds_lambda_funcs.zip *
```
