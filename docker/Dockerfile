FROM hysds/pge-base:latest

MAINTAINER aria-dev "aria-dev@jpl.nasa.gov"
LABEL description="SciHub/ApiHub scraper PGE"

USER ops

# copy packages
COPY . /home/ops/verdi/ops/scihub_acquisition_scraper
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/scihub_acquisition_scraper \
 && pip install tabulate \
 && pip install geojson \
 && pip install shapely \
 && pip install elasticsearch \
 && pip install pandas

ENV PYTHONPATH "/home/ops/verdi/ops/scihub_acquisition_scraper/acquisition_ingest/:$PYTHONPATH"


WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
