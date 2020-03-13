FROM hysds/pge-base:python2

MAINTAINER aria-dev "aria-dev@jpl.nasa.gov"
LABEL description="SciHub/ApiHub scraper PGE"

USER ops

#install shapely
RUN sudo pip install shapely

#install elasticsearch
RUN sudo pip install elasticsearch

#install pandas
RUN sudo pip install numpy==1.16.5
RUN sudo pip install pandas==0.24.2

# copy packages
COPY . /home/ops/verdi/ops/scihub_acquisition_scraper
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/scihub_acquisition_scraper \
 && pip install tabulate \
 && pip install geojson

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
