FROM hysds/pge-base

MAINTAINER aria-dev "aria-dev@jpl.nasa.gov"
LABEL description="SciHub/ApiHub scraper PGE"

USER ops

#install shapely
RUN sudo pip install shapely

# copy packages
COPY . /home/ops/verdi/ops/scihub_acquisition_scraper
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/scihub_acquisition_scraper \
 && pip install tabulate

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
