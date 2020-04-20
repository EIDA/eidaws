# Dockerizing eidaws-federator
#
# Build Container:
# docker build -t eidaws-federator:1.0 .
#
# Run Container:
# docker run [-d] [--rm] [--name eidaws-federator] -p 8080:80 eidaws-federator:1.0
#
# Modify running container:
# docker exec -it eidaws-federator /bin/bash

# Base image
FROM docker.io/phusion/baseimage:0.11

ARG INSTANCES_DATASELECT_MINISEED=8
ARG INSTANCES_STATION_TEXT=4
ARG INSTANCES_STATION_XML=8
ARG INSTANCES_WFCATALOG_JSON=6
ARG CONTEXT=eidaws.federator/container/federator

# Add label metadata
LABEL maintainer="Daniel Armbruster"
LABEL email="daniel.armbruster@sed.ethz.ch"

CMD ["/sbin/my_init"]

# System dependencies
RUN export DEBIAN_FRONTEND=noninteractive && \
  apt-get update && apt-get install -y libxml2-dev \
                                       libxslt-dev \
                                       python3.7 \
                                       python3.7-dev \
                                       python3.7-venv \
                                       python3-pip \
                                       nginx

# Configure nginx
COPY $CONTEXT/conf.d /etc/nginx/conf.d
COPY $CONTEXT/nginx.conf /etc/nginx

# Add the nginx service
RUN mkdir /etc/service/nginx && \
	echo "#!/bin/sh\nexec nginx 2>&1" > /etc/service/nginx/run && \
	chmod +x /etc/service/nginx/run

# Install backend services
RUN mkdir -p /var/www/eidaws-federator /run/eidaws-federator \
  /etc/eidaws-federator /usr/local/src /var/log/eidaws-federator && \
  chown www-data:www-data /run/eidaws-federator /var/log/eidaws-federator
COPY . /usr/local/src/eidaws

# Setup virtualenv and install services
RUN python3.7 -m venv /var/www/eidaws-federator/venv && \
  /bin/bash -c "source /var/www/eidaws-federator/venv/bin/activate && \
  pip install --upgrade pip && \
  pip install -e /usr/local/src/eidaws/eidaws.utils && \
  pip install -e /usr/local/src/eidaws/eidaws.federator && deactivate"

# Backend service configuration files
COPY $CONTEXT/eidaws_config.yml /etc/eidaws-federator

# Add backend services with installation script
COPY $CONTEXT/*.template $CONTEXT/install_backend.sh /tmp
RUN chmod +x /tmp/install_backend.sh && \
  ./tmp/install_backend.sh \
  eida-federator-dataselect-miniseed $INSTANCES_DATASELECT_MINISEED && \
  ./tmp/install_backend.sh \
  eida-federator-station-text $INSTANCES_STATION_TEXT && \
  ./tmp/install_backend.sh \
  eida-federator-station-xml $INSTANCES_STATION_XML && \
  ./tmp/install_backend.sh \
  eida-federator-wfcatalog-json $INSTANCES_WFCATALOG_JSON

# Static content
COPY $CONTEXT/static /var/www/eidaws-federator/static

# Expose HTTP default port
EXPOSE 8080

# Cleanup (baseimage recommended)
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
