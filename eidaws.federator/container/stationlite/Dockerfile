# Dockerizing eidaws-stationlite
#
# Build Container:
# docker build -t eidaws-stationlite:1.0 .
#
# Run Container:
# docker run [-d] [--rm] [--name eidaws-stationlite] -p 8090:8090 \
#   eidaws-stationlite:1.0
#
# Modify running container:
# docker exec -it eidaws-stationlite /bin/bash

# Base image
FROM docker.io/phusion/baseimage:0.11

ARG DIR_CONTAINERFILE=eidaws.federator/container/stationlite

ENV EIDAWS_STATIONLITE_SETTINGS=/etc/eidaws-stationlite/eidaws_config.yml
ENV FLASK_APP=/usr/local/src/eidaws/eidaws.stationlite/eidaws/stationlite/server/

# Add label metadata
LABEL maintainer="Daniel Armbruster"
LABEL email="daniel.armbruster@sed.ethz.ch"

CMD ["/sbin/my_init"]

# System dependencies
RUN export DEBIAN_FRONTEND=noninteractive && \
  apt-get update && apt-get install -y python3-dev \
                                       python3-venv \
                                       python3-pip \
                                       python-virtualenv \
                                       build-essential \
                                       libpq-dev \
                                       apache2 \
                                       libapache2-mod-wsgi-py3

# Install backend services
RUN mkdir -p /var/www/eidaws-stationlite /etc/eidaws-stationlite \
  /usr/local/src /var/log/eidaws-stationlite
COPY . /usr/local/src/eidaws

# Setup virtualenv and install eidaws.stationlite distribution
RUN virtualenv -p $(which python3.6) /var/www/eidaws-stationlite/venv && \
  /bin/bash -c "source /var/www/eidaws-stationlite/venv/bin/activate && \
  pip install --upgrade pip && \
  pip install numpy && \
  pip install -e /usr/local/src/eidaws/eidaws.utils && \
  pip install -e /usr/local/src/eidaws/eidaws.stationlite[postgres] && deactivate"

# Copy eidaws-stationlite(-harvest) configuration files
COPY $DIR_CONTAINERFILE/*.yml $DIR_CONTAINERFILE/logging*.conf \
  $DIR_CONTAINERFILE/env /etc/eidaws-stationlite/
COPY $DIR_CONTAINERFILE/eidaws-stationlite.syslog-ng.conf \
  /etc/syslog-ng/conf.d/eidaws-stationlite.conf
COPY $DIR_CONTAINERFILE/apache.syslog-ng.conf /etc/syslog-ng/conf.d/apache.conf

# Copy Apache configuration configuration
COPY $DIR_CONTAINERFILE/httpd.conf /etc/apache2/sites-available/

# Copy the WSGI configuration scripts
COPY $DIR_CONTAINERFILE/*.wsgi /var/www/eidaws-stationlite/

# Give read permissions
RUN chmod +r /etc/eidaws-stationlite/* \
  /var/www/eidaws-stationlite/*.wsgi

# Enable the supplied WSGI modules
RUN a2dissite 000-default.conf && a2ensite httpd.conf

# Add the apache2 service
RUN mkdir /etc/service/apache2 && \
	echo "#!/bin/sh\nexec apachectl -D FOREGROUND" > /etc/service/apache2/run && \
	chmod +x /etc/service/apache2/run

# Add the harvesting cronjob (once per day)
RUN sed -e 's/^/export /' -i /etc/eidaws-stationlite/env
RUN echo "0 1 * * * root source /etc/eidaws-stationlite/env; /var/www/eidaws-stationlite/venv/bin/eida-stationlite-harvest --config /etc/eidaws-stationlite/harvest_config.yml" >> /etc/crontab

# Expose HTTP default port
EXPOSE 8089

# Cleanup (baseimage recommended)
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
