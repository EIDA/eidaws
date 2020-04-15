#! /bin/bash
# -----------------------------------------------------------------------------
# Purpose: Simplify the installation of eidaws-federator backend services
# -----------------------------------------------------------------------------

service=$1
instances=$2

PATH_VENV=/var/www/eidaws-federator/venv

for i in `seq -w ${instances}`; \
  do 
    # create runit config
    mkdir -p /etc/service/${service}-${i} && \
    echo -e "#!/bin/sh\n"\
"exec ${PATH_VENV}/bin/${service} -U /run/eidaws-federator/${service}-${i}.sock "\
"-c /etc/eidaws_config.yml 2>&1" >> \
    /etc/service/${service}-${i}/run && \
    chmod +x /etc/service/${service}-${i}/run; \
    # create upstream template
    echo "  server unix:/run/eidaws-federator/${service}-${i}.sock "\
"fail_timeout=0;" >> /tmp/${service}.upstream
  done

# create nginx backend service config
sed -e "s/{{SERVICE_ID}}/${service}/" \
  -e "s/{{UNIX_SERVERS}}/ r /tmp/${service}.upstream" \
  -e "/{{UNIX_SERVERS}}/d" \
  /tmp/backend.conf.template >> /etc/nginx/conf.d/backends/${service}.conf

# clean up
rm -f "/tmp/nginx/conf.d/backends/${service}.upstream"
