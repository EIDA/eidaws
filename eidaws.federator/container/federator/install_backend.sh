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
    runit_service_dir=/etc/service/${service}-${i}
    mkdir -p "${runit_service_dir}" && \
    echo -e "#!/bin/sh\n"\
"exec /sbin/setuser www-data "\
"${PATH_VENV}/bin/${service} -U /run/eidaws-federator/${service}-${i}.sock "\
"2>&1" >> "${runit_service_dir}/run" && \
    chmod +x "${runit_service_dir}/run"; \
    # create upstream template
    echo "  server unix:/run/eidaws-federator/${service}-${i}.sock "\
"fail_timeout=0;" >> /tmp/${service}.upstream
  done

# create nginx backend service config
path_backend_config=/etc/nginx/conf.d/backends 
mkdir -p "${path_backend_config}"
sed -e "s/{{SERVICE_ID}}/${service}/" \
  -e "/{{UNIX_SERVERS}}/ r /tmp/${service}.upstream" \
  -e "/{{UNIX_SERVERS}}/d" \
  /tmp/backend.conf.template >> ${path_backend_config}/${service}.conf

# clean up
rm -f "/tmp/nginx/conf.d/backends/${service}.upstream"
