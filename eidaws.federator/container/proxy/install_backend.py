#!/usr/bin/env python3.7

import json
import os
import re
import shutil
import stat
import sys

from pathlib import Path


def main():

    PATH_TMP = Path("/tmp")
    PATH_NGINX_CONFD = Path("/etc/nginx/conf.d")
    PATH_RUNIT_SERVICE = Path("/etc/service")
    PATH_EIDAWS_ENDPOINT_PROXY_CONF = Path("/etc/eidaws-endpoint-proxy")
    PATH_VENV = Path("/var/www/eidaws-endpoint-proxy/venv/")
    BACKEND_MAP_CONF = "backend.map.conf"
    EIDAWS_CONFIG_YML_TEMPLATE = "eidaws_config.yml.template"

    backend_configs = json.load(sys.stdin)

    if not backend_configs:
        return

    # create nginx eidaws-endpoint-proxy related configuration
    with open(PATH_TMP / BACKEND_MAP_CONF, "w") as ofd:
        ofd.write("map $http_host $endpoint_proxy {\n  hostnames;\n")
        ofd.write("  default $http_host;\n")

        for config in backend_configs:
            ofd.write(f'  {config["fqdn"]} {config["proxy_netloc"]};\n')

        ofd.write("}")

    shutil.copyfile(
        PATH_TMP / BACKEND_MAP_CONF, PATH_NGINX_CONFD / BACKEND_MAP_CONF
    )

    # create runit eidaws-endpoint-proxy related configuration
    with open(PATH_TMP / EIDAWS_CONFIG_YML_TEMPLATE, "r") as ifd:
        eidaws_yml_template = ifd.read()

    for i, config in enumerate(backend_configs):

        service_dir = PATH_RUNIT_SERVICE / config["fqdn"]
        os.mkdir(service_dir)

        hostname, port = config["proxy_netloc"].split(":")

        with open(service_dir / "run", "w") as ofd:
            ofd.write("#!/bin/bash\n")
            ofd.write(
                f"exec /sbin/setuser www-data "
                f"{PATH_VENV}/bin/eida-endpoint-proxy "
                f"-H {hostname} -P {port} "
                f"-c {PATH_EIDAWS_ENDPOINT_PROXY_CONF}/eidaws_config_{i}.yml "
                f"2>&1"
            )

        os.chmod(service_dir / "run", 0o755)

        with open(
            PATH_EIDAWS_ENDPOINT_PROXY_CONF / f"eidaws_config_{i}.yml", "w"
        ) as ofd:
            ofd.write(
                re.sub(
                    r"\{\{CONNECTION_LIMIT\}\}",
                    str(config["connection_limit"]),
                    eidaws_yml_template,
                    flags=re.M,
                )
            )


if __name__ == "__main__":
    main()
