# -*- coding: utf-8 -*-

from aiohttp import web

from eidaws.endpoint_proxy.middleware import (
    before_request,
    exception_handling_middleware,
)
from eidaws.endpoint_proxy.route import setup_routes
from eidaws.endpoint_proxy.settings import PROXY_BASE_ID
from eidaws.endpoint_proxy.utils import setup_endpoint_http_conn_pool


def create_app(config_dict):

    config = config_dict[PROXY_BASE_ID]["config"]

    if config["unix_path"] is not None:
        # ignore hostname:port
        config["hostname"] = config["port"] = None

    app = web.Application(
        middlewares=[before_request, exception_handling_middleware]
    )

    # populate application with config
    for k, v in config_dict.items():
        app[k] = v

    setup_routes(app)

    setup_endpoint_http_conn_pool(app)

    return app
