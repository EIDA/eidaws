# -*- coding: utf-8 -*-

from aiohttp import web

from eidaws.endpoint_proxy.middleware import (
    before_request,
    exception_handling_middleware,
)
from eidaws.endpoint_proxy.remote import XForwardedRelaxed
from eidaws.endpoint_proxy.route import setup_routes
from eidaws.endpoint_proxy.settings import PROXY_BASE_ID
from eidaws.endpoint_proxy.utils import setup_http_conn_pool


def create_app(config_dict):
    def make_server_config(arg_dict):
        return {PROXY_BASE_ID: {"config": arg_dict}}

    if config_dict["unix_path"] is not None:
        # ignore hostname:port
        config_dict["hostname"] = config_dict["port"] = None

    app = web.Application(
        middlewares=[
            before_request,
            exception_handling_middleware,
            XForwardedRelaxed(num=config_dict["num_forwarded"]).middleware,
        ]
    )

    # populate application with config
    server_config = make_server_config(config_dict)
    for k, v in server_config.items():
        app[k] = v

    setup_routes(app)
    setup_http_conn_pool(app)

    return app
