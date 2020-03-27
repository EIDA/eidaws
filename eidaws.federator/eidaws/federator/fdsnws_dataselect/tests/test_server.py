# -*- coding: utf-8 -*-

import copy
import functools
import pytest

from aiohttp import web

from eidaws.federator.fdsnws_dataselect import create_app, SERVICE_ID
from eidaws.federator.fdsnws_dataselect.app import DEFAULT_CONFIG
from eidaws.federator.fdsnws_dataselect.route import FED_DATASELECT_PATH_QUERY
from eidaws.federator.utils.misc import get_config
from eidaws.federator.utils.pytest_plugin import (
    fdsnws_dataselect_content_type,
    fdsnws_error_content_type,
    eidaws_routing_path_query,
    load_data,
    make_federated_eida,
)
from eidaws.federator.utils.tests.server_mixin import (
    _TestKeywordParserMixin,
    _TestRoutingMixin,
)
from eidaws.utils.settings import FDSNWS_DATASELECT_PATH_QUERY


class TestFDSNDataselectServer(_TestKeywordParserMixin, _TestRoutingMixin):
    FED_PATH_QUERY = FED_DATASELECT_PATH_QUERY
    PATH_QUERY = FDSNWS_DATASELECT_PATH_QUERY

    @staticmethod
    def get_default_config():
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict["pool_size"] = 1

        return get_config(SERVICE_ID, defaults=config_dict)

    @classmethod
    def create_app(cls, config_dict=None):

        if config_dict is None:
            config_dict = cls.get_default_config()

        return functools.partial(create_app, config_dict)

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2019-01-05",
                },
            ),
            ("POST", b"CH HASLI -- LHZ 2019-01-01 2019-01-05",),
        ],
    )
    async def test_single_stream_epoch(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        fdsnws_dataselect_content_type,
        load_data,
        method,
        params_or_data,
    ):

        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://eida.ethz.ch/fdsnws/dataselect/1/query\n"
                            "CH HASLI -- LHZ 2019-01-01T00:00:00 2019-01-05T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        mocked_endpoints = {
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
            ]
        }

        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(self.FED_PATH_QUERY, **kwargs)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data(
            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
        )

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "DAVOX,HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2019-01-05",
                },
            ),
            (
                "POST",
                (
                    b"CH HASLI -- LHZ 2019-01-01 2019-01-05\n"
                    b"CH DAVOX -- LHZ 2019-01-01 2019-01-05"
                ),
            ),
        ],
    )
    async def test_multi_stream_epoch(
        self,
        make_federated_eida,
        fdsnws_dataselect_content_type,
        load_data,
        eidaws_routing_path_query,
        method,
        params_or_data,
    ):

        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://eida.ethz.ch/fdsnws/dataselect/1/query\n"
                            "CH DAVOX -- LHZ 2019-01-01T00:00:00 2019-01-05T00:00:00\n"
                            "CH HASLI -- LHZ 2019-01-01T00:00:00 2019-01-05T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        mocked_endpoints = {
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.DAVOX..LHZ.2019-01-01.2019-01-05T00:06:09"
                        ),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
            ]
        }

        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(self.FED_PATH_QUERY, **kwargs)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data("CH.DAVOX,HASLI..LHZ.2019-01-01.2019-01-05")

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2019-01-10",
                },
            ),
            ("POST", b"CH HASLI -- LHZ 2019-01-01 2019-01-10",),
        ],
    )
    async def test_split_with_overlap(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        fdsnws_dataselect_content_type,
        load_data,
        method,
        params_or_data,
    ):
        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://eida.ethz.ch/fdsnws/dataselect/1/query\n"
                            "CH HASLI -- LHZ 2019-01-01T00:00:00 2019-01-10T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        mocked_endpoints = {
            "eida.ethz.ch": [
                (self.PATH_QUERY, "GET", web.Response(status=413),),
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-05.2019-01-10"),
                    ),
                ),
            ]
        }

        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(self.FED_PATH_QUERY, **kwargs)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data("CH.HASLI..LHZ.2019-01-01.2019-01-10")

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2019-01-01T00:10:00",
                },
            ),
            ("POST", b"CH HASLI -- LHZ 2019-01-01 2019-01-01T00:10:00",),
        ],
    )
    async def test_split_without_overlap(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        fdsnws_dataselect_content_type,
        load_data,
        method,
        params_or_data,
    ):
        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://eida.ethz.ch/fdsnws/dataselect/1/query\n"
                            "CH HASLI -- LHZ 2019-01-01T00:00:00 2019-01-01T00:10:00\n"
                        ),
                    ),
                )
            ]
        }

        mocked_endpoints = {
            "eida.ethz.ch": [
                (self.PATH_QUERY, "GET", web.Response(status=413),),
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-00T00:05:04"
                        ),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01T05:05:00.2019-01-00T00:10:00"
                        ),
                    ),
                ),
            ]
        }

        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(self.FED_PATH_QUERY, **kwargs)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data(
            "CH.HASLI..LHZ.2019-01-01.2019-01-01T00:10:00"
        )

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2019-01-20",
                },
            ),
            ("POST", b"CH HASLI -- LHZ 2019-01-01 2019-01-20",),
        ],
    )
    async def test_split_with_overlap(
        self,
        make_federated_eida,
        fdsnws_dataselect_content_type,
        load_data,
        eidaws_routing_path_query,
        method,
        params_or_data,
    ):

        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://eida.ethz.ch/fdsnws/dataselect/1/query\n"
                            "CH HASLI -- LHZ 2019-01-01T00:00:00 2019-01-20T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        mocked_endpoints = {
            "eida.ethz.ch": [
                (self.PATH_QUERY, "GET", web.Response(status=413),),
                (self.PATH_QUERY, "GET", web.Response(status=413),),
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-05.2019-01-10"),
                    ),
                ),
                (self.PATH_QUERY, "GET", web.Response(status=413),),
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-10.2019-01-15"),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    "GET",
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-15.2019-01-20"),
                    ),
                ),
            ]
        }

        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(self.FED_PATH_QUERY, **kwargs)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data("CH.HASLI..LHZ.2019-01-01.2019-01-20")

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()
