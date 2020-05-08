# -*- coding: utf-8 -*-

import copy
import datetime
import functools
import json
import pytest

from aiohttp import web

from eidaws.federator.eidaws_wfcatalog_json import create_app, SERVICE_ID
from eidaws.federator.eidaws_wfcatalog_json.app import DEFAULT_CONFIG
from eidaws.federator.eidaws_wfcatalog_json.route import (
    FED_WFCATALOG_PATH_QUERY,
)
from eidaws.federator.utils.misc import get_config
from eidaws.federator.utils.pytest_plugin import (
    eidaws_wfcatalog_content_type,
    fdsnws_error_content_type,
    eidaws_routing_path_query,
    load_data,
    make_federated_eida,
    server_config,
    cache_config,
    tester,
)
from eidaws.federator.utils.tests.server_mixin import (
    _TestCommonServerConfig,
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
)
from eidaws.utils.settings import EIDAWS_WFCATALOG_PATH_QUERY


_now = datetime.datetime.utcnow()
_tomorrow = _now + datetime.timedelta(days=1)
_day_after_tomorrow = _now + datetime.timedelta(days=2)


@pytest.fixture
def content_tester(load_data):
    async def _content_tester(resp, expected=None):
        assert expected is not None
        assert await resp.json() == json.loads(load_data(expected))

    return _content_tester


class TestEIDAWFCatalogServer(
    _TestCommonServerConfig,
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
):
    FED_PATH_QUERY = FED_WFCATALOG_PATH_QUERY
    PATH_QUERY = EIDAWS_WFCATALOG_PATH_QUERY

    _DEFAULT_SERVER_CONFIG = {"pool_size": 1}

    @staticmethod
    def get_config(**kwargs):
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict.update(kwargs)

        return get_config(SERVICE_ID, defaults=config_dict)

    @classmethod
    def create_app(cls, config_dict=None):

        if config_dict is None:
            config_dict = cls.get_config(**cls._DEFAULT_SERVER_CONFIG)

        return functools.partial(create_app, config_dict)

    @staticmethod
    def lookup_config(key, config_dict):
        return config_dict["config"][SERVICE_ID][key]

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-03",
                },
            ),
            ("POST", b"CH HASLI -- BHZ 2020-01-01 2020-01-03",),
        ],
    )
    async def test_single_stream_epoch(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        eidaws_wfcatalog_content_type,
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
                            "http://eida.ethz.ch/eidaws/wfcatalog/1/query\n"
                            "CH HASLI -- BHZ 2020-01-01T00:00:00 2020-01-03T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-01.2020-01-03"),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": eidaws_wfcatalog_content_type,
            "result": "CH.HASLI..BHZ.2020-01-01.2020-01-03",
        }
        await tester(
            self.FED_PATH_QUERY,
            method,
            params_or_data,
            self.create_app(config_dict=config_dict),
            mocked_routing,
            mocked_endpoints,
            expected,
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHN,BHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-03",
                },
            ),
            (
                "POST",
                (
                    b"CH HASLI -- BHN 2020-01-01 2020-01-03\n"
                    b"CH HASLI -- BHZ 2019-01-01 2020-01-03"
                ),
            ),
        ],
    )
    async def test_multi_stream_epoch(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        eidaws_wfcatalog_content_type,
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
                            "http://eida.ethz.ch/eidaws/wfcatalog/1/query\n"
                            "CH HASLI -- BHN 2020-01-01T00:00:00 2020-01-03T00:00:00\n"
                            "CH HASLI -- BHZ 2020-01-01T00:00:00 2020-01-03T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        endpoint_request_method = self.lookup_config(
            "endpoint_request_method", config_dict
        )
        mocked_endpoints = {
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHN.2020-01-01.2020-01-03"),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-01.2020-01-03"),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": eidaws_wfcatalog_content_type,
            "result": "CH.HASLI..BHN,BHZ.2020-01-01.2020-01-03",
        }
        await tester(
            self.FED_PATH_QUERY,
            method,
            params_or_data,
            self.create_app(config_dict=config_dict),
            mocked_routing,
            mocked_endpoints,
            expected,
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH,GR",
                    "sta": "BFO,HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-03",
                },
            ),
            (
                "POST",
                (
                    b"GR BFO -- BHZ 2020-01-01 2020-01-03\n"
                    b"CH HASLI -- BHZ 2020-01-01 2020-01-03"
                ),
            ),
        ],
    )
    async def test_multi_endpoints(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        eidaws_wfcatalog_content_type,
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
                            "http://eida.bgr.de/eidaws/wfcatalog/1/query\n"
                            "GR BFO -- BHZ 2020-01-01T00:00:00 2020-01-03T00:00:00\n"
                            "\n"
                            "http://eida.ethz.ch/eidaws/wfcatalog/1/query\n"
                            "CH HASLI -- BHZ 2020-01-01T00:00:00 2020-01-03T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        endpoint_request_method = self.lookup_config(
            "endpoint_request_method", config_dict
        )
        mocked_endpoints = {
            "eida.bgr.de": [
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("GR.BFO..BHZ.2020-01-01.2020-01-03"),
                    ),
                ),
            ],
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-01.2020-01-03"),
                    ),
                ),
            ],
        }

        expected = {
            "status": 200,
            "content_type": eidaws_wfcatalog_content_type,
            "result": "CH,GR.BFO,HASLI..BHZ.2020-01-01.2020-01-03",
        }
        await tester(
            self.FED_PATH_QUERY,
            method,
            params_or_data,
            self.create_app(config_dict=config_dict),
            mocked_routing,
            mocked_endpoints,
            expected,
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-10",
                },
            ),
            ("POST", b"CH HASLI -- BHZ 2020-01-01 2020-01-10",),
        ],
    )
    async def test_split_without_overlap(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        eidaws_wfcatalog_content_type,
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
                            "http://eida.ethz.ch/eidaws/wfcatalog/1/query\n"
                            "CH HASLI -- BHZ 2020-01-01T00:00:00 2020-01-10T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        endpoint_request_method = self.lookup_config(
            "endpoint_request_method", config_dict
        )
        mocked_endpoints = {
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(status=413),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-01.2020-01-05"),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-05.2020-01-10"),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": eidaws_wfcatalog_content_type,
            "result": "CH.HASLI..BHZ.2020-01-01.2020-01-10",
        }
        await tester(
            self.FED_PATH_QUERY,
            method,
            params_or_data,
            self.create_app(config_dict=config_dict),
            mocked_routing,
            mocked_endpoints,
            expected,
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-10",
                },
            ),
            ("POST", b"CH HASLI -- BHZ 2020-01-01 2020-01-10",),
        ],
    )
    async def test_split_with_overlap(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        eidaws_wfcatalog_content_type,
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
                            "http://eida.ethz.ch/eidaws/wfcatalog/1/query\n"
                            "CH HASLI -- BHZ 2020-01-01T00:00:00 2020-01-10T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        endpoint_request_method = self.lookup_config(
            "endpoint_request_method", config_dict
        )
        mocked_endpoints = {
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(status=413),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-01.2020-01-06"),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-05.2020-01-10"),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": eidaws_wfcatalog_content_type,
            "result": "CH.HASLI..BHZ.2020-01-01.2020-01-10",
        }
        await tester(
            self.FED_PATH_QUERY,
            method,
            params_or_data,
            self.create_app(config_dict=config_dict),
            mocked_routing,
            mocked_endpoints,
            expected,
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-09",
                },
            ),
            ("POST", b"CH HASLI -- BHZ 2020-01-01 2020-01-09",),
        ],
    )
    async def test_split_with_overlap(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        eidaws_wfcatalog_content_type,
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
                            "http://eida.ethz.ch/eidaws/wfcatalog/1/query\n"
                            "CH HASLI -- BHZ 2020-01-01T00:00:00 2020-01-09T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        endpoint_request_method = self.lookup_config(
            "endpoint_request_method", config_dict
        )
        mocked_endpoints = {
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(status=413),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(status=413),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-01.2020-01-03"),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-03.2020-01-05"),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(status=413),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-05.2020-01-08"),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-07.2020-01-09"),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": eidaws_wfcatalog_content_type,
            "result": "CH.HASLI..BHZ.2020-01-01.2020-01-09",
        }
        await tester(
            self.FED_PATH_QUERY,
            method,
            params_or_data,
            self.create_app(config_dict=config_dict),
            mocked_routing,
            mocked_endpoints,
            expected,
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-03",
                },
            ),
            ("POST", b"CH HASLI -- BHZ 2020-01-01 2020-01-03",),
        ],
    )
    async def test_cached(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        eidaws_wfcatalog_content_type,
        load_data,
        cache_config,
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
                            "http://eida.ethz.ch/eidaws/wfcatalog/1/query\n"
                            "CH HASLI -- BHZ 2020-01-01T00:00:00 2020-01-03T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config, **cache_config)
        mocked_endpoints = {
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..BHZ.2020-01-01.2020-01-03"),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": eidaws_wfcatalog_content_type,
            "result": "CH.HASLI..BHZ.2020-01-01.2020-01-03",
        }
        await tester(
            self.FED_PATH_QUERY,
            method,
            params_or_data,
            self.create_app(config_dict=config_dict),
            mocked_routing,
            mocked_endpoints,
            expected,
            test_cached=True,
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "start": "2020-01-01",
                },
            ),
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "end": "2020-01-09",
                },
            ),
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "start": _tomorrow.isoformat(),
                    "end": _day_after_tomorrow.isoformat(),
                },
            ),
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "BHZ",
                    "start": "2020-01-02",
                    "end": "2020-01-01",
                },
            ),
            ("POST", b"CH HASLI -- BHZ 2020-01-01",),
            (
                "POST",
                (
                    f"CH HASLI -- BHZ {_tomorrow.isoformat()} "
                    f"{_day_after_tomorrow.isoformat()}"
                ).encode("utf-8"),
            ),
            ("POST", b"CH HASLI -- BHZ 2020-01-02 2020-01-01",),
            ("POST", b"CH HASLI -- BHZ",),
        ],
    )
    async def test_parser_invalid(
        self,
        make_federated_eida,
        fdsnws_error_content_type,
        method,
        params_or_data,
    ):

        client, _, _ = await make_federated_eida(self.create_app(),)

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(self.FED_PATH_QUERY, **kwargs)

        assert resp.status == 400
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_error_content_type
        )
