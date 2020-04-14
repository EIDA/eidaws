# -*- coding: utf-8 -*-

import copy
import functools
import pytest

from aiohttp import web

from eidaws.federator.fdsnws_dataselect_miniseed import create_app, SERVICE_ID
from eidaws.federator.fdsnws_dataselect_miniseed.app import DEFAULT_CONFIG
from eidaws.federator.fdsnws_dataselect_miniseed.route import (
    FED_DATASELECT_PATH_QUERY,
)
from eidaws.federator.utils.misc import get_config
from eidaws.federator.utils.pytest_plugin import (
    fdsnws_dataselect_content_type,
    fdsnws_error_content_type,
    eidaws_routing_path_query,
    load_data,
    make_federated_eida,
    server_config,
    tester,
)
from eidaws.federator.utils.tests.server_mixin import (
    _TestCommonServerConfig,
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
)
from eidaws.utils.settings import FDSNWS_DATASELECT_PATH_QUERY


@pytest.fixture
def content_tester(load_data):
    async def _content_tester(resp, expected=None):
        assert expected is not None
        assert await resp.read() == load_data(expected)

    return _content_tester


class TestFDSNDataselectServer(
    _TestCommonServerConfig,
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
):
    FED_PATH_QUERY = FED_DATASELECT_PATH_QUERY
    PATH_QUERY = FDSNWS_DATASELECT_PATH_QUERY

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
        server_config,
        tester,
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

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_dataselect_content_type,
            "result": "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45",
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
        server_config,
        tester,
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
                            "CH DAVOX -- LHZ 2019-01-01T00:00:00 2019-01-05T00:00:00\n"
                            "CH HASLI -- LHZ 2019-01-01T00:00:00 2019-01-05T00:00:00\n"
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
                        body=load_data(
                            "CH.DAVOX..LHZ.2019-01-01.2019-01-05T00:06:09"
                        ),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_dataselect_content_type,
            "result": "CH.DAVOX,HASLI..LHZ.2019-01-01.2019-01-05",
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
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2019-01-05",
                },
            ),
            (
                "POST",
                (
                    b"GR BFO -- LHZ 2019-01-01 2019-01-05\n"
                    b"CH HASLI -- LHZ 2019-01-01 2019-01-05"
                ),
            ),
        ],
    )
    async def test_multi_endpoints(
        self,
        server_config,
        tester,
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
                            "http://eida.bgr.de/fdsnws/dataselect/1/query\n"
                            "GR BFO -- LHZ 2019-01-01T00:00:00 2019-01-05T00:00:00\n"
                            "\n"
                            "http://eida.ethz.ch/fdsnws/dataselect/1/query\n"
                            "CH HASLI -- LHZ 2019-01-01T00:00:00 2019-01-05T00:00:00\n"
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
                        body=load_data("GR.BFO..LHZ.2019-01-01.2019-01-05"),
                    ),
                ),
            ],
            "eida.ethz.ch": [
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
            ],
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_dataselect_content_type,
            "result": "CH,GR.BFO,HASLI..LHZ.2019-01-01.2019-01-05",
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
        server_config,
        tester,
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
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-05.2019-01-10"),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_dataselect_content_type,
            "result": "CH.HASLI..LHZ.2019-01-01.2019-01-10",
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
        server_config,
        tester,
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
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-00T00:05:04"
                        ),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01T05:05:00.2019-01-00T00:10:00"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_dataselect_content_type,
            "result": "CH.HASLI..LHZ.2019-01-01.2019-01-01T00:10:00",
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
        server_config,
        tester,
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
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-05.2019-01-10"),
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
                        body=load_data("CH.HASLI..LHZ.2019-01-10.2019-01-15"),
                    ),
                ),
                (
                    self.PATH_QUERY,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-15.2019-01-20"),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_dataselect_content_type,
            "result": "CH.HASLI..LHZ.2019-01-01.2019-01-20",
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
