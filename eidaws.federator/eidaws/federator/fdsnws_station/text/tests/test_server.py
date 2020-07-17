# -*- coding: utf-8 -*-

import functools
import pytest

from aiohttp import web

from eidaws.federator.fdsnws_station.text import create_app, SERVICE_ID
from eidaws.federator.fdsnws_station.text.app import build_parser
from eidaws.federator.fdsnws_station.text.route import (
    FED_STATION_TEXT_PATH_QUERY,
)
from eidaws.federator.utils.pytest_plugin import (
    eidaws_routing_path_query,
    fdsnws_error_content_type,
    fdsnws_station_text_content_type,
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
    _TestServerBase,
)
from eidaws.utils.cli import NullConfigFileParser
from eidaws.utils.settings import FDSNWS_STATION_PATH_QUERY


@pytest.fixture
def content_tester(load_data):
    async def _content_tester(resp, expected=None):
        assert expected is not None
        assert await resp.text() == load_data(expected, reader="read_text")

    return _content_tester


class TestFDSNStationTextServer(
    _TestCommonServerConfig,
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
    _TestServerBase,
):

    FED_PATH_RESOURCE = FED_STATION_TEXT_PATH_QUERY
    PATH_RESOURCE = FDSNWS_STATION_PATH_QUERY
    SERVICE_ID = SERVICE_ID

    _DEFAULT_SERVER_CONFIG = {"pool_size": 1}

    @staticmethod
    def get_config(**kwargs):
        # get default configuration from parser
        args = build_parser(
            config_file_parser_class=NullConfigFileParser
        ).parse_args(args=[])
        config_dict = vars(args)
        config_dict.update(kwargs)
        return config_dict

    @classmethod
    def create_app(cls, config_dict=None):

        if config_dict is None:
            config_dict = cls.get_config(**cls._DEFAULT_SERVER_CONFIG)

        return functools.partial(create_app, config_dict)

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "NL",
                    "start": "2013-11-10",
                    "end": "2013-11-11",
                    "level": "network",
                    "format": "text",
                },
            ),
            (
                "POST",
                b"level=network\nformat=text\nNL * * * 2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_single_sncl_level_network(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_text_content_type,
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
                            "http://www.orfeus-eu.org/fdsnws/station/1/query\n"
                            "NL * * * "
                            "2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "www.orfeus-eu.org": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        text=load_data(
                            "NL....2013-11-10.2013-11-11.network",
                            reader="read_text",
                        ),
                    ),
                )
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_station_text_content_type,
            "result": "NL....2013-11-10.2013-11-11.network",
        }
        await tester(
            self.FED_PATH_RESOURCE,
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
                    "net": "NL",
                    "sta": "HGN",
                    "start": "2013-11-10",
                    "end": "2013-11-11",
                    "level": "station",
                    "format": "text",
                },
            ),
            (
                "POST",
                b"level=station\nformat=text\nNL HGN * * "
                b"2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_single_sncl_level_station(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_text_content_type,
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
                            "http://www.orfeus-eu.org/fdsnws/station/1/query\n"
                            "NL HGN * * "
                            "2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "www.orfeus-eu.org": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        text=load_data(
                            "NL.HGN...2013-11-10.2013-11-11.station",
                            reader="read_text",
                        ),
                    ),
                )
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_station_text_content_type,
            "result": "NL.HGN...2013-11-10.2013-11-11.station",
        }
        await tester(
            self.FED_PATH_RESOURCE,
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
                    "net": "NL",
                    "sta": "HGN",
                    "cha": "BHZ",
                    "start": "2013-11-10",
                    "end": "2013-11-11",
                    "level": "channel",
                    "format": "text",
                },
            ),
            (
                "POST",
                b"level=channel\nformat=text\nNL HGN * BHZ "
                b"2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_single_sncl_level_station(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_text_content_type,
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
                            "http://www.orfeus-eu.org/fdsnws/station/1/query\n"
                            "NL HGN -- BHZ "
                            "2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "www.orfeus-eu.org": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        text=load_data(
                            "NL.HGN..BHZ.2013-11-10.2013-11-11.channel",
                            reader="read_text",
                        ),
                    ),
                )
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_station_text_content_type,
            "result": "NL.HGN..BHZ.2013-11-10.2013-11-11.channel",
        }
        await tester(
            self.FED_PATH_RESOURCE,
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
                    "net": "NL",
                    "sta": "DBN,HGN",
                    "cha": "BHZ",
                    "start": "2013-11-10",
                    "end": "2013-11-11",
                    "level": "channel",
                    "format": "text",
                },
            ),
            (
                "POST",
                (
                    b"level=channel\nformat=text\n"
                    b"NL HGN * BHZ 2013-11-10 2013-11-11\n"
                    b"NL DBN * BHZ 2013-11-10 2013-11-11\n"
                ),
            ),
        ],
    )
    async def test_single_net_multi_stas(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_text_content_type,
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
                            "http://www.orfeus-eu.org/fdsnws/station/1/query\n"
                            "NL DBN -- BHZ "
                            "2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                            "NL HGN -- BHZ "
                            "2013-11-10T00:00:00 2013-11-11T00:00:00\n"
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
            "www.orfeus-eu.org": [
                (
                    self.PATH_RESOURCE,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        text=load_data(
                            "NL.DBN..BHZ.2013-11-10.2013-11-11.channel",
                            reader="read_text",
                        ),
                    ),
                ),
                (
                    self.PATH_RESOURCE,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        text=load_data(
                            "NL.HGN..BHZ.2013-11-10.2013-11-11.channel",
                            reader="read_text",
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_station_text_content_type,
            "result": "NL.DBN,HGN..BHZ.2013-11-10.2013-11-11.channel",
        }
        await tester(
            self.FED_PATH_RESOURCE,
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
                    "net": "CH,NL",
                    "sta": "HASLI,HGN",
                    "cha": "BHZ",
                    "start": "2013-11-10",
                    "end": "2013-11-11",
                    "level": "channel",
                    "format": "text",
                },
            ),
            (
                "POST",
                (
                    b"level=channel\nformat=text\n"
                    b"CH HASLI * BHZ 2013-11-10 2013-11-11\n"
                    b"NL DBN * BHZ 2013-11-10 2013-11-11\n"
                ),
            ),
        ],
    )
    async def test_multi_nets_multi_dcs(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_text_content_type,
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
                            "http://eida.ethz.ch/fdsnws/station/1/query\n"
                            "CH HASLI -- BHZ "
                            "2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                            "\n"
                            "http://www.orfeus-eu.org/fdsnws/station/1/query\n"
                            "NL DBN -- BHZ "
                            "2013-11-10T00:00:00 2013-11-11T00:00:00\n"
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
                    self.PATH_RESOURCE,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        text=load_data(
                            "CH.HASLI..BHZ.2013-11-10.2013-11-11.channel",
                            reader="read_text",
                        ),
                    ),
                ),
            ],
            "www.orfeus-eu.org": [
                (
                    self.PATH_RESOURCE,
                    endpoint_request_method,
                    web.Response(
                        status=200,
                        text=load_data(
                            "NL.HGN..BHZ.2013-11-10.2013-11-11.channel",
                            reader="read_text",
                        ),
                    ),
                ),
            ],
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_station_text_content_type,
            "result": "CH,NL.HASLI,HGN..BHZ.2013-11-10.2013-11-11.channel",
        }
        await tester(
            self.FED_PATH_RESOURCE,
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
                    "net": "NL",
                    "start": "2013-11-10",
                    "end": "2013-11-11",
                    "level": "network",
                    "format": "text",
                },
            ),
            (
                "POST",
                b"level=network\nformat=text\nNL * * * 2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_cached(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_text_content_type,
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
                            "http://www.orfeus-eu.org/fdsnws/station/1/query\n"
                            "NL * * * "
                            "2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config, **cache_config)
        mocked_endpoints = {
            "www.orfeus-eu.org": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        text=load_data(
                            "NL....2013-11-10.2013-11-11.network",
                            reader="read_text",
                        ),
                    ),
                )
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_station_text_content_type,
            "result": "NL....2013-11-10.2013-11-11.network",
        }
        await tester(
            self.FED_PATH_RESOURCE,
            method,
            params_or_data,
            self.create_app(config_dict=config_dict),
            mocked_routing,
            mocked_endpoints,
            expected,
            test_cached=True,
        )
