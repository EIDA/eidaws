# -*- coding: utf-8 -*-

import copy
import functools
import io
import pytest

from aiohttp import web
from lxml import etree

from eidaws.federator.fdsnws_station_xml import create_app, SERVICE_ID
from eidaws.federator.fdsnws_station_xml.app import DEFAULT_CONFIG
from eidaws.federator.utils.misc import get_config
from eidaws.federator.fdsnws_station_xml.route import (
    FED_STATION_XML_PATH_QUERY,
)
from eidaws.federator.utils.pytest_plugin import (
    eidaws_routing_path_query,
    fdsnws_error_content_type,
    fdsnws_station_xml_content_type,
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
from eidaws.utils.settings import (
    FDSNWS_STATION_PATH_QUERY,
    STATIONXML_TAGS_NETWORK,
    STATIONXML_TAGS_STATION,
    STATIONXML_TAGS_CHANNEL,
)


@pytest.fixture
def xml_schema(load_data):
    xsd = load_data("fdsn-station.xsd")
    xmlschema_doc = etree.parse(io.BytesIO(xsd))
    return etree.XMLSchema(xmlschema_doc)


@pytest.fixture
def content_tester(xml_schema):
    async def _content_tester(resp, expected=None):
        station_xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(station_xml)

        assert expected is not None
        # validate tree
        root = station_xml.getroot()
        t = []
        for net_element in root.iter(*STATIONXML_TAGS_NETWORK):
            stas = []
            for sta_element in net_element.iter(*STATIONXML_TAGS_STATION):
                chas = 0
                for cha_element in sta_element.iter(*STATIONXML_TAGS_CHANNEL):
                    chas += 1

                stas.append((1, chas))

            t.append((1, stas))

        assert t == expected

    return _content_tester


class TestFDSNStationXMLServer(
    _TestCommonServerConfig,
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
    _TestServerBase,
):
    FED_PATH_RESOURCE = FED_STATION_XML_PATH_QUERY
    PATH_RESOURCE = FDSNWS_STATION_PATH_QUERY
    SERVICE_ID = SERVICE_ID

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
                    "format": "xml",
                },
            ),
            (
                "POST",
                b"level=network\nformat=xml\nNL * * * 2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_single_sncl_level_network(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
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
                            "NL * * * 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
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
            "content_type": fdsnws_station_xml_content_type,
            "result": [(1, [])],
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
                    "format": "xml",
                },
            ),
            (
                "POST",
                b"level=station\nformat=xml\nNL HGN * * 2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_single_sncl_level_station(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
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
                            "NL HGN * * 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
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
            "content_type": fdsnws_station_xml_content_type,
            "result": [(1, [(1, 0)])],
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
                    "format": "xml",
                },
            ),
            (
                "POST",
                b"level=channel\nformat=xml\nNL HGN * "
                b"BHZ 2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_single_sncl_level_channel(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
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
                            "NL HGN -- BHZ 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
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
            "content_type": fdsnws_station_xml_content_type,
            "result": [(1, [(1, 1)])],
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
                    "level": "response",
                    "format": "xml",
                },
            ),
            (
                "POST",
                b"level=response\nformat=xml\nNL HGN * "
                b"BHZ 2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_single_sncl_level_response(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
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
                            "NL HGN -- BHZ 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
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
                            "NL.HGN..BHZ.2013-11-10.2013-11-11.response",
                            reader="read_text",
                        ),
                    ),
                )
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_station_xml_content_type,
            "result": [(1, [(1, 1)])],
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
                    "format": "xml",
                },
            ),
            (
                "POST",
                (
                    b"level=channel\nformat=xml\n"
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
        fdsnws_station_xml_content_type,
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
                            "NL DBN -- BHZ 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
                            "NL HGN -- BHZ 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
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
                            "NL.HGN..BHZ.2013-11-10.2013-11-11.channel",
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
                            "NL.DBN..BHZ.2013-11-10.2013-11-11.channel",
                            reader="read_text",
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_station_xml_content_type,
            "result": [(1, [(1, 1), (1, 1)])],
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
                    "cha": "BHN,BHZ",
                    "start": "2013-11-10",
                    "end": "2013-11-11",
                    "level": "channel",
                    "format": "xml",
                },
            ),
            (
                "POST",
                (
                    b"level=channel\nformat=xml\n"
                    b"NL HGN * BHN 2013-11-10 2013-11-11\n"
                    b"NL HGN * BHZ 2013-11-10 2013-11-11\n"
                ),
            ),
        ],
    )
    async def test_single_net_single_sta_multi_chas(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
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
                            "NL HGN -- BHN 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
                            "NL HGN -- BHZ 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
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
                            "NL.HGN..BHN.2013-11-10.2013-11-11.channel",
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
            "content_type": fdsnws_station_xml_content_type,
            "result": [(1, [(1, 2)])],
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
                    "format": "xml",
                },
            ),
            (
                "POST",
                (
                    b"level=channel\nformat=xml\n"
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
        fdsnws_station_xml_content_type,
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
                            "CH HASLI -- BHZ 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
                            "\n"
                            "http://www.orfeus-eu.org/fdsnws/station/1/query\n"
                            "NL DBN -- BHZ 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
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
            "content_type": fdsnws_station_xml_content_type,
            "result": [(1, [(1, 1)]), (1, [(1, 1)])],
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
                    "format": "xml",
                },
            ),
            (
                "POST",
                b"level=network\nformat=xml\nNL * * * 2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_cached(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
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
                            "NL * * * 2013-11-10T00:00:00 "
                            "2013-11-11T00:00:00\n"
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
            "content_type": fdsnws_station_xml_content_type,
            "result": [(1, [])],
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
