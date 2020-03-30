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
    fdsnws_error_content_type,
    fdsnws_station_xml_content_type,
    eidaws_routing_path_query,
    load_data,
    make_federated_eida,
)
from eidaws.federator.utils.tests.server_mixin import (
    _TestCommonServerConfig,
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
)
from eidaws.utils.settings import FDSNWS_STATION_PATH_QUERY


# TODO(damb): Implement validate_station_xml(tree) in order to validate the
# number of net, sta, cha objects. The function should return a list of tuples
# with three elements.


@pytest.fixture
def xml_schema(load_data):
    xsd = load_data("fdsn-station-1.0.xsd")
    xmlschema_doc = etree.parse(io.BytesIO(xsd))
    return etree.XMLSchema(xmlschema_doc)


class TestFDSNStationXMLServer(
    _TestCommonServerConfig,
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
):
    FED_PATH_QUERY = FED_STATION_XML_PATH_QUERY
    PATH_QUERY = FDSNWS_STATION_PATH_QUERY

    @staticmethod
    def get_config(**kwargs):
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict.update(kwargs)

        return get_config(SERVICE_ID, defaults=config_dict)

    @classmethod
    def create_app(cls, config_dict=None):

        if config_dict is None:
            config_dict = cls.get_config(**{"pool_size": 1})

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
        make_federated_eida,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
        xml_schema,
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
                            "NL * * * 2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                        ),
                    ),
                )
            ]
        }
        mocked_endpoints = {
            "www.orfeus-eu.org": [
                (
                    self.PATH_QUERY,
                    "GET",
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
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

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
        make_federated_eida,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
        xml_schema,
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
                            "NL HGN * * 2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                        ),
                    ),
                )
            ]
        }
        mocked_endpoints = {
            "www.orfeus-eu.org": [
                (
                    self.PATH_QUERY,
                    "GET",
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
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

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
                b"level=channel\nformat=xml\nNL HGN * BHZ 2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_single_sncl_level_channel(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
        xml_schema,
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
                            "NL HGN * BHZ 2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                        ),
                    ),
                )
            ]
        }
        mocked_endpoints = {
            "www.orfeus-eu.org": [
                (
                    self.PATH_QUERY,
                    "GET",
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
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

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
                b"level=response\nformat=xml\nNL HGN * BHZ 2013-11-10 2013-11-11",
            ),
        ],
    )
    async def test_single_sncl_level_response(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        fdsnws_station_xml_content_type,
        xml_schema,
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
                            "NL HGN * BHZ 2013-11-10T00:00:00 2013-11-11T00:00:00\n"
                        ),
                    ),
                )
            ]
        }
        mocked_endpoints = {
            "www.orfeus-eu.org": [
                (
                    self.PATH_QUERY,
                    "GET",
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
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()
