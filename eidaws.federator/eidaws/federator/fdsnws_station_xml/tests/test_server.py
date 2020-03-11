# -*- coding: utf-8 -*-

import asyncio
import copy
import io
import pathlib
import pytest

from lxml import etree

from eidaws.federator.fdsnws_station_xml import create_app, SERVICE_ID
from eidaws.federator.fdsnws_station_xml.app import DEFAULT_CONFIG
from eidaws.federator.utils.misc import get_config
from eidaws.federator.fdsnws_station_xml.route import (
    FED_STATION_XML_PATH_QUERY,
)


# TODO(damb): Check if both Redis and eida-federator.ethz.ch's Stationlite are
# up and running. Else skip tests.


_PATH_QUERY = FED_STATION_XML_PATH_QUERY


@pytest.fixture
def make_aiohttp_client(aiohttp_client):

    config_dict = get_config(SERVICE_ID, defaults=DEFAULT_CONFIG)

    async def _make_aiohttp_client(config_dict=config_dict):

        app = create_app(config_dict)
        return await aiohttp_client(app)

    return _make_aiohttp_client


@pytest.fixture(scope="session")
def xml_schema():
    path_xsd = pathlib.Path(__file__).parent / "data" / "fdsn-station-1.0.xsd"
    with open(path_xsd) as ifd:
        xmlschema_doc = etree.parse(ifd)

    return etree.XMLSchema(xmlschema_doc)


@pytest.fixture(scope="session")
def fdsnws_station_xml_content_type():
    return "application/xml"


class TestFDSNStationXMLServer:
    @staticmethod
    def get_default_config():
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict[
            "url_routing"
        ] = "http://eida-federator.ethz.ch/eidaws/routing/1/query"
        config_dict["pool_size"] = 1

        return get_config(SERVICE_ID, defaults=config_dict)

    async def test_get_single_sncl(
        self, make_aiohttp_client, xml_schema, fdsnws_station_xml_content_type,
    ):
        client = await make_aiohttp_client(
            config_dict=self.get_default_config()
        )

        _params = {
            "net": "NL",
            "sta": "HGN",
            "loc": "??",
            "cha": "*",
            "start": "2013-11-10",
            "end": "2013-11-11",
        }

        params = copy.deepcopy(_params)
        params["level"] = "network"

        resp = await client.get(_PATH_QUERY, params=params)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        params = copy.deepcopy(_params)
        params["level"] = "station"

        resp = await client.get(_PATH_QUERY, params=params)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        params = copy.deepcopy(_params)
        params["level"] = "channel"

        resp = await client.get(_PATH_QUERY, params=params)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        params = copy.deepcopy(_params)
        params["level"] = "response"

        resp = await client.get(_PATH_QUERY, params=params)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        # # XXX(damb): This is a workaround in order to avoid the message
        # # "Exception ignored in: <coroutine object ...>"
        await asyncio.sleep(0.01)

    async def test_post_single_sncl(
        self, make_aiohttp_client, xml_schema, fdsnws_station_xml_content_type
    ):
        client = await make_aiohttp_client(
            config_dict=self.get_default_config()
        )

        sncl = b"NL HGN ?? * 2013-10-10 2013-10-11"

        data = b"level=network\n" + sncl
        resp = await client.post(_PATH_QUERY, data=data)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        data = b"level=station\n" + sncl
        resp = await client.post(_PATH_QUERY, data=data)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        data = b"level=channel\n" + sncl
        resp = await client.post(_PATH_QUERY, data=data)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        data = b"level=response\n" + sncl
        resp = await client.post(_PATH_QUERY, data=data)

        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_station_xml_content_type
        )

        xml = etree.parse(io.BytesIO(await resp.read()))
        assert xml_schema.validate(xml)

        # # XXX(damb): This is a workaround in order to avoid the message
        # # "Exception ignored in: <coroutine object ...>"
        await asyncio.sleep(0.01)
