# -*- coding: utf-8 -*-

import asyncio
import copy
import pytest

from eidaws.federator.fdsnws_station_text import create_app, SERVICE_ID
from eidaws.federator.fdsnws_station_text.app import DEFAULT_CONFIG
from eidaws.federator.settings import FED_STATION_PATH_TEXT
from eidaws.federator.utils.misc import get_config
from eidaws.utils.settings import FDSNWS_QUERY_METHOD_TOKEN


# TODO(damb): Check if both Redis and eida-federator.ethz.ch's Stationlite are
# up and running. Else skip tests.


_PATH_QUERY = "/".join([FED_STATION_PATH_TEXT, FDSNWS_QUERY_METHOD_TOKEN])


@pytest.fixture
def make_aiohttp_client(aiohttp_client):

    config_dict = get_config(SERVICE_ID, defaults=DEFAULT_CONFIG)

    async def _make_aiohttp_client(config_dict=config_dict):

        app = create_app(config_dict)
        return await aiohttp_client(app)

    return _make_aiohttp_client


class TestFDSNStationTextServer:
    async def test_post_single_sncl(self, make_aiohttp_client):
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict[
            "url_routing"
        ] = "http://eida-federator.ethz.ch/eidaws/routing/1/query"
        config_dict["pool_size"] = 1

        client = await make_aiohttp_client(
            config_dict=get_config(SERVICE_ID, defaults=config_dict)
        )

        data = b"NL HGN ?? * 2013-10-10 2013-10-11"
        resp = await client.post(_PATH_QUERY, data=data)

        assert resp.status == 200

        # XXX(damb): This is a workaround in order to avoid the message
        # "Exception ignored in: <coroutine object ...>"
        await asyncio.sleep(0.01)


class TestFDSNStationTextServerConfig:
    async def test_client_max_size(self, make_aiohttp_client):

        client_max_size = 32
        # avoid large request
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict["client_max_size"] = client_max_size

        client = await make_aiohttp_client(
            config_dict=get_config(SERVICE_ID, defaults=config_dict)
        )

        data = b"level=channel\n" b"\n" b"CH * * * 2020-01-01 2020-01-02"

        assert client_max_size < len(data)

        resp = await client.post(_PATH_QUERY, data=data)

        assert resp.status == 413
        assert "Request Entity Too Large" in await resp.text()


class TestFDSNStationTextServerKeywordParser:
    async def test_get_with_strict_args_invalid(self, make_aiohttp_client):
        client = await make_aiohttp_client()

        resp = await client.get(_PATH_QUERY, params={"foo": "bar"})

        assert resp.status == 400
        assert (
            f"ValidationError: Invalid request query parameters: {{'foo'}}"
            in await resp.text()
        )

    async def test_post_with_strict_args_invalid(self, make_aiohttp_client):
        client = await make_aiohttp_client()

        data = b"foo=bar\n\nNL HGN ?? * 2013-10-10 2013-10-11"
        resp = await client.post(_PATH_QUERY, data=data)

        assert resp.status == 400
        assert (
            f"ValidationError: Invalid request query parameters: {{'foo'}}"
            in await resp.text()
        )

    async def test_post_empty(self, make_aiohttp_client):
        client = await make_aiohttp_client()

        data = b""
        resp = await client.post(_PATH_QUERY, data=data)

        assert resp.status == 400

    async def test_post_equal(self, make_aiohttp_client):
        client = await make_aiohttp_client()

        data = b"="
        resp = await client.post(_PATH_QUERY, data=data)

        assert resp.status == 400
        assert "ValidationError: RTFM :)." in await resp.text()


class TestFDSNStationTextServerCORS:
    async def test_get_cors_simple(self, make_aiohttp_client):
        client = await make_aiohttp_client()

        origin = "http://foo.example.com"

        resp = await client.get(
            _PATH_QUERY, headers={"Origin": origin}, params={"foo": "bar"}
        )

        assert resp.status == 400
        assert (
            "Access-Control-Expose-Headers" in resp.headers
            and resp.headers["Access-Control-Expose-Headers"] == ""
        )
        assert (
            "Access-Control-Allow-Origin" in resp.headers
            and resp.headers["Access-Control-Allow-Origin"] == origin
        )

    async def test_post_cors_simple(self, make_aiohttp_client):
        client = await make_aiohttp_client()

        origin = "http://foo.example.com"

        data = b""
        resp = await client.post(
            _PATH_QUERY, headers={"Origin": origin}, data=data
        )

        assert resp.status == 400
        assert (
            "Access-Control-Expose-Headers" in resp.headers
            and resp.headers["Access-Control-Expose-Headers"] == ""
        )
        assert (
            "Access-Control-Allow-Origin" in resp.headers
            and resp.headers["Access-Control-Allow-Origin"] == origin
        )

    async def test_cors_preflight(self, make_aiohttp_client):
        client = await make_aiohttp_client()

        origin = "http://foo.example.com"
        method = "GET"
        headers = {"Origin": origin, "Access-Control-Request-Method": method}

        resp = await client.options(_PATH_QUERY, headers=headers)

        assert resp.status == 200
        assert (
            "Access-Control-Allow-Methods" in resp.headers
            and resp.headers["Access-Control-Allow-Methods"] == method
        )
        assert (
            "Access-Control-Allow-Origin" in resp.headers
            and resp.headers["Access-Control-Allow-Origin"] == origin
        )

    async def test_cors_preflight_forbidden(self, make_aiohttp_client):
        client = await make_aiohttp_client()

        origin = "http://foo.example.com"

        resp = await client.options(_PATH_QUERY, headers={"Origin": origin})
        assert resp.status == 403

        resp = await client.options(
            _PATH_QUERY, headers={"Access-Control-Request-Method": "GET"}
        )
        assert resp.status == 403
