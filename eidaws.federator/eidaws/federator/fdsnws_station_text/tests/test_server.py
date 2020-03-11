# -*- coding: utf-8 -*-

import asyncio
import copy
import pytest

from eidaws.federator.fdsnws_station_text import create_app, SERVICE_ID
from eidaws.federator.fdsnws_station_text.app import DEFAULT_CONFIG
from eidaws.federator.utils.misc import get_config
from eidaws.federator.fdsnws_station_text.route import (
    FED_STATION_TEXT_PATH_QUERY,
)


# TODO(damb): Check if both Redis and eida-federator.ethz.ch's Stationlite are
# up and running. Else skip tests.


_PATH_QUERY = FED_STATION_TEXT_PATH_QUERY


@pytest.fixture
def make_aiohttp_client(aiohttp_client):

    config_dict = get_config(SERVICE_ID, defaults=DEFAULT_CONFIG)

    async def _make_aiohttp_client(config_dict=config_dict):

        app = create_app(config_dict)
        return await aiohttp_client(app)

    return _make_aiohttp_client


@pytest.fixture(scope="session")
def fdsnws_station_text_content_type():
    return "text/plain; charset=utf-8"


class TestFDSNStationTextServer:
    async def test_post_single_sncl(
        self, make_aiohttp_client, fdsnws_station_text_content_type
    ):
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
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_station_text_content_type
        )

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

    async def test_max_stream_epoch_duration(self, make_aiohttp_client):
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict[
            "url_routing"
        ] = "http://eida-federator.ethz.ch/eidaws/routing/1/query"
        config_dict["max_stream_epoch_duration"] = 1

        client = await make_aiohttp_client(
            config_dict=get_config(SERVICE_ID, defaults=config_dict)
        )

        params = {
            "net": "CH",
            "sta": "HASLI",
            "cha": "BHZ",
            "start": "2020-01-01",
            "end": "2020-01-02",
        }
        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 200

        params = {
            "net": "CH",
            "sta": "HASLI",
            "cha": "BHZ",
            "start": "2020-01-01",
            "end": "2020-01-02T00:00:01",
        }
        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 413

    async def test_max_total_stream_epoch_duration(self, make_aiohttp_client):
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict[
            "url_routing"
        ] = "http://eida-federator.ethz.ch/eidaws/routing/1/query"
        config_dict["max_total_stream_epoch_duration"] = 3

        client = await make_aiohttp_client(
            config_dict=get_config(SERVICE_ID, defaults=config_dict)
        )

        params = {
            "net": "CH",
            "sta": "HASLI",
            "cha": "BH?",
            "start": "2020-01-01",
            "end": "2020-01-02",
            "level": "channel",
        }
        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 200

        params = {
            "net": "CH",
            "sta": "HASLI",
            "cha": "BH?",
            "start": "2020-01-01",
            "end": "2020-01-02T00:00:01",
            "level": "channel",
        }
        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 413

    async def test_max_stream_epoch_durations(self, make_aiohttp_client):
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict[
            "url_routing"
        ] = "http://eida-federator.ethz.ch/eidaws/routing/1/query"
        config_dict["max_stream_epoch_durations"] = 2
        config_dict["max_total_stream_epoch_duration"] = 3

        client = await make_aiohttp_client(
            config_dict=get_config(SERVICE_ID, defaults=config_dict)
        )

        params = {
            "net": "CH",
            "sta": "HASLI",
            "cha": "BHZ",
            "start": "2020-01-01",
            "end": "2020-01-03",
            "level": "channel",
        }
        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 200

        params = {
            "net": "CH",
            "sta": "HASLI",
            "cha": "BHZ,BHN",
            "start": "2020-01-01",
            "end": "2020-01-03",
            "level": "channel",
        }
        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 413


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
