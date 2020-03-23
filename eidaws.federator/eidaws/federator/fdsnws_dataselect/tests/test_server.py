# -*- coding: utf-8 -*-

import aiohttp
import collections
import copy
import pathlib
import pytest
import socket

from aiohttp import web
from aiohttp.resolver import DefaultResolver
from aiohttp.test_utils import unused_port

from eidaws.federator.fdsnws_dataselect import create_app, SERVICE_ID
from eidaws.federator.fdsnws_dataselect.app import DEFAULT_CONFIG
from eidaws.federator.fdsnws_dataselect.route import FED_DATASELECT_PATH_QUERY
from eidaws.federator.utils.misc import get_config


_PATH_QUERY = FED_DATASELECT_PATH_QUERY


class FakeResolver:
    _LOCAL_HOST = {
        0: "127.0.0.1",
        socket.AF_INET: "127.0.0.1",
        socket.AF_INET6: "::1",
    }

    def __init__(self, fakes):
        """fakes -- dns -> port dict"""
        self._fakes = fakes
        self._resolver = DefaultResolver()

    async def resolve(self, host, port=0, family=socket.AF_INET):
        fake_port = self._fakes.get(host)
        if fake_port is not None:
            return [
                {
                    "hostname": host,
                    "host": self._LOCAL_HOST[family],
                    "port": fake_port,
                    "family": family,
                    "proto": 0,
                    "flags": socket.AI_NUMERICHOST,
                }
            ]
        else:
            return await self._resolver.resolve(host, port, family)


class FakeServer:
    Route = collections.namedtuple("Route", ["path", "method"])

    def __init__(self, host="localhost"):
        self.app = web.Application()
        self.runner = None

        self._host = host.lower()
        self._responses = collections.defaultdict(list)

    async def start(self):
        port = unused_port()
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", port)
        await site.start()
        return {self._host: port}

    async def stop(self):
        await self.runner.cleanup()

    async def _handler(self, request):
        route, resp = self._find_response(request)
        return resp

    def add(self, path, method, response, **kwargs):
        route = self.Route(path=path, method=method)
        try:
            self.app.router.add_route(
                route.method, route.path, self._handler, **kwargs
            )
        except RuntimeError:
            pass
        finally:
            self._responses[route].append(response)

    def _find_response(self, request):
        def matches(route, request):

            if route.path != request.path:
                return False

            if route.method != request.method:
                return False

            return True

        for route, responses in self._responses.items():

            if not matches(route, request):
                continue

            try:
                response = responses.pop(0)
            except IndexError:
                response = None
            else:
                return route, response

            return route, None


@pytest.fixture
def make_aiohttp_client(aiohttp_client):

    config_dict = get_config(SERVICE_ID, defaults=DEFAULT_CONFIG)

    async def _make_aiohttp_client(
        config_dict=config_dict,
        mocked_routing_config={},
        mocked_endpoint_config={},
    ):

        assert len(mocked_routing_config.keys()) <= 1

        app = create_app(config_dict)
        # create mocked routing
        for host, mocked in mocked_routing_config.items():
            fake_routing = FakeServer(host=host)
            # mock routing responses
            for m in mocked:
                fake_routing.add(*m)
            info = await fake_routing.start()

            resolver = FakeResolver(info)
            routing_connector = aiohttp.TCPConnector(
                resolver=resolver, ssl=False
            )
            app["routing_http_conn_pool"] = routing_connector

        # create mocked endpoints
        faked_endpoint_info = {}
        for host, mocked in mocked_endpoint_config.items():
            fake_endpoint = FakeServer(host=host)
            # mock endpoint responses
            for m in mocked:
                fake_endpoint.add(*m)
            info = await fake_endpoint.start()
            faked_endpoint_info.update(info)

        resolver = FakeResolver(faked_endpoint_info)
        endpoint_connector = aiohttp.TCPConnector(resolver=resolver, ssl=False)
        app["endpoint_http_conn_pool"] = endpoint_connector

        return await aiohttp_client(app)

    return _make_aiohttp_client


@pytest.fixture(scope="session")
def fdsnws_dataselect_content_type():
    return "application/vnd.fdsn.mseed"


@pytest.fixture
def load_data():

    path_data = pathlib.Path(__file__).parent / "data"

    def _load_data(fname):
        with open(path_data / fname, "rb") as ifd:
            retval = ifd.read()

        return retval

    return _load_data


class TestFDSNDataselectServer:
    FDSNWS_DATASELECT_PATH_QUERY = "/fdsnws/dataselect/1/query"

    @staticmethod
    def get_default_config():
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict["pool_size"] = 1

        return get_config(SERVICE_ID, defaults=config_dict)

    async def test_get_single_stream_epoch(
        self, make_aiohttp_client, fdsnws_dataselect_content_type, load_data,
    ):

        method = "GET"
        mocked_routing = {
            "localhost": [
                (
                    "/eidaws/routing/1/query",
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
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
            ]
        }

        client = await make_aiohttp_client(
            config_dict=self.get_default_config(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        params = {
            "net": "CH",
            "sta": "HASLI",
            "loc": "--",
            "cha": "LHZ",
            "start": "2019-01-01",
            "end": "2019-01-05",
        }

        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data(
            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
        )

    async def test_get_multi_stream_epoch(
        self, make_aiohttp_client, fdsnws_dataselect_content_type, load_data,
    ):

        method = "GET"
        mocked_routing = {
            "localhost": [
                (
                    "/eidaws/routing/1/query",
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
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.DAVOX..LHZ.2019-01-01.2019-01-05T00:06:09"
                        ),
                    ),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
            ]
        }

        client = await make_aiohttp_client(
            config_dict=self.get_default_config(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        params = {
            "net": "CH",
            "sta": "DAVOX,HASLI",
            "loc": "--",
            "cha": "LHZ",
            "start": "2019-01-01",
            "end": "2019-01-05",
        }

        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data("CH.DAVOX,HASLI..LHZ.2019-01-01.2019-01-05")

    async def test_get_split_with_overlap(
        self, make_aiohttp_client, fdsnws_dataselect_content_type, load_data,
    ):
        method = "GET"
        mocked_routing = {
            "localhost": [
                (
                    "/eidaws/routing/1/query",
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
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(status=413),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-05.2019-01-10"),
                    ),
                ),
            ]
        }

        client = await make_aiohttp_client(
            config_dict=self.get_default_config(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        params = {
            "net": "CH",
            "sta": "HASLI",
            "loc": "--",
            "cha": "LHZ",
            "start": "2019-01-01",
            "end": "2019-01-10",
        }

        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data("CH.HASLI..LHZ.2019-01-01.2019-01-10")

    async def test_get_split_without_overlap(
        self, make_aiohttp_client, fdsnws_dataselect_content_type, load_data,
    ):
        method = "GET"
        mocked_routing = {
            "localhost": [
                (
                    "/eidaws/routing/1/query",
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
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(status=413),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-00T00:05:04"
                        ),
                    ),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01T05:05:00.2019-01-00T00:10:00"
                        ),
                    ),
                ),
            ]
        }

        client = await make_aiohttp_client(
            config_dict=self.get_default_config(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        params = {
            "net": "CH",
            "sta": "HASLI",
            "loc": "--",
            "cha": "LHZ",
            "start": "2019-01-01",
            "end": "2019-01-01T00:10:00",
        }

        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data(
            "CH.HASLI..LHZ.2019-01-01.2019-01-01T00:10:00"
        )

    async def test_get_multi_split_with_overlap(
        self, make_aiohttp_client, fdsnws_dataselect_content_type, load_data,
    ):

        method = "GET"
        mocked_routing = {
            "localhost": [
                (
                    "/eidaws/routing/1/query",
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
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(status=413),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(status=413),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data(
                            "CH.HASLI..LHZ.2019-01-01.2019-01-05T00:05:45"
                        ),
                    ),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-05.2019-01-10"),
                    ),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(status=413),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-10.2019-01-15"),
                    ),
                ),
                (
                    self.FDSNWS_DATASELECT_PATH_QUERY,
                    method,
                    web.Response(
                        status=200,
                        body=load_data("CH.HASLI..LHZ.2019-01-15.2019-01-20"),
                    ),
                ),
            ]
        }

        client = await make_aiohttp_client(
            config_dict=self.get_default_config(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        params = {
            "net": "CH",
            "sta": "HASLI",
            "loc": "--",
            "cha": "LHZ",
            "start": "2019-01-01",
            "end": "2019-01-20",
        }

        resp = await client.get(_PATH_QUERY, params=params)
        assert resp.status == 200
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_dataselect_content_type
        )
        data = await resp.read()

        assert data == load_data("CH.HASLI..LHZ.2019-01-01.2019-01-20")
