# -*- coding: utf-8 -*-

import aiohttp
import collections
import pathlib
import pytest
import socket

from aiohttp import web
from aiohttp.resolver import DefaultResolver
from aiohttp.test_utils import unused_port

from eidaws.federator.utils.misc import RedisError


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

    def assert_no_unused_routes(self):
        assert not self._responses

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
                if not responses:
                    del self._responses[route]
            except IndexError:
                response = None
            else:
                return route, response

            return route, None


@pytest.fixture
def make_federated_eida(aiohttp_client):
    class FakedEndpointDictWrapper(dict):
        def assert_no_unused_routes(self):
            for faked_server in self.values():
                faked_server.assert_no_unused_routes()

    async def _make_federated_eida(
        app_factory, mocked_routing_config={}, mocked_endpoint_config={},
    ):

        assert len(mocked_routing_config.keys()) <= 1

        app = app_factory()
        faked_routing = None
        # create mocked routing
        for host, mocked in mocked_routing_config.items():
            faked_routing = FakeServer(host=host)
            # mock routing responses
            for m in mocked:
                faked_routing.add(*m)
            info = await faked_routing.start()

            resolver = FakeResolver(info)
            routing_connector = aiohttp.TCPConnector(
                resolver=resolver, ssl=False
            )
            app["routing_http_conn_pool"] = routing_connector

        # create mocked endpoints
        faked_endpoint_info = {}
        faked_endpoints = FakedEndpointDictWrapper()
        for host, mocked in mocked_endpoint_config.items():
            fake_endpoint = FakeServer(host=host)
            faked_endpoints[host] = fake_endpoint
            # mock endpoint responses
            for m in mocked:
                fake_endpoint.add(*m)
            info = await fake_endpoint.start()
            faked_endpoint_info.update(info)

        resolver = FakeResolver(faked_endpoint_info)
        endpoint_connector = aiohttp.TCPConnector(resolver=resolver, ssl=False)
        app["endpoint_http_conn_pool"] = endpoint_connector

        try:
            client = await aiohttp_client(app)
        except RedisError as err:
            pytest.skip(str(err))

        return client, faked_routing, faked_endpoints

    return _make_federated_eida


@pytest.fixture(scope="session")
def fdsnws_dataselect_content_type():
    return "application/vnd.fdsn.mseed"


@pytest.fixture(scope="session")
def fdsnws_station_xml_content_type():
    return "application/xml"


@pytest.fixture(scope="session")
def fdsnws_station_text_content_type():
    return "text/plain; charset=utf-8"


fdsnws_error_content_type = fdsnws_station_text_content_type


@pytest.fixture(scope="session")
def eidaws_routing_path_query():
    return "/eidaws/routing/1/query"


@pytest.fixture
def load_data(request):
    path_data = pathlib.Path(request.fspath.dirname) / "data"

    def _load_data(fname, reader="read_bytes"):
        return getattr((path_data / fname), reader)()

    return _load_data


@pytest.fixture(
    params=[
        {"pool_size": 1, "endpoint_request_method": "GET"},
        {"pool_size": 1, "endpoint_request_method": "POST"},
    ],
    ids=["req_method=GET", "req_method=POST"]
)
def server_config(request):
    def _get_config(config_factory, **kwargs):
        kwargs.update(request.param)
        config = config_factory(**kwargs)
        return config

    return _get_config


@pytest.fixture
def tester(make_federated_eida, content_tester):
    async def _tester(
        path,
        method,
        params_or_data,
        app_factory,
        mocked_routing,
        mocked_endpoints,
        expected,
    ):

        client, faked_routing, faked_endpoints = await make_federated_eida(
            app_factory(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(path, **kwargs)

        assert resp.status == expected["status"]
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == expected["content_type"]
        )

        await content_tester(resp, expected=expected.get("result"))

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

    return _tester
