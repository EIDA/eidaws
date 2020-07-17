# -*- coding: utf-8 -*-

import pytest

from aiohttp import web


class _TestServerBase:

    # override within implementation
    FED_PATH_RESOURCE = None
    PATH_RESOURCE = None
    SERVICE_ID = None

    @classmethod
    def lookup_config(cls, key, config_dict):
        return config_dict[key]


class _TestRoutingMixin:
    """
    Routing specific tests for test classes providing both the properties
    ``FED_PATH_RESOURCE`` and ``PATH_RESOURCE`` and a ``create_app`` method.
    """

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "FOO",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2019-01-05",
                },
            ),
            ("POST", b"CH FOO -- LHZ 2019-01-01 2019-01-05",),
        ],
    )
    async def test_no_route(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        method,
        params_or_data,
    ):
        mocked_routing = {
            "localhost": [
                (eidaws_routing_path_query, method, web.Response(status=204,),)
            ]
        }
        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(), mocked_routing_config=mocked_routing,
        )

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(self.FED_PATH_RESOURCE, **kwargs)

        assert resp.status == 204

        faked_routing.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "FOO",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2019-01-05",
                },
            ),
            ("POST", b"CH FOO -- LHZ 2019-01-01 2019-01-05",),
        ],
    )
    async def test_no_data(
        self,
        make_federated_eida,
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
                            "http://eida.ethz.ch" + self.PATH_RESOURCE + "\n"
                            "CH FOO -- LHZ "
                            "2019-01-01T00:00:00 2019-01-05T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        mocked_endpoints = {
            "eida.ethz.ch": [
                (self.PATH_RESOURCE, "GET", web.Response(status=204,),),
            ]
        }

        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(self.FED_PATH_RESOURCE, **kwargs)

        assert resp.status == 204

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "CH",
                    "sta": "FOO",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2019-01-05",
                },
            ),
            ("POST", b"CH FOO -- LHZ 2019-01-01 2019-01-05",),
        ],
    )
    async def test_routing_unavailable(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        fdsnws_error_content_type,
        aiohttp_unused_port,
        method,
        params_or_data,
    ):
        async def tester(resp):
            assert resp.status == 500
            assert (
                "Content-Type" in resp.headers
                and resp.headers["Content-Type"] == fdsnws_error_content_type
            )
            assert "Error while routing" in await resp.text()

        _method = method.lower()
        kwargs = {"params" if _method == "get" else "data": params_or_data}

        config_dict = self.get_config(
            **{
                "url_routing": (
                    f"http://localhost:{aiohttp_unused_port()}"
                    f"{eidaws_routing_path_query}"
                )
            }
        )

        client, _, _ = await make_federated_eida(
            self.create_app(config_dict=config_dict)
        )
        resp = await getattr(client, _method)(self.FED_PATH_RESOURCE, **kwargs)

        await tester(resp)

        mocked_routing = {
            "localhost": [
                (eidaws_routing_path_query, method, web.Response(status=500,),)
            ]
        }
        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(), mocked_routing_config=mocked_routing,
        )
        resp = await getattr(client, _method)(self.FED_PATH_RESOURCE, **kwargs)

        await tester(resp)

        faked_routing.assert_no_unused_routes()


class _TestKeywordParserMixin:
    """
    Keyword parser specific tests for test classes providing both the property
    ``FED_PATH_RESOURCE`` and a ``create_app`` method.
    """

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            ("GET", {"foo": "bar"},),
            ("POST", b"foo=bar\nCH HASLI -- LHZ 2019-01-01 2019-01-05",),
        ],
    )
    async def test_invalid_args(
        self,
        make_federated_eida,
        fdsnws_error_content_type,
        method,
        params_or_data,
    ):
        client, _, _ = await make_federated_eida(self.create_app())

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(self.FED_PATH_RESOURCE, **kwargs)

        assert resp.status == 400
        assert (
            f"ValidationError: Invalid request query parameters: {{'foo'}}"
            in await resp.text()
        )
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_error_content_type
        )

    async def test_post_empty(
        self, make_federated_eida, fdsnws_error_content_type,
    ):
        client, _, _ = await make_federated_eida(self.create_app())

        data = b""
        resp = await client.post(self.FED_PATH_RESOURCE, data=data)

        assert resp.status == 400
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_error_content_type
        )

    async def test_post_equal(
        self, make_federated_eida, fdsnws_error_content_type
    ):
        client, _, _ = await make_federated_eida(self.create_app())

        data = b"="
        resp = await client.post(self.FED_PATH_RESOURCE, data=data)

        assert resp.status == 400
        assert "ValidationError: RTFM :)." in await resp.text()
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_error_content_type
        )


class _TestCORSMixin:
    """
    CORS related tests for test classes providing both the property
    ``FED_PATH_RESOURCE`` and a ``create_app`` method.
    """

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            ("GET", {"foo": "bar"},),
            ("POST", b"foo=bar\nCH HASLI -- LHZ 2019-01-01 2019-01-05",),
        ],
    )
    async def test_get_cors_simple(
        self, make_federated_eida, method, params_or_data
    ):
        client, _, _ = await make_federated_eida(self.create_app())

        origin = "http://foo.example.com"

        method = method.lower()
        kwargs = {"params" if method == "get" else "data": params_or_data}
        resp = await getattr(client, method)(
            self.FED_PATH_RESOURCE, headers={"Origin": origin}, **kwargs
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

    @pytest.mark.parametrize("method", ["GET", "POST"])
    async def test_cors_preflight(self, make_federated_eida, method):
        client, _, _ = await make_federated_eida(self.create_app())

        origin = "http://foo.example.com"
        headers = {"Origin": origin, "Access-Control-Request-Method": method}

        resp = await client.options(self.FED_PATH_RESOURCE, headers=headers)

        assert resp.status == 200
        assert (
            "Access-Control-Allow-Methods" in resp.headers
            and resp.headers["Access-Control-Allow-Methods"] == method
        )
        assert (
            "Access-Control-Allow-Origin" in resp.headers
            and resp.headers["Access-Control-Allow-Origin"] == origin
        )

    @pytest.mark.parametrize("method", ["GET", "POST"])
    async def test_cors_preflight_forbidden(self, make_federated_eida, method):
        client, _, _ = await make_federated_eida(self.create_app())

        origin = "http://foo.example.com"

        resp = await client.options(
            self.FED_PATH_RESOURCE, headers={"Origin": origin}
        )
        assert resp.status == 403

        resp = await client.options(
            self.FED_PATH_RESOURCE,
            headers={"Access-Control-Request-Method": method},
        )
        assert resp.status == 403


class _TestCommonServerConfig:
    """
    Server configuration tests for test classes providing the properties
    ``FED_PATH_RESOURCE`` and ``PATH_RESOURCE`` and both a ``create_app`` and a
    ``get_config`` method.
    """

    async def test_client_max_size(
        self, make_federated_eida,
    ):

        # avoid large POST requests
        client_max_size = 32
        config_dict = self.get_config(**{"client_max_size": client_max_size})

        client, _, _ = await make_federated_eida(
            self.create_app(config_dict=config_dict)
        )

        data = b"level=channel\n" b"\n" b"CH * * * 2020-01-01 2020-01-02"

        assert client_max_size < len(data)

        resp = await client.post(self.FED_PATH_RESOURCE, data=data)

        assert resp.status == 413

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "NET",
                    "sta": "STA",
                    "loc": "LOC",
                    "cha": "CHA",
                    "start": "2020-01-01",
                    "end": "2020-01-02",
                },
            ),
            ("POST", b"NET STA LOC CHA 2020-01-01 2020-01-02"),
        ],
    )
    async def test_max_stream_epoch_duration_ok(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        method,
        params_or_data,
    ):
        # NOTE(damb): For fdsnws-station-* resource implementations the query
        # filter parameter level=channel would be required to simulate a
        # proper behaviour; however, since the response depends actually on the
        # routing service's response the mixin can be used for testing
        # fdsnws-station-* implementations, too.
        config_dict = self.get_config(
            **{"pool_size": 1, "max_stream_epoch_duration": 1}
        )

        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://example.com" + self.PATH_RESOURCE + "\n"
                            "NET STA LOC CHA "
                            "2020-01-01T00:00:00 2020-01-02T00:00:00\n"
                        ),
                    ),
                )
            ]
        }
        mocked_endpoints = {
            "example.com": [
                (self.PATH_RESOURCE, "GET", web.Response(status=204,),),
            ]
        }

        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(config_dict=config_dict),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        _method = method.lower()
        kwargs = {"params" if _method == "get" else "data": params_or_data}
        resp = await getattr(client, _method)(self.FED_PATH_RESOURCE, **kwargs)
        assert resp.status == 204

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "NET",
                    "sta": "STA",
                    "loc": "LOC",
                    "cha": "CHA",
                    "start": "2020-01-01",
                    "end": "2020-01-02T00:00:01",
                },
            ),
            ("POST", b"NET STA LOC CHA 2020-01-01 2020-01-02T00:00:01"),
        ],
    )
    async def test_max_stream_epoch_duration_exceeded(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        method,
        params_or_data,
    ):
        # NOTE(damb): For fdsnws-station-* resource implementations the query
        # filter parameter level=channel would be required to simulate a
        # proper behaviour; however, since the response depends actually on the
        # routing service's response the mixin can be used for testing
        # fdsnws-station-* implementations, too.
        config_dict = self.get_config(
            **{"pool_size": 1, "max_stream_epoch_duration": 1}
        )

        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://example.com" + self.PATH_RESOURCE + "\n"
                            "NET STA LOC CHA "
                            "2020-01-01T00:00:00 2020-01-02T00:00:01\n"
                        ),
                    ),
                )
            ]
        }

        client, faked_routing, _ = await make_federated_eida(
            self.create_app(config_dict=config_dict),
            mocked_routing_config=mocked_routing,
        )

        _method = method.lower()
        kwargs = {"params" if _method == "get" else "data": params_or_data}
        resp = await getattr(client, _method)(self.FED_PATH_RESOURCE, **kwargs)
        assert resp.status == 413

        faked_routing.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "NET",
                    "sta": "STA",
                    "loc": "LOC",
                    "cha": "CHA?",
                    "start": "2020-01-01",
                    "end": "2020-01-02",
                },
            ),
            ("POST", b"NET STA LOC CHA? 2020-01-01 2020-01-02"),
        ],
    )
    async def test_max_total_stream_epoch_duration_ok(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        method,
        params_or_data,
    ):
        # NOTE(damb): For fdsnws-station-* resource implementations the query
        # filter parameter level=channel would be required to simulate a
        # proper behaviour; however, since the response depends actually on the
        # routing service's response the mixin can be used for testing
        # fdsnws-station-* implementations, too.
        config_dict = self.get_config(
            **{"pool_size": 1, "max_total_stream_epoch_duration": 3}
        )

        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://example.com" + self.PATH_RESOURCE + "\n"
                            "NET STA LOC CHAE "
                            "2020-01-01T00:00:00 2020-01-02T00:00:00\n"
                            "NET STA LOC CHAN "
                            "2020-01-01T00:00:00 2020-01-02T00:00:00\n"
                            "NET STA LOC CHAZ "
                            "2020-01-01T00:00:00 2020-01-02T00:00:00\n"
                        ),
                    ),
                )
            ]
        }
        mocked_endpoints = {
            "example.com": [
                (self.PATH_RESOURCE, "GET", web.Response(status=204,),),
                (self.PATH_RESOURCE, "GET", web.Response(status=204,),),
                (self.PATH_RESOURCE, "GET", web.Response(status=204,),),
            ]
        }

        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(config_dict=config_dict),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        _method = method.lower()
        kwargs = {"params" if _method == "get" else "data": params_or_data}
        resp = await getattr(client, _method)(self.FED_PATH_RESOURCE, **kwargs)
        assert resp.status == 204

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "NET",
                    "sta": "STA",
                    "loc": "LOC",
                    "cha": "CHA?",
                    "start": "2020-01-01",
                    "end": "2020-01-02T00:00:01",
                },
            ),
            ("POST", b"NET STA LOC CHA? 2020-01-01 2020-01-02T00:00:01"),
        ],
    )
    async def test_max_total_stream_epoch_duration_exceeded(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        method,
        params_or_data,
    ):
        # NOTE(damb): For fdsnws-station-* resource implementations the query
        # filter parameter level=channel would be required to simulate a
        # proper behaviour; however, since the response depends actually on the
        # routing service's response the mixin can be used for testing
        # fdsnws-station-* implementations, too.
        config_dict = self.get_config(
            **{"pool_size": 1, "max_total_stream_epoch_duration": 3}
        )

        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://example.com" + self.PATH_RESOURCE + "\n"
                            "NET STA LOC CHAE "
                            "2020-01-01T00:00:00 2020-01-02T00:00:01\n"
                            "NET STA LOC CHAN "
                            "2020-01-01T00:00:00 2020-01-02T00:00:01\n"
                            "NET STA LOC CHAZ "
                            "2020-01-01T00:00:00 2020-01-02T00:00:01\n"
                        ),
                    ),
                )
            ]
        }

        client, faked_routing, _ = await make_federated_eida(
            self.create_app(config_dict=config_dict),
            mocked_routing_config=mocked_routing,
        )

        _method = method.lower()
        kwargs = {"params" if _method == "get" else "data": params_or_data}
        resp = await getattr(client, _method)(self.FED_PATH_RESOURCE, **kwargs)
        assert resp.status == 413

        faked_routing.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "NET",
                    "sta": "STA",
                    "loc": "LOC",
                    "cha": "CHAZ",
                    "start": "2020-01-01",
                    "end": "2020-01-03",
                },
            ),
            ("POST", b"NET STA LOC CHAZ 2020-01-01 2020-01-03"),
        ],
    )
    async def test_max_stream_epoch_durations_ok(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        method,
        params_or_data,
    ):
        # NOTE(damb): For fdsnws-station-* resource implementations the query
        # filter parameter level=channel would be required to simulate a
        # proper behaviour; however, since the response depends actually on the
        # routing service's response the mixin can be used for testing
        # fdsnws-station-* implementations, too.
        config_dict = self.get_config(
            **{
                "max_stream_epoch_durations": 2,
                "max_total_stream_epoch_duration": 3,
            }
        )

        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://example.com" + self.PATH_RESOURCE + "\n"
                            "NET STA LOC CHAZ "
                            "2020-01-01T00:00:00 2020-01-03T00:00:00\n"
                        ),
                    ),
                )
            ]
        }
        mocked_endpoints = {
            "example.com": [
                (self.PATH_RESOURCE, "GET", web.Response(status=204,),),
            ]
        }

        client, faked_routing, faked_endpoints = await make_federated_eida(
            self.create_app(config_dict=config_dict),
            mocked_routing_config=mocked_routing,
            mocked_endpoint_config=mocked_endpoints,
        )

        _method = method.lower()
        kwargs = {"params" if _method == "get" else "data": params_or_data}
        resp = await getattr(client, _method)(self.FED_PATH_RESOURCE, **kwargs)
        assert resp.status == 204

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "NET",
                    "sta": "STA",
                    "loc": "LOC",
                    "cha": "CHAN,CHAZ",
                    "start": "2020-01-01",
                    "end": "2020-01-03",
                },
            ),
            (
                "POST",
                (
                    b"NET STA LOC CHAN 2020-01-01 2020-01-03\n"
                    b"NET STA LOC CHAZ 2020-01-01 2020-01-03"
                ),
            ),
        ],
    )
    async def test_max_stream_epoch_durations_exceeded(
        self,
        make_federated_eida,
        eidaws_routing_path_query,
        method,
        params_or_data,
    ):
        # NOTE(damb): For fdsnws-station-* resource implementations the query
        # filter parameter level=channel would be required to simulate a
        # proper behaviour; however, since the response depends actually on the
        # routing service's response the mixin can be used for testing
        # fdsnws-station-* implementations, too.
        config_dict = self.get_config(
            **{
                "max_stream_epoch_durations": 2,
                "max_total_stream_epoch_duration": 3,
            }
        )
        mocked_routing = {
            "localhost": [
                (
                    eidaws_routing_path_query,
                    method,
                    web.Response(
                        status=200,
                        text=(
                            "http://example.com" + self.PATH_RESOURCE + "\n"
                            "NET STA LOC CHAN "
                            "2020-01-01T00:00:00 2020-01-03T00:00:00\n"
                            "NET STA LOC CHAZ "
                            "2020-01-01T00:00:00 2020-01-03T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        client, faked_routing, _ = await make_federated_eida(
            self.create_app(config_dict=config_dict),
            mocked_routing_config=mocked_routing,
        )

        _method = method.lower()
        kwargs = {"params" if _method == "get" else "data": params_or_data}
        resp = await getattr(client, _method)(self.FED_PATH_RESOURCE, **kwargs)
        assert resp.status == 413

        faked_routing.assert_no_unused_routes()
