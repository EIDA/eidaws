# -*- coding: utf-8 -*-

import pytest

from aiohttp import web


class _TestAPIMixin:
    """
    Keyword parser specific tests for test classes providing both the property
    ``FED_PATH_RESOURCE`` and a ``create_app`` method.
    """

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            ("GET", {"merge": "foo"}),
            ("POST", b"merge=foo\nNET STA LOC CHA 2020-01-01 2020-01-02"),
            ("GET", {"merge": "quality,foo"}),
            (
                "POST",
                b"merge=quality,foo\nNET STA LOC CHA 2020-01-01 2020-01-02",
            ),
            ("GET", {"merge": ""}),
            ("POST", b"merge=\nNET STA LOC CHA 2020-01-01 2020-01-02"),
            ("GET", {"orderby": "foo"}),
            ("POST", b"orderby=foo\nNET STA LOC CHA 2020-01-01 2020-01-02"),
            ("GET", {"orderby": "nslc_time_quality_samplerate,foo"}),
            (
                "POST",
                (
                    b"orderby=nslc_time_quality_samplerate,foo\n"
                    b"NET STA LOC CHA 2020-01-01 2020-01-02"
                ),
            ),
            ("GET", {"orderby": ""}),
            ("POST", b"orderby=\nNET STA LOC CHA 2020-01-01 2020-01-02"),
            ("GET", {"limit": "foo"}),
            ("POST", b"limit=foo\nNET STA LOC CHA 2020-01-01 2020-01-02"),
            ("GET", {"limit": ""}),
            ("POST", b"limit=\nNET STA LOC CHA 2020-01-01 2020-01-02"),
            ("GET", {"limit": "0"}),
            ("POST", b"limit=0\nNET STA LOC CHA 2020-01-01 2020-01-02"),
        ],
        ids=[
            "method=GET,merge=foo",
            "method=POST,merge=foo",
            "method=GET,merge=quality,foo",
            "method=POST,merge=quality,foo",
            'method=GET,merge=""',
            "method=POST,merge=",
            "method=GET,orderby=foo",
            "method=POST,orderby=foo",
            "method=GET,orderby=nslc_time_quality_samplerate,foo",
            "method=POST,orderby=nslc_time_quality_samplerate,foo",
            'method=GET,orderby=""',
            "method=POST,orderby=",
            "method=GET,limit=foo",
            "method=POST,limit=foo",
            'method=GET,limit=""',
            "method=POST,limit=",
            "method=GET,limit=0",
            "method=POST,limit=0",
        ],
    )
    async def test_bad_request(
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
        assert f"Error 400: Bad request" in await resp.text()
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_error_content_type
        )


class _TestAvailabilityQueryMixin:
    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                },
            ),
            ("POST", b"FR ZELS 00 LHZ 2019-01-01 2020-01-01",),
        ],
    )
    async def test_single_net_sta_cha(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/query\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.query"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.query",
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                    "merge": "samplerate",
                },
            ),
            (
                "POST",
                b"merge=samplerate\nFR ZELS 00 LHZ 2019-01-01 2020-01-01",
            ),
        ],
    )
    async def test_single_net_sta_cha_merge_samplerate(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/query\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.merge_samplerate"
                            ".query"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": (
                "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.merge_samplerate"
                ".query"
            ),
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                    "merge": "quality",
                },
            ),
            ("POST", b"merge=quality\nFR ZELS 00 LHZ 2019-01-01 2020-01-01",),
        ],
    )
    async def test_single_net_sta_cha_merge_quality(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/query\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.merge_quality"
                            ".query"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": (
                "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.merge_quality.query"
            ),
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                    "merge": "samplerate,quality",
                },
            ),
            (
                "POST",
                (
                    b"merge=samplerate,quality\n"
                    b"FR ZELS 00 LHZ 2019-01-01 2020-01-01"
                ),
            ),
        ],
    )
    async def test_single_net_sta_cha_merge_samplerate_quality(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/query\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.LHZ.2019-01-01.2020-01-01."
                            "merge_samplerate_quality.query"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": (
                "FR.ZELS.00.LHZ.2019-01-01.2020-01-01."
                "merge_samplerate_quality.query"
            ),
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                    "mergegaps": "10",
                },
            ),
            (
                "POST",
                (b"mergegaps=10\n" b"FR ZELS 00 LHZ 2019-01-01 2020-01-01"),
            ),
        ],
    )
    async def test_single_net_sta_cha_mergegaps(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/query\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.mergegaps_10"
                            ".query"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": (
                "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.mergegaps_10.query"
            ),
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "HHZ,LHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                },
            ),
            (
                "POST",
                (
                    b"FR ZELS 00 HHZ 2019-01-01 2020-01-01\n"
                    b"FR ZELS 00 LHZ 2019-01-01 2020-01-01"
                ),
            ),
        ],
    )
    async def test_single_net_sta_multi_chas(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/query\n"
                            "FR ZELS 00 HHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.HHZ.2019-01-01.2020-01-01.query"
                        ),
                    ),
                ),
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.query"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": "FR.ZELS.00.HHZ,LHZ.2019-01-01.2020-01-01.query",
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
                    "net": "CL,FR",
                    "sta": "MALA,ZELS",
                    "loc": "00",
                    "cha": "HHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                },
            ),
            (
                "POST",
                (
                    b"CL MALA 00 HHZ 2019-01-01 2020-01-01\n"
                    b"FR ZELS 00 HHZ 2019-01-01 2020-01-01"
                ),
            ),
        ],
    )
    async def test_multi_nets(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/query\n"
                            "CL MALA 00 HHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config, **{"pool_size": 2})
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "CL.MALA.00.HHZ.2019-01-01.2020-01-01.query"
                        ),
                    ),
                ),
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.HHZ.2019-01-01.2020-01-01.query"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": "CL,FR.MALA,ZELS.00.HHZ.2019-01-01.2020-01-01.query",
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                },
            ),
            ("POST", b"FR ZELS 00 LHZ 2019-01-01 2020-01-01",),
        ],
    )
    async def test_cached(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/query\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config, **cache_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.query"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.query",
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


class _TestAvailabilityExtentMixin:
    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            ("GET", {"merge": "overlap"}),
            ("POST", b"merge=overlap\nNET STA LOC CHA 2020-01-01 2020-01-02"),
        ],
    )
    async def test_bad_request_extent(
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
        assert "Error 400: Bad request" in await resp.text()
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_error_content_type
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "HHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                },
            ),
            ("POST", b"FR ZELS 00 HHZ 2019-01-01 2020-01-01",),
        ],
    )
    async def test_single_net_sta_cha(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/extent\n"
                            "FR ZELS 00 HHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.HHZ.2019-01-01.2020-01-01.extent"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": "FR.ZELS.00.HHZ.2019-01-01.2020-01-01.extent",
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "HHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                    "merge": "samplerate",
                },
            ),
            (
                "POST",
                b"merge=samplerate\nFR ZELS 00 HHZ 2019-01-01 2020-01-01",
            ),
        ],
    )
    async def test_single_net_sta_cha_merge_samplerate(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/extent\n"
                            "FR ZELS 00 HHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.HHZ.2019-01-01.2020-01-01."
                            "merge_samplerate.extent"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": (
                "FR.ZELS.00.HHZ.2019-01-01.2020-01-01.merge_samplerate"
                ".extent"
            ),
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "HHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                    "merge": "quality",
                },
            ),
            ("POST", b"merge=quality\nFR ZELS 00 HHZ 2019-01-01 2020-01-01",),
        ],
    )
    async def test_single_net_sta_cha_merge_quality(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/extent\n"
                            "FR ZELS 00 HHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.HHZ.2019-01-01.2020-01-01.merge_quality"
                            ".extent"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": (
                "FR.ZELS.00.HHZ.2019-01-01.2020-01-01.merge_quality.extent"
            ),
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "HHZ,LHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                },
            ),
            (
                "POST",
                (
                    b"FR ZELS 00 HHZ 2019-01-01 2020-01-01\n"
                    b"FR ZELS 00 LHZ 2019-01-01 2020-01-01"
                ),
            ),
        ],
    )
    async def test_single_net_sta_multi_chas(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/extent\n"
                            "FR ZELS 00 HHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.HHZ.2019-01-01.2020-01-01.extent"
                        ),
                    ),
                ),
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.extent"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": "FR.ZELS.00.HHZ,LHZ.2019-01-01.2020-01-01.extent",
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
                    "net": "CL,FR",
                    "sta": "MALA,ZELS",
                    "loc": "00",
                    "cha": "HHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                },
            ),
            (
                "POST",
                (
                    b"CL MALA 00 HHZ 2019-01-01 2020-01-01\n"
                    b"FR ZELS 00 HHZ 2019-01-01 2020-01-01"
                ),
            ),
        ],
    )
    async def test_multi_nets(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/extent\n"
                            "CL MALA 00 HHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config, **{"pool_size": 2})
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "CL.MALA.00.HHZ.2019-01-01.2020-01-01.extent"
                        ),
                    ),
                ),
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.HHZ.2019-01-01.2020-01-01.extent"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": "CL,FR.MALA,ZELS.00.HHZ.2019-01-01.2020-01-01.extent",
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
                    "net": "FR",
                    "sta": "ZELS",
                    "loc": "00",
                    "cha": "LHZ",
                    "start": "2019-01-01",
                    "end": "2020-01-01",
                },
            ),
            ("POST", b"FR ZELS 00 LHZ 2019-01-01 2020-01-01",),
        ],
    )
    async def test_cached(
        self,
        server_config,
        tester,
        eidaws_routing_path_query,
        fdsnws_availability_content_type,
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
                            "http://ws.resif.fr/fdsnws/availability/1/extent\n"
                            "FR ZELS 00 LHZ "
                            "2019-01-01T00:00:00 2020-01-01T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        config_dict = server_config(self.get_config, **cache_config)
        mocked_endpoints = {
            "ws.resif.fr": [
                (
                    self.PATH_RESOURCE,
                    self.lookup_config("endpoint_request_method", config_dict),
                    web.Response(
                        status=200,
                        body=load_data(
                            "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.extent"
                        ),
                    ),
                ),
            ]
        }

        expected = {
            "status": 200,
            "content_type": fdsnws_availability_content_type,
            "result": "FR.ZELS.00.LHZ.2019-01-01.2020-01-01.extent",
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
