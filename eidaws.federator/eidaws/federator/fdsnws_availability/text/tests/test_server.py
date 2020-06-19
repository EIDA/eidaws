import copy
import functools
import pytest

from aiohttp import web

from eidaws.federator.fdsnws_availability.text import create_app, SERVICE_ID
from eidaws.federator.fdsnws_availability.text.app import DEFAULT_CONFIG
from eidaws.federator.fdsnws_availability.text.route import (
    FED_AVAILABILITY_TEXT_PATH_EXTENT,
    FED_AVAILABILITY_TEXT_PATH_QUERY,
)
from eidaws.federator.utils.misc import get_config
from eidaws.federator.fdsnws_availability.tests.server_mixin import (
    _TestAPIMixin,
)
from eidaws.federator.utils.pytest_plugin import (
    fdsnws_availability_text_content_type,
    fdsnws_error_content_type,
    eidaws_routing_path_query,
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
    FDSNWS_AVAILABILITY_PATH_EXTENT,
    FDSNWS_AVAILABILITY_PATH_QUERY,
)


@pytest.fixture
def content_tester(load_data):
    async def _content_tester(resp, expected=None):
        assert expected is not None
        assert await resp.text() == load_data(expected, reader="read_text")

    return _content_tester


class _TestAvailabilityTextServerMixin:
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


class TestFDSNAvailabilityQueryServer(
    _TestCommonServerConfig,
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
    _TestAPIMixin,
    _TestAvailabilityTextServerMixin,
    _TestServerBase,
):
    FED_PATH_RESOURCE = FED_AVAILABILITY_TEXT_PATH_QUERY
    PATH_RESOURCE = FDSNWS_AVAILABILITY_PATH_QUERY
    SERVICE_ID = SERVICE_ID

    _DEFAULT_SERVER_CONFIG = {"pool_size": 1}

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            ("GET", {"show": "foo"}),
            ("POST", b"show=foo\nNET STA LOC CHA 2020-01-01 2020-01-02"),
            ("GET", {"show": ""}),
            ("POST", b"show=\nNET STA LOC CHA 2020-01-01 2020-01-02"),
        ],
    )
    async def test_bad_request_query(
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
                    "sta": "ZELS,MALA",
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
            "result": "CL,FR.ZELS,MALA.00.HHZ.2019-01-01.2020-01-01.query",
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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


class TestFDSNAvailabilityExtentServer(
    _TestCORSMixin,
    _TestKeywordParserMixin,
    _TestRoutingMixin,
    _TestAPIMixin,
    _TestAvailabilityTextServerMixin,
    _TestServerBase,
):
    FED_PATH_RESOURCE = FED_AVAILABILITY_TEXT_PATH_EXTENT
    PATH_RESOURCE = FDSNWS_AVAILABILITY_PATH_EXTENT
    SERVICE_ID = SERVICE_ID

    _DEFAULT_SERVER_CONFIG = {"pool_size": 1}

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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
                    "sta": "ZELS,MALA",
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
            "result": "CL,FR.ZELS,MALA.00.HHZ.2019-01-01.2020-01-01.extent",
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
        fdsnws_availability_text_content_type,
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
            "content_type": fdsnws_availability_text_content_type,
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
