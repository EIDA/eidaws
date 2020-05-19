# -*- coding: utf-8 -*-

import pathlib
import pytest

from eidaws.stationlite.server import create_app
from eidaws.stationlite.version import __version__
from eidaws.utils.settings import (
    EIDAWS_ROUTING_PATH_QUERY,
    FDSNWS_QUERY_METHOD_TOKEN,
    FDSNWS_QUERYAUTH_METHOD_TOKEN,
)

_path_module = pathlib.Path(__file__).parent


@pytest.fixture
def client():
    app = create_app()
    app.config[
        "SQLALCHEMY_DATABASE_URI"
    ] = f"sqlite:///{_path_module}/data/test.db"
    app.config["TESTING"] = True

    with app.test_client() as client:
        yield client


@pytest.fixture(
    params=[
        {"service": "station"},
        {"service": "dataselect"},
        {"service": "wfcatalog"},
    ],
    ids=["service=station", "service=dataselect", "service=wfcatalog"],
)
def service_args(request):
    return request.param


@pytest.fixture(
    params=[
        {"level": "network"},
        {"level": "station"},
        {"level": "channel"},
        {"level": "response"},
    ],
    ids=["level=network", "level=station", "level=channel", "level=response"],
)
def level_args(request):
    return request.param


@pytest.fixture(
    params=[{"access": "open"}, {"access": "closed"}, {"access": "any"}],
    ids=["access=open", "access=closed", "access=any"],
)
def access_args(request):
    return request.param


@pytest.fixture
def content_type():
    def _content_type(query_format_or_status_code):
        if query_format_or_status_code in ("post", "get", 200, 204, 413, 500):
            return "text/plain; charset=utf-8"

    return _content_type


def build_query_string(mappings):
    return "&".join(f"{k}={v}" for m in mappings for k, v in m.items())


def build_postfile(sncls, **filter_args):
    payload = [f"{k}={v}".encode("utf-8") for k, v in filter_args.items()]
    payload.extend(sncls)

    return b"\n".join(payload)


def create_url(netloc, service_arg, method_token=FDSNWS_QUERY_METHOD_TOKEN):
    service_scopes = {
        "station": "fdsnws",
        "dataselect": "fdsnws",
        "wfcatalog": "eidaws",
    }
    service = service_arg["service"]

    return (
        f"http://{netloc}/{service_scopes[service]}/{service}/1/{method_token}"
    ).encode("utf-8")


def create_request_kwargs(method, params_or_data, **filter_args):
    method = method.lower()
    if method == "get":
        return {
            "query_string": build_query_string([filter_args, params_or_data])
        }
    return {"data": build_postfile(params_or_data, **filter_args)}


class TestStationLiteServer:
    def test_version(self, client):
        resp = client.get("eidaws/routing/1/version")

        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "text/plain; charset=utf-8"
        assert __version__.encode("utf-8") == resp.data

    def test_wadl(self, client):
        resp = client.get("eidaws/routing/1/application.wadl")

        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == "application/xml"
        # TODO(damb): Test response body

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "get",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-02",
                },
            ),
            ("post", [b"CH HASLI -- LHZ 2020-01-01 2020-01-02"]),
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_single_sncl_single_dc(
        self, client, service_args, content_type, method, params_or_data
    ):

        req_kwargs = create_request_kwargs(
            method, params_or_data, **service_args
        )
        resp = getattr(client, method)(EIDAWS_ROUTING_PATH_QUERY, **req_kwargs)

        expected = [
            create_url("eida.ethz.ch", service_args),
            b"CH HASLI -- LHZ 2020-01-01T00:00:00 2020-01-02T00:00:00",
            b"",
        ]

        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == content_type("post")
        assert b"\n".join(expected) == resp.data

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "get",
                {
                    "net": "CH",
                    "sta": "HASLI,DAVOX",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-02",
                },
            ),
            (
                "post",
                [
                    b"CH HASLI -- LHZ 2020-01-01 2020-01-02",
                    b"CH DAVOX -- LHZ 2020-01-01 2020-01-02",
                ],
            ),
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_multi_sncl_single_dc(
        self, client, service_args, content_type, method, params_or_data
    ):

        req_kwargs = create_request_kwargs(
            method, params_or_data, **service_args
        )
        resp = getattr(client, method)(EIDAWS_ROUTING_PATH_QUERY, **req_kwargs)

        expected = [
            create_url("eida.ethz.ch", service_args),
            b"CH DAVOX -- LHZ 2020-01-01T00:00:00 2020-01-02T00:00:00",
            b"CH HASLI -- LHZ 2020-01-01T00:00:00 2020-01-02T00:00:00",
            b"",
        ]

        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == content_type("post")
        assert b"\n".join(expected) == resp.data

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "get",
                {
                    "net": "CH,GR",
                    "sta": "HASLI,WET",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-02",
                },
            ),
            (
                "post",
                [
                    b"GR WET -- LHZ 2020-01-01 2020-01-02",
                    b"CH HASLI -- LHZ 2020-01-01 2020-01-02",
                ],
            ),
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_multi_sncl_multi_dc(
        self, client, service_args, content_type, method, params_or_data
    ):

        req_kwargs = create_request_kwargs(
            method, params_or_data, **service_args
        )
        resp = getattr(client, method)(EIDAWS_ROUTING_PATH_QUERY, **req_kwargs)

        expected = [
            create_url("eida.bgr.de", service_args),
            b"GR WET -- LHZ 2020-01-01T00:00:00 2020-01-02T00:00:00",
            b"",
            create_url("eida.ethz.ch", service_args),
            b"CH HASLI -- LHZ 2020-01-01T00:00:00 2020-01-02T00:00:00",
            b"",
        ]

        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == content_type("post")
        assert b"\n".join(expected) == resp.data

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "get",
                {
                    "net": "CH",
                    "sta": "GUT,HASLI",
                    "loc": "--",
                    "cha": "HHZ",
                    "start": "2008-01-01",
                    "end": "2008-01-02",
                },
            ),
            (
                "post",
                [
                    b"CH HASLI -- HHZ 2008-01-01 2008-01-02",
                    b"CH GUT -- HHZ 2008-01-01 2008-01-02",
                ],
            ),
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_multi_sncl_single_dc_access(
        self, client, access_args, content_type, method, params_or_data
    ):
        service_arg = {"service": "dataselect"}

        def create_expected_response(access="any"):
            if access == "open":
                return [
                    create_url("eida.ethz.ch", service_arg),
                    b"CH HASLI -- HHZ 2008-01-01T00:00:00 2008-01-02T00:00:00",
                    b"",
                ]

            elif access == "closed":
                return [
                    create_url(
                        "eida.ethz.ch",
                        service_arg,
                        method_token=FDSNWS_QUERYAUTH_METHOD_TOKEN,
                    ),
                    b"CH GUT -- HHZ 2008-01-01T00:00:00 2008-01-02T00:00:00",
                    b"",
                ]

            elif access == "any":
                return [
                    create_url("eida.ethz.ch", service_arg),
                    b"CH HASLI -- HHZ 2008-01-01T00:00:00 2008-01-02T00:00:00",
                    b"",
                    create_url(
                        "eida.ethz.ch",
                        service_arg,
                        method_token=FDSNWS_QUERYAUTH_METHOD_TOKEN,
                    ),
                    b"CH GUT -- HHZ 2008-01-01T00:00:00 2008-01-02T00:00:00",
                    b"",
                ]

            raise ValueError(f"Invalid access: {access}")

        req_kwargs = create_request_kwargs(
            method, params_or_data, **access_args, **service_arg,
        )
        resp = getattr(client, method)(EIDAWS_ROUTING_PATH_QUERY, **req_kwargs)

        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == content_type("post")
        assert (
            b"\n".join(create_expected_response(access=access_args["access"]))
            == resp.data
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "get",
                {
                    "net": "FOO",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-02",
                },
            ),
            ("post", [b"FOO HASLI -- LHZ 2020-01-01 2020-01-02"]),
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_no_content(
        self, client, service_args, content_type, method, params_or_data
    ):
        req_kwargs = create_request_kwargs(
            method, params_or_data, **service_args
        )
        resp = getattr(client, method)(EIDAWS_ROUTING_PATH_QUERY, **req_kwargs)

        assert resp.status_code == 204
        assert resp.headers["Content-Type"] == content_type(204)
        assert b"" == resp.data

    @pytest.mark.parametrize(
        "service_config",
        [
            {
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{_path_module}/data/",
                "PROPAGATE_EXCEPTIONS": True,
                "TESTING": True,
            },
            {
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{_path_module}/data/test.db",
                "SQLALCHEMY_ENGINE_OPTIONS": {"pool_size": 5},
                "PROPAGATE_EXCEPTIONS": True,
                "TESTING": True,
            },
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_invalid_db_config(self, service_config, content_type):
        app = create_app(service_config)

        with app.test_client() as client:
            resp = client.get(
                EIDAWS_ROUTING_PATH_QUERY,
                query_string=(
                    "net=CH&sta=HASLI&loc=--&cha=LHZ&"
                    "start=2020-01-01&end=2020-01-02"
                ),
            )

            assert resp.status_code == 500
            assert resp.headers["Content-Type"] == content_type(500)
            assert resp.data.startswith(
                b"\nError 500: Internal server error\n"
            )
