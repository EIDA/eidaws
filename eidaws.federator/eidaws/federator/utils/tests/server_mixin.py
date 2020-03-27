# -*- coding: utf-8 -*-

import pytest

from aiohttp import web

# TODO(damb): Test if routing returns 500, ClientError etc


class _TestRoutingMixin:
    """
    Routing specific tests for test classes providing both the properties
    ``FED_PATH_QUERY`` and ``PATH_QUERY`` and a ``create_app`` method.
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
        resp = await getattr(client, method)(self.FED_PATH_QUERY, **kwargs)

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
                            "http://eida.ethz.ch" + self.PATH_QUERY + "\n"
                            "CH FOO -- LHZ 2019-01-01T00:00:00 2019-01-05T00:00:00\n"
                        ),
                    ),
                )
            ]
        }

        mocked_endpoints = {
            "eida.ethz.ch": [
                (self.PATH_QUERY, "GET", web.Response(status=204,),),
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

        assert resp.status == 204

        faked_routing.assert_no_unused_routes()
        faked_endpoints.assert_no_unused_routes()
