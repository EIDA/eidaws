# -*- coding: utf-8 -*-

import pytest


class _TestCommonStationMixin:
    """
    Common tests for `fdsnws_station` test classes providing both the
    properties ``FED_PATH_RESOURCE`` and ``PATH_RESOURCE`` and a ``create_app``
    method.
    """

    async def _test_invalid(
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

        print(await resp.text())
        assert resp.status == 400
        assert f"Bad request" in await resp.text()
        assert (
            "Content-Type" in resp.headers
            and resp.headers["Content-Type"] == fdsnws_error_content_type
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {"minlat": "90"},
            ),
            (
                "POST",
                b"minlat=90\nCH HASLI -- LHZ 2019-01-01 2019-01-05",
            ),
            (
                "GET",
                {"minlatitude": "90"},
            ),
            (
                "POST",
                b"minlatitude=90\nCH HASLI -- LHZ 2019-01-01 2019-01-05",
            ),
            (
                "GET",
                {"maxlat": "-90"},
            ),
            (
                "POST",
                b"maxlat=-90\nCH HASLI -- LHZ 2019-01-01 2019-01-05",
            ),
            (
                "GET",
                {"maxlatitude": "-90"},
            ),
            (
                "POST",
                b"maxlatitude=-90\nCH HASLI -- LHZ 2019-01-01 2019-01-05",
            ),
            (
                "GET",
                {"minlat": "1", "maxlat": "0"},
            ),
            (
                "POST",
                b"minlat=1\nmaxlat=0\nCH HASLI -- LHZ 2019-01-01 2019-01-05",
            ),
            ("GET", {"minradius": "180.0"}),
            (
                "POST",
                b"minradius=180.0\nCH HASLI -- LHZ 2019-01-01 2019-01-05",
            ),
            ("GET", {"maxradius": "0"}),
            (
                "POST",
                b"maxradius=0\nCH HASLI -- LHZ 2019-01-01 2019-01-05",
            ),
        ],
    )
    async def test_invalid_spatial_args(
        self,
        make_federated_eida,
        fdsnws_error_content_type,
        method,
        params_or_data,
    ):
        await self._test_invalid(
            make_federated_eida,
            fdsnws_error_content_type,
            method,
            params_or_data,
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "GET",
                {"startafter": "2020-01-01", "endbefore": "2019-01-01"},
            ),
            (
                "POST",
                b"startafter=2020-01-01\nendbefore=2019-01-01\nCH HASLI -- LHZ 2019-01-01 2019-01-05",
            ),
            (
                "GET",
                {"startafter": "2020-01-01", "endbefore": "2020-01-01"},
            ),
            (
                "POST",
                b"startafter=2020-01-01\nendbefore=2020-01-01\nCH HASLI -- LHZ 2019-01-01 2019-01-05",
            ),
        ],
    )
    async def test_invalid_time_constraints(
        self,
        make_federated_eida,
        fdsnws_error_content_type,
        method,
        params_or_data,
    ):
        await self._test_invalid(
            make_federated_eida,
            fdsnws_error_content_type,
            method,
            params_or_data,
        )
