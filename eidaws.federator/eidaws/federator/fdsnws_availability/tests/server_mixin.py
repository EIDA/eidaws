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
