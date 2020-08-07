# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import copy
import logging
import socket

from aiohttp import web

from eidaws.endpoint_proxy.settings import PROXY_BASE_ID
from eidaws.utils.misc import make_context_logger


class RedirectView(web.View):

    LOGGER = PROXY_BASE_ID + ".view"

    def __init__(self, request):
        super().__init__(request)
        self.config = self.request.config_dict[PROXY_BASE_ID]["config"]

        self._logger = logging.getLogger(self.LOGGER)
        self.logger = make_context_logger(self._logger, self.request)

    @property
    def client_timeout(self):
        return aiohttp.ClientTimeout(
            connect=self.config["timeout_connect"],
            sock_connect=self.config["timeout_sock_connect"],
            sock_read=self.config["timeout_sock_read"],
        )

    async def get(self):

        return await self._redirect(
            self.request,
            connector=self.request.config_dict[PROXY_BASE_ID][
                "http_conn_pool"
            ],
        )

    post = get

    async def _redirect(self, request, connector):

        if request.host in (
            socket.getfqdn(),
            f'{self.config["hostname"]}:{self.config["port"]}',
        ):
            raise web.HTTPBadRequest(
                text=(
                    "ERROR: Recursion error. "
                    "Invalid 'Host' header specified.\n"
                )
            )

        self.logger.debug(
            f"Request (redirecting): method={request.method!r} "
            f"path={request.path!r}, query_string={request.query_string!r}, "
            f"headers={request.headers!r}, remote={request.remote!r}"
        )

        req_headers = request.headers
        body = await request.read()

        # modify headers
        if self.config["num_forwarded"]:
            req_headers = copy.deepcopy(dict(req_headers))
            req_headers["X-Forwarded-For"] = request.remote

            self.logger.debug(
                f"Request headers (redirecting, modified): {req_headers!r}"
            )

        try:
            async with aiohttp.ClientSession(
                headers=req_headers,
                connector=connector,
                timeout=self.client_timeout,
                connector_owner=False,
                auto_decompress=False,
            ) as session:
                async with session.request(
                    request.method, request.url, data=body,
                ) as resp:

                    # XXX(damb): Workaround since aiohttp seems to always set
                    # the Transfer-Encoding header field which violates RFC7329
                    # see also:
                    # https://tools.ietf.org/html/rfc7230#section-3.3.1
                    if (
                        resp.status == 204
                        or (resp.status >= 100 and resp.status <= 199)
                        or request.method == "CONNECT"
                    ):
                        return web.Response(
                            headers=resp.headers, status=resp.status
                        )

                    proxied_response = web.StreamResponse(
                        headers=resp.headers, status=resp.status
                    )
                    if (
                        resp.headers.get("Transfer-Encoding", "").lower()
                        == "chunked"
                    ):
                        proxied_response.enable_chunked_encoding()

                    await proxied_response.prepare(request)

                    async for data in resp.content.iter_any():
                        await proxied_response.write(data)

                    await proxied_response.write_eof()
                    return proxied_response

        except ConnectionResetError as err:
            self.logger.warning(f"Connection reset by peer: {err}")
            # TODO(damb): Would implementing a retry mechanism be an
            # alternative?
            raise web.HTTPNoContent()
        except asyncio.TimeoutError as err:
            self.logger.warning(
                f"Error while executing request: error={type(err)}, "
                f"url={request.url!r}, method={request.method!r}"
            )
            raise web.HTTPGatewayTimeout(text=f"ERROR: {str(type(err))}\n")

        except aiohttp.ClientError as err:
            msg = (
                f"Error while executing request: error={type(err)}, "
                f"url={request.url!r}, method={request.method!r}"
            )
            if isinstance(err, aiohttp.ClientOSError):
                msg += f", errno={err.errno}"

            self.logger.warning(msg)
            raise web.HTTPServiceUnavailable(text=f"ERROR: {str(type(err))}\n")
