# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import logging
import socket

from aiohttp import web

from eidaws.endpoint_proxy.settings import PROXY_BASE_ID
from eidaws.endpoint_proxy.utils import make_context_logger


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
            connect=self.config["endpoint_timeout_connect"],
            sock_connect=self.config["endpoint_timeout_sock_connect"],
            sock_read=self.config["endpoint_timeout_sock_read"],
        )

    async def get(self):

        return await self._redirect(
            self.request,
            connector=self.request.config_dict[PROXY_BASE_ID][
                "endpoint_http_conn_pool"
            ],
        )

    post = get

    async def _redirect(self, request, connector):

        headers = request.headers
        body = await request.read()

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

        self.logger.info(
            f"Proxying request (host={request.host!r}, "
            f"path={request.path!r}, query={request.query_string!r}) ..."
        )
        # XXX(damb): Modify request headers if required
        try:
            async with aiohttp.ClientSession(
                headers=headers,
                connector=connector,
                timeout=self.client_timeout,
                connector_owner=False,
                auto_decompress=False,
            ) as session:
                async with session.request(
                    request.method, request.url, data=body,
                ) as resp:
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
            self.logger.debug(f"Connection reset by peer: {err}")
        except asyncio.TimeoutError as err:
            self.logger.warning(
                f"Error while executing request: error={type(err)}, "
                f"url={request.url!r}, method={request.method!r}"
            )
            raise web.HTTPGatewayTimeout(text=f"ERROR: {str(type(err))}\n")

        except aiohttp.ClientError as err:
            self.logger.warning(
                f"Error while executing request: error={type(err)}, "
                f"url={request.url!r}, method={request.method!r}"
            )
            raise web.HTTPServiceUnavailable(text=f"ERROR: {str(type(err))}\n")
