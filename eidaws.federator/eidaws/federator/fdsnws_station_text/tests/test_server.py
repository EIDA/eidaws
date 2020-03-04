# -*- coding: utf-8 -*-

import copy
import pytest

from aiohttp import web

from eidaws.federator.fdsnws_station_text import create_app, SERVICE_ID
from eidaws.federator.fdsnws_station_text.app import DEFAULT_CONFIG
from eidaws.federator.settings import FED_STATION_PATH_TEXT
from eidaws.federator.utils.misc import get_config
from eidaws.utils.settings import FDSNWS_QUERY_METHOD_TOKEN


class TestFDSNStationTextServer:

    PATH_QUERY = "/".join([FED_STATION_PATH_TEXT, FDSNWS_QUERY_METHOD_TOKEN])

    @staticmethod
    def create_server(*args, **kwargs):
        return create_app(*args, **kwargs)

    async def test_client_max_size(self, aiohttp_client):

        client_max_size = 32

        # reduce client_max_size
        config_dict = copy.deepcopy(DEFAULT_CONFIG)
        config_dict['client_max_size'] = client_max_size

        app = self.create_server(
            config_dict=get_config(SERVICE_ID, defaults=config_dict)
        )

        client = await aiohttp_client(app)

        data = (b'level=channel\n'
                b'\n'
                b'CH * * * 2020-01-01 2020-01-02')

        assert client_max_size < len(data)

        resp = await client.post(self.PATH_QUERY, data=data)

        assert resp.status == 413
        assert 'Request Entity Too Large' in await resp.text()
