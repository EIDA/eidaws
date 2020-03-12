import aiohttp
import asyncio
import copy
import timeit


async def make_request(session, url, params):
    async with session.get(url, params=params) as resp:
        # print(resp.status)
        # print(resp.request_info)
        return await resp.text()


async def test_eida_dc(url, params, channels):
    async def fetch(session, url, params, channels):
        _params = copy.deepcopy(params)
        if isinstance(channels, list):
            _params["cha"] = ",".join(channels)
        elif isinstance(channels, str):
            _params["cha"] = channels
        else:
            ValueError(f"Invalid value for channels: {channels!r}")

        _ = await make_request(session, url, _params)

    print(url)
    conn = aiohttp.TCPConnector(limit=120, limit_per_host=10)
    async with aiohttp.ClientSession(connector=conn) as session:

        start = timeit.default_timer()
        _ = await fetch(session, url, params, channels)
        elapsed = timeit.default_timer() - start
        print(elapsed)

    conn = aiohttp.TCPConnector(limit=120, limit_per_host=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        start = timeit.default_timer()
        tasks = []
        for cha in channels:
            t = fetch(session, url, params, cha)
            tasks.append(t)

        await asyncio.gather(*tasks)
        elapsed = timeit.default_timer() - start
        print(elapsed)


async def main():
    channels = [
        "BHZ",
        "BHN",
        "BHE",
        "HHZ",
        "HHN",
        "HHE",
        "LHZ",
        "LHN",
        "LHE",
    ]

    url = "http://eida.ethz.ch/fdsnws/station/1/query"
    params = {
        "net": "CH",
        "sta": "DAVOX",
        "loc": "--",
        "level": "response",
        "start": "1999-01-19T00:00:00",
    }

    await test_eida_dc(url, params, channels)

    url = "http://www.orfeus-eu.org/fdsnws/station/1/query"
    params = {
        "net": "NL",
        "sta": "DBN",
        "loc": "01",
        "level": "response",
        "start": "1999-01-19T00:00:00",
    }

    await test_eida_dc(url, params, channels)

    url = "http://geofon.gfz-potsdam.de/fdsnws/station/1/query"
    params = {
        "net": "GE",
        "sta": "STU",
        "loc": "--",
        "level": "response",
        "start": "1999-01-19T00:00:00",
    }
    await test_eida_dc(url, params, channels)

    url = "http://eida.bgr.de/fdsnws/station/1/query"
    params = {
        "net": "GR",
        "sta": "BFO",
        "loc": "--",
        "level": "response",
        "start": "1999-01-19T00:00:00",
    }
    await test_eida_dc(url, params, channels)

    url = "http://webservices.ingv.it/fdsnws/station/1/query"
    params = {
        "net": "SI",
        "sta": "BOSI",
        "loc": "--",
        "level": "response",
        "start": "1999-01-19T00:00:00",
    }
    await test_eida_dc(url, params, channels)




if __name__ == "__main__":
    asyncio.run(main(), debug=True)
