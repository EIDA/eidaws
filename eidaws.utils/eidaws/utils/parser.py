# -*- coding: utf-8 -*-
import itertools

from collections import namedtuple

from webargs.core import ValidationError

from eidaws.utils.settings import (
    FDSNWS_QUERY_LIST_SEPARATOR_CHAR,
    FDSNWS_QUERY_VALUE_SEPARATOR_CHAR,
)


class FDSNWSParserMixin:
    """
    Mixin providing additional FDSNWS specific parsing facilities for `webargs
    https://webargs.readthedocs.io/en/latest/`_ parsers.
    """

    @staticmethod
    def _parse_streamepochs_from_argdict(arg_dict):
        """
        Parse stream epoch (i.e. :code:`network`, :code:`net`, :code:`station`,
        :code:`sta`, :code:`location`, :code:`loc`, :code:`channel:,
        :code:`cha`, :code:`starttime`, :code:`start`, :code:`endtime` and
        :code:`end`)related parameters from a dictionary like structure.

        :param dict arg_dict: Dictionary like structure to be parsed

        :returns: Dictionary with parsed stream epochs
        :rtype: dict

        Keys automatically are merged. If necessary, parameters are
        demultiplexed.
        """

        def _get_values(keys, raw=False):
            """
            Look up :code:`keys` in :code:`arg_dict`.

            :param keys: an iterable with keys to look up
            :param bool raw: return the raw value if True - else the value is
                splitted i.e. a list is returned
            """
            for key in keys:
                val = arg_dict.get(key)
                if val:
                    if not raw:
                        return val.split(FDSNWS_QUERY_LIST_SEPARATOR_CHAR)
                    return val
            return None

        # preprocess the req.args multidict regarding SNCL parameters
        networks = _get_values(("net", "network")) or ["*"]
        networks = set(networks)
        if "*" in networks:
            networks = ["*"]
        stations = _get_values(("sta", "station")) or ["*"]
        stations = set(stations)
        if "*" in stations:
            stations = ["*"]
        locations = _get_values(("loc", "location")) or ["*"]
        locations = set(locations)
        if "*" in locations:
            locations = ["*"]
        channels = _get_values(("cha", "channel")) or ["*"]
        channels = set(channels)
        if "*" in channels:
            channels = ["*"]

        stream_epochs = []
        for prod in itertools.product(networks, stations, locations, channels):
            stream_epochs.append(
                {
                    "net": prod[0],
                    "sta": prod[1],
                    "loc": prod[2],
                    "cha": prod[3],
                }
            )
        # add times
        starttime = _get_values(("start", "starttime"), raw=True)
        if starttime:
            for stream_epoch_dict in stream_epochs:
                stream_epoch_dict["start"] = starttime
        endtime = _get_values(("end", "endtime"), raw=True)
        if endtime:
            for stream_epoch_dict in stream_epochs:
                stream_epoch_dict["end"] = endtime

        return {"stream_epochs": stream_epochs}

    @staticmethod
    def _parse_postfile(postfile):
        """
        Parse a FDSNWS formatted POST request file.

        :param str postfile: Postfile content

        :returns: Dictionary with parsed parameters.
        :rtype: dict
        """
        _StreamEpoch = namedtuple(
            "_StreamEpoch", ["net", "sta", "loc", "cha", "start", "end"]
        )

        retval = {}
        stream_epochs = []
        for line in postfile.split("\n"):
            check_param = line.split(FDSNWS_QUERY_VALUE_SEPARATOR_CHAR, 1)
            if len(check_param) == 2:

                if not all(v.strip() for v in check_param):
                    raise ValidationError(f"Illegal POST line: {line!r}")

                # add query params
                retval[check_param[0].strip()] = check_param[1].strip()

            elif len(check_param) == 1:
                # ignore empty lines
                if not check_param[0].strip():
                    continue

                # parse StreamEpoch
                stream_epoch = line.split()
                if len(stream_epoch) == 6:
                    stream_epoch = _StreamEpoch(
                        net=stream_epoch[0],
                        sta=stream_epoch[1],
                        loc=stream_epoch[2],
                        cha=stream_epoch[3],
                        start=stream_epoch[4],
                        end=stream_epoch[5],
                    )
                    stream_epochs.append(stream_epoch)
                else:
                    raise ValidationError(f"Illegal POST line: {line!r}")

        # remove duplicates
        stream_epochs = list(set(stream_epochs))
        retval["stream_epochs"] = [se._asdict() for se in stream_epochs]

        return retval
