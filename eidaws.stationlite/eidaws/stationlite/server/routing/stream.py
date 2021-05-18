# -*- coding: utf-8 -*-
"""
eidaws-stationlite output format facilities.
"""

from urllib.parse import urlsplit, urlunsplit

from eidaws.utils.schema import StreamEpochSchema


class OutputStream:
    """
    Base class for the StationLite ouput stream format.

    :param list routes: List of :py:class:`eidangservices.utils.Route` objects
    :param str netloc_proxy: Network location of a proxy
    """

    def __init__(self, routes=[], **kwargs):
        self.routes = routes

    @classmethod
    def create(cls, format, **kwargs):
        if format == "post":
            return PostStream(**kwargs)
        elif format == "get":
            return GetStream(**kwargs)
        else:
            raise KeyError("Invalid output format chosen.")

    def __str__(self):
        raise NotImplementedError


class PostStream(OutputStream):
    """
    StationLite output stream for `format=post`.
    """

    SERIALIZER = StreamEpochSchema(context={"routing": True})

    @staticmethod
    def _serialize(stream_epoch):
        return " ".join(PostStream.SERIALIZER.dump(stream_epoch).values())

    def __str__(self):
        lines = []
        for url, stream_epoch_lst in self.routes:
            lines.append(url)
            lines.extend("%s" % self._serialize(se) for se in stream_epoch_lst)
            lines.append("")

        return "\n".join(lines)


class GetStream(OutputStream):
    """
    StationLite output stream for `format=post`.
    """

    SERIALIZER = StreamEpochSchema(context={"routing": True})

    @staticmethod
    def _serialize(stream_epoch):
        return "&".join(
            [
                "{}={}".format(k, v)
                for k, v in GetStream.SERIALIZER.dump(stream_epoch).items()
            ]
        )

    def __str__(self):
        lines = []
        for url, stream_epoch_lst in self.routes:
            lines.extend(
                "{}?{}\n".format(url, self._serialize(se))
                for se in stream_epoch_lst
            )

        return "".join(lines)
