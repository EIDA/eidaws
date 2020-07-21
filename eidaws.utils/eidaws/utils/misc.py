# -*- coding: utf-8 -*-
import argparse
import collections
import datetime
import logging
import os
import re

from marshmallow.utils import from_iso_date

from eidaws.utils.settings import (
    REQUEST_CONFIG_KEY,
    KEY_REQUEST_ID,
    KEY_REQUEST_STARTTIME,
)


dateutil_available = False
try:
    from dateutil import parser

    dateutil_available = True
except ImportError:
    dateutil_available = False


Route = collections.namedtuple("Route", ["url", "stream_epochs"])


# from marshmallow (originally from Django)
_iso8601_re = re.compile(
    r"(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})"
    r"[T ](?P<hour>\d{1,2}):(?P<minute>\d{1,2})"
    r"(?::(?P<second>\d{1,2})(?:\.(?P<microsecond>\d{1,6})\d{0,6})?)?"
    r"(?P<tzinfo>Z|(?![+-]\d{2}(?::?\d{2})?))?$"
)


# -----------------------------------------------------------------------------
def _callable_or_raise(obj):
    """
    Makes sure an object is callable if it is not ``None``. If not
    callable, a ``ValueError`` is raised.
    """
    if obj and not callable(obj):
        raise ValueError(f"{obj!r} is not callable.")
    else:
        return obj


def realpath(p):
    return os.path.realpath(os.path.expanduser(p))


def real_file_path(path):
    """
    Check if file exists.
    :param str path: Path to be checked
    :returns: realpath in case the file exists
    :rtype: str
    :raises argparse.ArgumentTypeError: if file does not exist
    """
    path = realpath(path)
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"Invalid file path: {path!r}")
    return path


def get_req_config(request, key):
    return request[REQUEST_CONFIG_KEY].get(key)


# -----------------------------------------------------------------------------
def from_fdsnws_datetime(datestring, use_dateutil=True):
    """
    Parse a datestring from a string specified by the FDSNWS datetime
    specification.

    :param str datestring: String to be parsed
    :param bool use_dateutil: Make use of the :code:`dateutil` package if set
        to :code:`True`
    :returns: Datetime
    :rtype: :py:class:`datetime.datetime`

    See: http://www.fdsn.org/webservices/FDSN-WS-Specifications-1.1.pdf
    """
    IGNORE_TZ = True

    if len(datestring) == 10:
        # only YYYY-mm-dd is defined
        return datetime.datetime.combine(
            from_iso_date(datestring), datetime.time()
        )
    else:
        # from marshmallow
        if not _iso8601_re.match(datestring):
            raise ValueError("Not a valid ISO8601-formatted string.")
        # Use dateutil's parser if possible
        if dateutil_available and use_dateutil:
            return parser.parse(datestring, ignoretz=IGNORE_TZ)
        else:
            # Strip off microseconds and timezone info.
            return datetime.datetime.strptime(
                datestring[:19], "%Y-%m-%dT%H:%M:%S"
            )


def fdsnws_isoformat(dt, localtime=False, *args, **kwargs):
    """
    Convert a :py:class:`datetime.datetime` object to a ISO8601 conform string.

    :param datetime.datetime dt: Datetime object to be converted
    :param bool localtime: The parameter is ignored
    :returns: ISO8601 conform datetime string
    :rtype: str
    """
    # ignores localtime parameter
    return dt.isoformat(*args, **kwargs)


def convert_sncl_dicts_to_query_params(stream_epochs_dict):
    """
    Convert a list of :py:class:`~sncl.StreamEpoch` objects to FDSNWS HTTP
    **GET** query parameters. Return values are ordered based on the order of
    the keys of the first dictionary in the :code:`stream_epochs_dict` list.

    :param list stream_epochs_dict: A list of :py:class:`~sncl.StreamEpoch`
        dictionaries

    :return: StreamEpoch related FDSNWS conform HTTP **GET** query parameters
    :rtype: :py:class:`dict`
    :raises ValueError: If temporal constraints differ between stream epochs.

    Usage:

    .. code::

        se_schema = StreamEpochSchema(many=True, context={'request': self.GET})
        retval = convert_sncl_dicts_to_query_params(
            se_schema.dump(stream_epochs))

    .. note::

        :py:class:`~sncl.StreamEpoch` objects are flattened.
    """
    _temporal_constraints_params = ("starttime", "endtime")

    retval = DefaultOrderedDict(set)
    if stream_epochs_dict:
        for stream_epoch in stream_epochs_dict:
            for key, value in stream_epoch.items():
                retval[key].update([value])

    for key, values in retval.items():
        if key in _temporal_constraints_params:
            if len(values) != 1:
                raise ValueError(
                    "StreamEpoch objects provide "
                    "multiple temporal constraints."
                )
            retval[key] = values.pop()
        else:
            retval[key] = ",".join(values)

    return retval


# -----------------------------------------------------------------------------
class DefaultOrderedDict(collections.OrderedDict):
    """
    Returns a new ordered dictionary-like object.
    :py:class:`DefaultOrderedDict` is a subclass of the built-in
    :code:`OrderedDict` class. It overrides one method and adds one writable
    instance variable. The remaining functionality is the same as for the
    :code:`OrderedDict` class and is not documented here.
    """

    # Source: http://stackoverflow.com/a/6190500/562769
    def __init__(self, default_factory=None, *args, **kwargs):
        if default_factory is not None and not callable(default_factory):
            raise TypeError("first argument must be callable")

        super().__init__(*args, **kwargs)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = (self.default_factory,)
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy

        return type(self)(self.default_factory, copy.deepcopy(self.items()))


def make_context_logger(logger, request, *args):
    ctx = [get_req_config(request, KEY_REQUEST_ID)] + list(args)
    return ContextLoggerAdapter(logger, {"ctx": ctx})


def log_access(logger, request):
    def get_req_header(key):
        return request.headers.get(key, "-")

    start_time = get_req_config(request, KEY_REQUEST_STARTTIME)
    logger = make_context_logger(logger, request)
    logger.info(
        f"{request.remote} {start_time.isoformat()} "
        f'"{request.method} {request.path_qs} '
        f"HTTP/{request.version.major}.{request.version.minor}' "
        f"{get_req_header('Referer')!r} {get_req_header('User-Agent')!r}"
    )


class ContextLoggerAdapter(logging.LoggerAdapter):
    """
    Adapter expecting the passed in dict-like object to have a 'ctx' key, whose
    value in brackets is prepended to the log message.
    """

    def process(self, msg, kwargs):
        try:
            ctx = self.extra["ctx"]
            if isinstance(ctx, (list, tuple)):
                ctx = "::".join(str(c) for c in ctx)
        except KeyError:
            return f"{msg}", kwargs
        else:
            return f"[{ctx}] {msg}", kwargs
