# -*- coding: utf-8 -*-
"""
SNCL related facilities.
"""

import contextlib
import datetime
import functools

from collections import namedtuple, OrderedDict

from intervaltree import IntervalTree

from eidaws.utils.misc import from_fdsnws_datetime
from eidaws.utils.settings import (
    FDSNWS_QUERY_WILDCARD_SINGLE_CHAR,
    FDSNWS_QUERY_WILDCARD_MULT_CHAR,
)


Epochs = IntervalTree

# ----------------------------------------------------------------------------
@contextlib.contextmanager
def none_as_max(endtime):
    """
    Contextmanager for :code:`endtime`. Uses :py:obj:`datetime.datetime.max`
    instead of :code:`None`.

    :param endtime: Endtime to be processed
    :type endtime: :py:class:`datetime.datetime` or None
    """
    # convert endtime to datetime.datetime.max if None
    end = endtime
    if end is None:
        end = datetime.datetime.max
    yield end


@contextlib.contextmanager
def none_as_now(endtime, now=datetime.datetime.utcnow()):
    """
    Contextmanager for :code:`endtime`. Uses :code:`now` instead of
    :code:`None`.

    :param endtime: Endtime to be processed
    :type endtime: :py:class:`datetime.datetime` or None
    :param datetime.datetime now: Value to be used for :code:`now`
    """
    # convert endtime to now if None
    end = endtime
    if end is None:
        end = now
    yield end


@contextlib.contextmanager
def max_as_none(endtime):
    """
    Contextmanager for :code:`endtime`. Converts
    :py:obj:`datetime.datetime.max` back to :code:`None`.

    :param datetime.datetime endtime: Endtime to be processed
    """
    end = endtime
    if end == datetime.datetime.max:
        end = None
    yield end


@contextlib.contextmanager
def max_as_empty(endtime):
    """
    Contextmanager for :code:`endtime`. Converts
    :py:obj:`datetime.datetime.max` back to the empty string.

    :param datetime.datetime endtime: Endtime to be processed
    """
    end = endtime
    if end == datetime.datetime.max:
        end = ""
    yield end


def fdsnws_to_sql_wildcards(
    str_old, like_multiple="%", like_single="_", like_escape="/"  # noqa
):
    """
    Replace the FDSNWS wildcard characters in :code:`str_old` with the
    corresponding SQL LIKE statement character.

    :param str str_old: String to be processed
    :param str like_multiple: Character replacing the FDSNWS :code:`*` wildcard
        character
    :param str like_single: Character replacing the FDSNWS :code:`?` wildcard
        character
    :param str like_escape: Character used in the `SQL ESCAPE` clause
    :returns: Converted string
    :rtype: str
    """
    # NOTE(damb): first escape the *like_single* character, then replace '*'
    return (
        str_old.replace(like_single, like_escape + like_single)
        .replace(FDSNWS_QUERY_WILDCARD_SINGLE_CHAR, like_single)
        .replace(FDSNWS_QUERY_WILDCARD_MULT_CHAR, like_multiple)
    )


def generate_stream_epochs(stream_epochs_handler, merge_epochs=True):
    for stream_id, epochs in stream_epochs_handler.d.items():
        yield StreamEpochs.from_stream(
            Stream(**StreamEpochsHandler._stream_id_to_dict(stream_id)),
            epochs,
            merge_epochs,
        )


# ----------------------------------------------------------------------------
@functools.total_ordering
class Stream(
    namedtuple("Stream", ["network", "station", "location", "channel"])
):
    """
    This class represents a stream coming along with the properties:
        - network
        - station
        - location
        - channel

    .. note::

        For the sake of simplicity a Stream object is also named SNCL referring
        to the Stream object's properties.
    """

    __slots__ = ()

    FIELDS_SHORT = ("net", "sta", "loc", "cha")

    def id(self, sep="."):
        """
        Returns the :py:class:`Stream`'s identifier.

        :param str sep: Separator to be used
        """
        # TODO(damb): configure separator globally (i.e. in settings module)
        return sep.join(
            [self.network, self.station, self.location, self.channel]
        )

    def __new__(cls, network="*", station="*", location="*", channel="*"):
        return super().__new__(
            cls,
            network=network,
            station=station,
            location=location,
            channel=channel,
        )

    @classmethod
    def from_route_attrs(cls, **kwargs):
        """
        Creates a :py:class:`Stream` from attributes found at *eidaws-routing*
        :code:`localconfig` configuration files.
        """
        return super().__new__(
            cls,
            network=kwargs.get("networkCode", "*"),
            station=kwargs.get("stationCode", "*"),
            location=kwargs.get("locationCode", "*"),
            channel=kwargs.get("streamCode", "*"),
        )

    def _asdict(self, short_keys=False):
        """
        Return a new OrderedDict which maps field names to their values.

        :param bool short_keys: Use short keys instead of the long version e.g.
            uses :code:`net` instead of :code:`network`
        """
        fields = self.FIELDS_SHORT if short_keys else self._fields
        return OrderedDict(zip(fields, self))

    def _as_query_string(self, short_keys=True):
        attrs = self._asdict(short_keys=short_keys)
        return "&".join(f"{k}={v}" for k, v in attrs.items())

    def __eq__(self, other):
        return self.id() == other.id()

    def __lt__(self, other):
        return self.id() < other.id()

    def __hash__(self):
        return hash(self.id())

    def __repr__(self):
        return "<Stream(net=%r, sta=%r, loc=%r, cha=%r)>" % (
            self.network,
            self.station,
            self.location,
            self.channel,
        )

    def __str__(self):
        return " ".join(
            [self.network, self.station, self.location, self.channel]
        )


@functools.total_ordering
class StreamEpoch(
    namedtuple("StreamEpoch", ["stream", "starttime", "endtime"])
):
    """
    This class represents a stream epoch i.e. a :py:class:`Stream` object +
    epoch (:code:`starttime` + :code:`endtime`).
    """

    __slots__ = ()

    FIELDS_SHORT = ("stream", "start", "end")

    def __new__(cls, stream, starttime=None, endtime=None):
        return super().__new__(
            cls, stream=stream, starttime=starttime, endtime=endtime
        )

    @classmethod
    def from_sncl(cls, **kwargs):
        """
        Creates a :py:class:StreamEpoch` from SNCL-like attributes.
        """
        net = kwargs.get("network") or kwargs.get("net", "*")
        sta = kwargs.get("station") or kwargs.get("sta", "*")
        cha = kwargs.get("channel") or kwargs.get("cha", "*")
        # location / loc might be equal to an empty string
        loc = (
            kwargs.get("location")
            if kwargs.get("location") is not None
            else kwargs.get("loc", "*")
        )
        start = kwargs.get("starttime") or kwargs.get("start", None)
        end = kwargs.get("endtime") or kwargs.get("end", None)

        return cls(
            stream=Stream(network=net, station=sta, location=loc, channel=cha),
            starttime=start,
            endtime=end,
        )

    @classmethod
    def from_snclline(cls, line, default_endtime=None):
        """
        Create a StreamEpoch from a SNCL line (FDSN POST format).

        :param str line: SNCL line in FDSN POST format i.e.
            :code:`NET STA LOC CHA START END`
        :param default_endtime: Substitute an empty endtime with the datetime
            passed. If :code:`None` no substitution is performed.
        :type default_endtime: :py:class:`datetime.datetime` or None
        """
        if isinstance(line, bytes):
            line = line.decode("utf-8")

        args = line.strip().split(" ")
        end = None
        if len(args) == 6:
            end = from_fdsnws_datetime(args[5])
        elif len(args) == 5:
            end = default_endtime

        return cls(
            stream=Stream(
                network=args[0],
                station=args[1],
                location=args[2],
                channel=args[3],
            ),
            starttime=from_fdsnws_datetime(args[4]),
            endtime=end,
        )

    @classmethod
    def from_orm(cls, stream_epoch):
        """
        Creates a :py:class:`StreamEpoch` from a corresponding ORM object.
        """
        return cls(
            stream=Stream(
                network=stream_epoch.network.name,
                station=stream_epoch.station.name,
                location=stream_epoch.location,
                channel=stream_epoch.channel,
            ),
            starttime=stream_epoch.starttime,
            endtime=stream_epoch.endtime,
        )

    @classmethod
    def from_streamepochs(cls, stream_epochs):
        """
        Creates a :py:class:`StreamEpoch` from :py:class:`StreamEpochs`.
        """
        return cls(
            stream=Stream(
                network=stream_epochs.network,
                station=stream_epochs.station,
                location=stream_epochs.location,
                channel=stream_epochs.channel,
            ),
            starttime=stream_epochs.starttime,
            endtime=stream_epochs.endtime,
        )

    @property
    def epochs(self):
        """
        Return the epoch of the :py:class:`StreamEpoch`.
        """
        return Epochs.from_tuples([(self.starttime, self.endtime)])

    def id(self, sep="."):
        """
        Returns the :py:class:`StreamEpoch`'s identifier.

        :param str sep: Separator to be used
        """
        return self.stream.id(sep=sep)

    def fdsnws_to_sql_wildcards(
        self, like_multiple="%", like_single="_", like_escape="/"
    ):
        """
        Replace the FDSNWS wildcard characters in *network*, *station*,
        *location* and *channel* with the corresponding SQL LIKE statement
        character. Since StreamEpoch is immutable a new StreamEpoch instance is
        returned.

        :param str like_multiple: Character replacing the FDSNWS :code:`*`
            wildcard character
        :param str like_single: Character replacing the FDSNWS :code:`?`
            wildcard character
        :param str like_escape: Character used in the `SQL ESCAPE` clause
        :returns: A new :py:class:`StreamEpoch` object
        :rtype: :py:class:`StreamEpoch`
        """
        net = fdsnws_to_sql_wildcards(
            self.network, like_multiple, like_single, like_escape
        )
        sta = fdsnws_to_sql_wildcards(
            self.station, like_multiple, like_single, like_escape
        )
        loc = fdsnws_to_sql_wildcards(
            self.location, like_multiple, like_single, like_escape
        )
        cha = fdsnws_to_sql_wildcards(
            self.channel, like_multiple, like_single, like_escape
        )
        stream = self.stream._replace(
            network=net, station=sta, location=loc, channel=cha
        )
        return self._replace(stream=stream)

    def slice(self, num=2, default_endtime=datetime.datetime.utcnow()):
        """
        Split :py:class:`StreamEpoch` into :code:`num` equally sized chunks.

        :param int num: Number of resulting :py:class:`StreamEpoch` objects
        :param  default_endtime: Default endtime to use in case
            :code:`self.endtime == None`
        :type default_endtime: :py:class:`datetime.datetime`
        :returns: List of :py:class:`StreamEpoch` objects
        """
        if num < 2:
            return [self]
        end = self.endtime or default_endtime
        t = Epochs.from_tuples([(self.starttime, end)])

        for n in range(1, num):
            t.slice(
                self.starttime
                + datetime.timedelta(
                    seconds=((end - self.starttime).total_seconds() / num * n)
                )
            )

        return [
            type(self)(stream=self.stream, starttime=i.begin, endtime=i.end)
            for i in t
        ]

    def _asdict(self, short_keys=False):
        """
        Return a new :py:class:`OrderedDict` which maps field names to their
        values.
        """
        retval = self.stream._asdict(short_keys=short_keys)
        fields = self.FIELDS_SHORT if short_keys else self._fields
        retval.update(zip(fields[1:], (self.starttime, self.endtime)))
        return retval

    @property
    def network(self):
        return self.stream.network

    @property
    def station(self):
        return self.stream.station

    @property
    def location(self):
        return self.stream.location

    @property
    def channel(self):
        return self.stream.channel

    @property
    def duration(self):
        with none_as_max(self.endtime) as end:
            return abs(end - self.starttime)

    def __eq__(self, other):
        """
        Allows comparing :py:class:`StreamEpoch` objects.
        """
        return (
            self.stream == other.stream
            and self.starttime == other.starttime
            and self.endtime == other.endtime
        )

    def __lt__(self, other):
        if self.stream == other.stream:
            if self.starttime == other.starttime:
                if None not in (self.endtime, other.endtime):
                    return self.endtime < other.endtime
                elif self.endtime is None:
                    return False
                return True
            return self.starttime < other.starttime
        return self.stream < other.stream

    def __hash__(self):
        return hash((self.stream, self.starttime, self.endtime))

    def __repr__(self):
        return "<StreamEpoch(stream=%r, start=%r, end=%r)>" % (
            self.stream,
            self.starttime,
            self.endtime,
        )

    def __str__(self):
        from eidaws.utils.schema import StreamEpochSchema

        se_schema = StreamEpochSchema()
        stream_epoch = se_schema.dump(self)
        return " ".join(str(v) for v in stream_epoch.values())


@functools.total_ordering
class StreamEpochs:
    """
    This class represents a mapping of a :py:class:`Stream` object to multiple
    epochs. In an abstract sense it is a container for :py:class:`StreamEpoch`
    objects.

    Uses `IntervalTree <https://github.com/chaimleib/intervaltree>`_.
    """

    def __init__(
        self,
        network="*",
        station="*",
        location="*",
        channel="*",
        epochs=[],
        merge_epochs=True,
    ):
        """
        :param str network: Network code
        :param str station: Station code
        :param str location: Location code
        :param str channel: Channel code
        :param list epochs: Epochs is a list of (t1, t2) tuples, with t1 and t2
            of type datetime.datetime. It can contain overlaps.
        :param bool merge_epochs: If `True` the intervals are merged within the
        constructor.
        """

        self._stream = Stream(
            network=network,
            station=station,
            location=location,
            channel=channel,
        )

        try:
            self.epochs = Epochs.from_tuples(epochs)
        except TypeError:
            self.epochs = Epochs()

        if merge_epochs:
            # intervals are merged even if they are only end-to-end adjacent
            self.epochs.merge_overlaps(strict=False)

    @classmethod
    def from_stream_epoch(cls, stream_epoch):
        """
        Creates a :py:class:`StreamEpochs` object from :py:class:`StreamEpoch`.
        """
        return cls(
            network=stream_epoch.network,
            station=stream_epoch.station,
            location=stream_epoch.location,
            channel=stream_epoch.channel,
            epochs=[(stream_epoch.starttime, stream_epoch.endtime)],
        )

    @classmethod
    def from_stream(cls, stream, epochs=[], merge_epochs=True):
        """
        Creates a :py:class:`StreamEpochs` object from :py:class:`Stream` and a
        list of :code:`epochs`.
        """
        return cls(
            network=stream.network,
            station=stream.station,
            location=stream.location,
            channel=stream.channel,
            epochs=epochs,
            merge_epochs=merge_epochs,
        )

    def id(self, sep="."):
        """
        Returns the :py:class:`StreamEpoch`'s identifier.

        :param str sep: Separator to be used
        """
        return self._stream.id(sep)

    def merge(self, epochs=[]):
        """
        Merge an epoch list into an existing :py:class:`StreamEpochs` object.

        :param list epochs: List of (t1, t2) tuples
        """

        for iv in epochs:
            self.epochs.addi(iv[0], iv[1])

        # intervals are merged even if they are only end-to-end adjacent
        self.epochs.merge_overlaps(strict=False)

    def fdsnws_to_sql_wildcards(
        self, like_multiple="%", like_single="_", like_escape="/"
    ):
        """
        Replace the FDSNWS wildcard characters in *network*, *station*,
        *location* and *channel* with the corresponding SQL LIKE statement
        character. Replacement is done inplace.

        :param str like_multiple: Character replacing the FDSNWS * wildcard
            character
        :param str like_single: Character replacing the FDSNWS _ wildcard
            character
        :param str like_escape: Character used in the SQL ESCAPE clause.
        """
        net = fdsnws_to_sql_wildcards(
            self.network, like_multiple, like_single, like_escape
        )
        sta = fdsnws_to_sql_wildcards(
            self.station, like_multiple, like_single, like_escape
        )
        loc = fdsnws_to_sql_wildcards(
            self.location, like_multiple, like_single, like_escape
        )
        cha = fdsnws_to_sql_wildcards(
            self.channel, like_multiple, like_single, like_escape
        )

        self._stream = self._stream._replace(
            network=net, station=sta, location=loc, channel=cha
        )

    def modify_with_temporal_constraints(self, start=None, end=None):
        """
        Modify epochs by performing a real intersection.

        Using the :code:`start` and :code:`end` parameter the method also
        provides truncation facilities.

        :param start: Truncate epochs beginning from :code:`start`
        :type start: :py:class:`datetime.datetime` or None
        :param end: Truncate epochs terminating with :code:`end`
        :type end: :py:class:`datetime.datetime` or None
        """
        # perform a real intersection i.e.
        # ------..----..--------
        #           +
        #    ---------------
        #           =
        #    ---..----..----
        _start = start
        _end = end

        if _start is None:
            _start = self.epochs.begin()
        if _end is None:
            _end = self.epochs.end()

        # slice at new boundaries
        self.epochs.slice(_start)
        self.epochs.slice(_end)
        # search and assign the overlap
        self.epochs = Epochs(sorted(self.epochs.overlap(_start, _end)))

    @property
    def network(self):
        return self._stream.network

    @property
    def station(self):
        return self._stream.station

    @property
    def location(self):
        return self._stream.location

    @property
    def channel(self):
        return self._stream.channel

    @property
    def starttime(self):
        if not self.epochs.begin():
            return None
        return self.epochs.begin()

    @property
    def endtime(self):
        if not self.epochs.end():
            return None
        return self.epochs.end()

    @property
    def stream(self):
        """
        Expose stream to provide an interface equal to StreamEpoch.
        """
        return self._stream

    @property
    def duration(self):
        with none_as_max(self.endtime) as end:
            return abs(end - self.starttime)

    def __iter__(self):
        """
        Iterator protocol by means of a generator.

        The generator emerges :py:class:`StreamEpoch` objects.
        """
        for epoch in self.epochs:
            yield StreamEpoch.from_sncl(
                network=self.network,
                station=self.station,
                location=self.location,
                channel=self.channel,
                starttime=epoch.begin,
                endtime=epoch.end,
            )

    def __eq__(self, other):
        """
        Allows comparing :py:class:`StreamEpochs` objects.
        """
        return self._stream == other._stream and self.epochs == other.epochs

    def __lt__(self, other):
        if self._stream == other._stream:
            if self.starttime == other.starttime:
                return self.epochs.end() < other.epochs.end()
            return self.epochs.begin() < other.epochs.begin()
        return self._stream < other._stream

    def __repr__(self):
        return "<StreamEpochs(stream=%r, start=%r, end=%r)>" % (
            self._stream,
            self.starttime,
            self.endtime,
        )

    def __str__(self):
        from eidaws.utils.schema import StreamEpochSchema

        se_schema = StreamEpochSchema(many=True)
        stream_epochs = se_schema.dump(list(self))
        return "\n".join(
            [" ".join(stream_epoch.values()) for stream_epoch in stream_epochs]
        )


class StreamEpochsHandler:
    """
    This class is intended to represent a handler/container for
    :py:class:`StreamEpochs` objects.
    """

    def __init__(self, stream_epochs=[]):
        self.d = {}

        if stream_epochs:
            self.merge(stream_epochs)

    def modify_with_temporal_constraints(
        self, start=None, end=None, merge_epochs=True
    ):
        """
        Modify epochs by performing a real intersection.

        Using the :code:`start` and :code:`end` parameter the method also
        provides truncation facilities.

        :param start: Truncate epochs beginning from :code:`start`
        :type start: :py:class:`datetime.datetime` or None
        :param end: Truncate epochs terminating with :code:`end`
        :type end: :py:class:`datetime.datetime` or None
        """
        # perform a real intersection i.e.
        # ------..----..--------
        #           +
        #    ---------------
        #           =
        #    ---..----..----
        for stream_id, epochs in self.d.items():
            se = StreamEpochs.from_stream(
                Stream(**self._stream_id_to_dict(stream_id)),
                epochs=epochs,
                merge_epochs=merge_epochs,
            )
            se.modify_with_temporal_constraints(start, end)

            self.d[se.id()] = se.epochs

    def canonicalize_epochs(
        self,
        start=None,
        end=None,
        canonicalization_offset=datetime.timedelta(microseconds=1),
    ):

        for stream_id, epochs in self.d.items():
            canonicalized = []
            for epoch in epochs:
                _start = epoch.begin
                _end = epoch.end
                if start is None or start != _start:
                    _start += canonicalization_offset
                if _end not in (None, datetime.datetime.max) and (
                    end is None or end != _end
                ):
                    _end -= canonicalization_offset

                if _start == _end:
                    # skip if original epoch was too short
                    continue

                canonicalized.append((_start, _end))

            self.d[stream_id] = Epochs.from_tuples(canonicalized)

    def add(self, other):
        """
        Add ``other`` to :py:class:`StreamEpochsHandler` without merging
        overlaps.

        :param other: Object to be added.
        :type other: :py:class:`StreamEpochs` or :py:class:`StreamEpoch`
        """
        try:
            # merge epoch interval trees (union)
            self.d[other.id()] |= other.epochs
        except KeyError:
            self.d[other.id()] = other.epochs

    def merge(self, others):
        """
        Merge a list of ``others``.

        :param list others: List of :py:class:`StreamEpoch` and
            :py:class:`StreamEpochs` objects
        """
        for other in others:
            self.add(other)

            # tree for key may be overlapping; intervals are merged even if
            # they are only end-to-end adjacent
            self.d[other.id()].merge_overlaps(strict=False)

    @property
    def streams(self):
        return list(self)

    def __iter__(self):
        """
        Iterator protocol by means of a generator.

        The iterator emerges objects of type :py:class:`StreamEpochs`.
        """
        for stream_id, epochs in self.d.items():
            yield StreamEpochs.from_stream(
                Stream(**self._stream_id_to_dict(stream_id)),
                epochs=epochs,
            )

    def __repr__(self):
        return "<StreamEpochsHandler(streams=%r)>" % list(self)

    def __str__(self):
        return "\n".join(str(stream_epochs) for stream_epochs in self)

    @staticmethod
    def _stream_id_to_dict(stream_id, sep="."):
        net, sta, loc, cha = stream_id.split(sep)
        return dict(network=net, station=sta, location=loc, channel=cha)
