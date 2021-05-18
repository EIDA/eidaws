# -*- coding: utf-8 -*-

import enum
import json

from collections import namedtuple

from eidaws.utils.sncl import max_as_empty, Epochs


class _Enum(enum.Enum):
    def __str__(self):
        return self.name.lower()

    @classmethod
    def from_str(cls, s):
        return cls[s.upper()]


class Epoch(_Enum):
    NETWORK = 1
    STATION = 2
    CHANNEL = 3


class RestrictedStatus(_Enum):
    OPEN = 1
    CLOSED = 2
    PARTIAL = 3


class ChannelEpoch(
    namedtuple(
        "ChannelEpoch",
        [
            "network",
            "station",
            "location",
            "channel",
            "starttime",
            "endtime",
            "restrictedStatus",
        ],
    )
):
    @property
    def epochs(self):
        return Epochs.from_tuples([(self.starttime, self.endtime)])

    def jsonify(self):
        retval = self._asdict()
        retval["starttime"] = retval["starttime"].isoformat()
        retval["restrictedStatus"] = str(retval["restrictedStatus"])
        with max_as_empty(self.endtime) as end:
            retval["endtime"] = end
            if retval["endtime"]:
                retval["endtime"] = retval["endtime"].isoformat()

        return json.dumps(retval)


class ChannelEpochsHandler:
    def __init__(self):
        self.d = {}

    def add(self, other):
        self._add(other)

    def merge(self, other, merge_epochs=True):
        """
        Merge ``other`` based on its ``restrictedStatus`` attribute property.
        """
        key = self._create_key_from_cha_epoch(other)
        if key in self.d:
            self._add(other, key=key)
        else:
            key_partial = self._create_key(
                other.network,
                other.station,
                other.location,
                other.channel,
                RestrictedStatus.PARTIAL,
            )
            if key_partial in self.d:
                self._add(other, key=key_partial)
                key = key_partial
            else:
                restricted_status = other.restrictedStatus
                if restricted_status == RestrictedStatus.OPEN:
                    key_closed = self._create_key(
                        other.network,
                        other.station,
                        other.location,
                        other.channel,
                        RestrictedStatus.CLOSED,
                    )
                    if key_closed in self.d:
                        self.d[key_partial] = self.d[key_closed]
                        del self.d[key_closed]
                        self._add(other, key=key_closed)
                        key = key_partial
                    else:
                        self._add(other, key=key)
                elif restricted_status == RestrictedStatus.CLOSED:
                    key_open = self._create_key(
                        other.network,
                        other.station,
                        other.location,
                        other.channel,
                        RestrictedStatus.OPEN,
                    )
                    if key_open in self.d:
                        self.d[key_partial] = self.d[key_open]
                        del self.d[key_open]
                        self._add(other, key=key_partial)
                        key = key_partial
                    else:
                        self._add(other, key=key)

        if merge_epochs:
            # tree for key may be overlapping; intervals are merged even if
            # they are only end-to-end adjacent
            self.d[key].merge_overlaps(strict=False)

    def _add(self, other, key=None):
        if key is None:
            key = self._create_key_from_cha_epoch(other)

        try:
            # merge epoch interval trees (union)
            self.d[key] |= other.epochs
        except KeyError:
            self.d[key] = other.epochs

    def __iter__(self):
        for key, epochs in self.d.items():
            net, sta, loc, cha, status = key
            for epoch in epochs:
                yield ChannelEpoch(
                    network=net,
                    station=sta,
                    location=loc,
                    channel=cha,
                    restrictedStatus=status,
                    starttime=epoch.begin,
                    endtime=epoch.end,
                )

    def __len__(self):
        return len(self.d)

    @staticmethod
    def _create_key(*args):
        return tuple(args)

    @staticmethod
    def _create_key_from_cha_epoch(cha_epoch):
        return ChannelEpochsHandler._create_key(
            cha_epoch.network,
            cha_epoch.station,
            cha_epoch.location,
            cha_epoch.channel,
            cha_epoch.restrictedStatus,
        )
