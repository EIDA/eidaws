# -*- coding: utf-8 -*-

import enum


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
