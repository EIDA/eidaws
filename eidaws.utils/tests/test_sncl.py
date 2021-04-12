# -*- coding: utf-8 -*-

import datetime
import pytest

from eidaws.utils.sncl import (
    Stream,
    StreamEpoch,
    StreamEpochs,
    StreamEpochsHandler,
)


class TestStreamEpochsHandler:
    @pytest.mark.parametrize(
        "epochs_slice,epochs_expected",
        [
            (
                {
                    "start": datetime.datetime(2018, 1, 13),
                    "end": datetime.datetime(2018, 1, 16),
                },
                [
                    (
                        datetime.datetime(2018, 1, 14),
                        datetime.datetime(2018, 1, 15),
                    )
                ],
            ),
            (
                {
                    "start": datetime.datetime(2018, 1, 2),
                    "end": datetime.datetime(2018, 1, 21),
                },
                [
                    (
                        datetime.datetime(2018, 1, 2),
                        datetime.datetime(2018, 1, 7),
                    ),
                    (
                        datetime.datetime(2018, 1, 14),
                        datetime.datetime(2018, 1, 15),
                    ),
                    (
                        datetime.datetime(2018, 1, 20),
                        datetime.datetime(2018, 1, 21),
                    ),
                ],
            ),
            (
                {
                    "start": None,
                    "end": None,
                },
                [
                    (
                        datetime.datetime(2018, 1, 1),
                        datetime.datetime(2018, 1, 7),
                    ),
                    (
                        datetime.datetime(2018, 1, 14),
                        datetime.datetime(2018, 1, 15),
                    ),
                    (
                        datetime.datetime(2018, 1, 20),
                        datetime.datetime(2018, 1, 27),
                    ),
                ],
            ),
        ],
        ids=["central-win", "slice-wins", "missing-start-end"],
    )
    def test_modify_with_temporal_constraints(
        self, epochs_slice, epochs_expected
    ):

        stream_epochs = [
            StreamEpochs(
                network="GR",
                station="BFO",
                location="",
                channel="LHZ",
                epochs=[
                    (
                        datetime.datetime(2018, 1, 1),
                        datetime.datetime(2018, 1, 7),
                    ),
                    (
                        datetime.datetime(2018, 1, 14),
                        datetime.datetime(2018, 1, 15),
                    ),
                    (
                        datetime.datetime(2018, 1, 20),
                        datetime.datetime(2018, 1, 27),
                    ),
                ],
            )
        ]
        expected = [
            StreamEpochs(
                network="GR",
                station="BFO",
                location="",
                channel="LHZ",
                epochs=epochs_expected,
            )
        ]

        ses_handler = StreamEpochsHandler(stream_epochs)
        ses_handler.modify_with_temporal_constraints(**epochs_slice)
        assert list(ses_handler) == expected


class TestStreamEpoch:
    @pytest.mark.parametrize(
        "epochs,epochs_expected,params",
        [
            (
                {
                    "starttime": datetime.datetime(2018, 1, 1),
                    "endtime": datetime.datetime(2018, 1, 8),
                },
                [
                    {
                        "starttime": datetime.datetime(2018, 1, 1),
                        "endtime": datetime.datetime(2018, 1, 4, 12),
                    },
                    {
                        "starttime": datetime.datetime(2018, 1, 4, 12),
                        "endtime": datetime.datetime(2018, 1, 8),
                    },
                ],
                {
                    "num": 2,
                },
            ),
            (
                {"starttime": datetime.datetime(2018, 1, 1), "endtime": None},
                [
                    {
                        "starttime": datetime.datetime(2018, 1, 1),
                        "endtime": datetime.datetime(2018, 1, 4, 12),
                    },
                    {
                        "starttime": datetime.datetime(2018, 1, 4, 12),
                        "endtime": datetime.datetime(2018, 1, 8),
                    },
                ],
                {"num": 2, "default_endtime": datetime.datetime(2018, 1, 8)},
            ),
            (
                {
                    "starttime": datetime.datetime(2018, 1, 1),
                    "endtime": datetime.datetime(2018, 1, 8),
                },
                [
                    {
                        "starttime": datetime.datetime(2018, 1, 1),
                        "endtime": datetime.datetime(2018, 1, 2, 18),
                    },
                    {
                        "starttime": datetime.datetime(2018, 1, 2, 18),
                        "endtime": datetime.datetime(2018, 1, 4, 12),
                    },
                    {
                        "starttime": datetime.datetime(2018, 1, 4, 12),
                        "endtime": datetime.datetime(2018, 1, 6, 6),
                    },
                    {
                        "starttime": datetime.datetime(2018, 1, 6, 6),
                        "endtime": datetime.datetime(2018, 1, 8),
                    },
                ],
                {
                    "num": 4,
                },
            ),
            (
                {
                    "starttime": datetime.datetime(2018, 1, 1),
                    "endtime": datetime.datetime(2018, 1, 4),
                },
                [
                    {
                        "starttime": datetime.datetime(2018, 1, 1),
                        "endtime": datetime.datetime(2018, 1, 2),
                    },
                    {
                        "starttime": datetime.datetime(2018, 1, 2),
                        "endtime": datetime.datetime(2018, 1, 3),
                    },
                    {
                        "starttime": datetime.datetime(2018, 1, 3),
                        "endtime": datetime.datetime(2018, 1, 4),
                    },
                ],
                {
                    "num": 3,
                },
            ),
        ],
        ids=[
            "with-endtime",
            "with-default-endtime",
            "test-num-even",
            "test-num-odd",
        ],
    )
    def test_slice(self, epochs, epochs_expected, params):

        stream = Stream(
            network="GR", station="BFO", location="", channel="LHZ"
        )
        stream_epoch = StreamEpoch(stream=stream, **epochs)

        expected = [
            StreamEpoch(stream=stream, **epoch) for epoch in epochs_expected
        ]
        assert sorted(stream_epoch.slice(**params)) == expected
