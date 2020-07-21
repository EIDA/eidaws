# -*- coding: utf-8 -*-

import argparse
import functools
import sys

from eidaws.federator.fdsnws_dataselect.miniseed import SERVICE_ID, create_app
from eidaws.federator.settings import (
    FED_DEFAULT_TMPDIR,
    FED_DEFAULT_BUFFER_ROLLOVER_SIZE,
    FED_DEFAULT_SPLITTING_FACTOR,
    FED_DEFAULT_FALLBACK_MSEED_RECORD_SIZE,
)
from eidaws.federator.utils.app import _main
from eidaws.federator.utils.cli import (
    build_parser as _build_parser,
    abs_path,
)
from eidaws.utils.cli import (
    between,
    positive_int,
    InterpolatingYAMLConfigFileParser,
)


def build_parser(config_file_parser_class=InterpolatingYAMLConfigFileParser):
    def fallback_mseed_record_size(num):
        if 0 != (positive_int(num) % 64):
            raise argparse.ArgumentTypeError("Not a multiple of 64 bytes.")
        return num

    parser = _build_parser(
        SERVICE_ID,
        prog="eida-federator-dataselect-miniseed",
        config_file_parser_class=config_file_parser_class,
    )
    parser.add_argument(
        "--tempdir",
        dest="tempdir",
        type=abs_path,
        default=FED_DEFAULT_TMPDIR,
        metavar="PATH",
        help="Absolute path to a temporary directory where buffers are "
        "stored. If not specified the value is determined as described "
        "under https://docs.python.org/3/library/tempfile.html#tempfile."
        "gettempdir.",
    )
    parser.add_argument(
        "--buffer-rollover-size",
        dest="buffer_rollover_size",
        type=positive_int,
        default=FED_DEFAULT_BUFFER_ROLLOVER_SIZE,
        metavar="BYTES",
        help="Defines when data is buffered on disk. If 0, data is never "
        "buffered on disk i.e. buffers are exclusively kept in memory. "
        "Buffering is using an approach based on spooled temporary files "
        "(https://docs.python.org/3/library/tempfile.html#tempfile."
        "SpooledTemporaryFile) (default: %(default)s).",
    )
    parser.add_argument(
        "--splitting-factor",
        dest="splitting_factor",
        metavar="NUM",
        type=functools.partial(between, num_type=int, minimum=2),
        default=FED_DEFAULT_SPLITTING_FACTOR,
        help="Splitting factor when performing splitting and aligning for "
        "large requests (default: %(default)s).",
    )
    parser.add_argument(
        "--fallback-miniseed-record-size",
        dest="fallback_mseed_record_size",
        type=fallback_mseed_record_size,
        metavar="BYTES",
        default=FED_DEFAULT_FALLBACK_MSEED_RECORD_SIZE,
        help="Fallback miniseed record size in bytes in case blockette 1000 "
        "was not found. If set to 0, miniseed data is considered as invalid "
        "in case of blockette 1000 missing. Valid values are a multiple of 64 "
        "bytes (default: %(default)s).",
    )

    return parser


parser = build_parser()


def main(argv=sys.argv[1:]):

    _main(
        SERVICE_ID, create_app, parser, argv=argv,
    )


if __name__ == "__main__":
    main()
