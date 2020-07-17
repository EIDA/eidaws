# -*- coding: utf-8 -*-

import functools
import sys

from eidaws.federator.eidaws_wfcatalog.json import SERVICE_ID, create_app
from eidaws.federator.settings import (
    FED_DEFAULT_TMPDIR,
    FED_DEFAULT_BUFFER_ROLLOVER_SIZE,
    FED_DEFAULT_SPLITTING_FACTOR,
)
from eidaws.federator.utils.app import _main
from eidaws.federator.utils.cli import (
    build_parser as _build_parser,
    abs_path,
    between,
    positive_int,
)
from eidaws.utils.cli import InterpolatingYAMLConfigFileParser


def build_parser(config_file_parser_class=InterpolatingYAMLConfigFileParser):
    parser = _build_parser(
        SERVICE_ID,
        prog="eida-federator-wfcatalog-json",
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

    return parser


parser = build_parser()


def main(argv=sys.argv[1:]):

    _main(
        SERVICE_ID, create_app, parser, argv=argv,
    )


if __name__ == "__main__":
    main()
