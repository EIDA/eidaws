# -*- coding: utf-8 -*-

import sys

from eidaws.federator.fdsnws_station.xml import SERVICE_ID, create_app
from eidaws.federator.utils.app import _main
from eidaws.federator.utils.cli import build_parser as _build_parser
from eidaws.utils.cli import InterpolatingYAMLConfigFileParser


def build_parser(config_file_parser_class=InterpolatingYAMLConfigFileParser):
    return _build_parser(
        SERVICE_ID,
        prog="eida-federator-station-xml",
        config_file_parser_class=config_file_parser_class,
    )


parser = build_parser()


def main(argv=sys.argv[1:]):
    _main(
        SERVICE_ID, create_app, parser, argv=argv,
    )


if __name__ == "__main__":
    main()
