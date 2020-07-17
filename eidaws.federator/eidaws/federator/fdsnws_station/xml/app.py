# -*- coding: utf-8 -*-

import sys

from eidaws.federator.fdsnws_station.xml import SERVICE_ID, create_app
from eidaws.federator.utils.app import _main, config as default_config
from eidaws.federator.utils.cli import build_parser


DEFAULT_CONFIG = default_config()


def main(argv=sys.argv[1:]):
    parser = build_parser(SERVICE_ID, prog="eida-federator-station-xml")
    _main(
        SERVICE_ID, create_app, parser, argv=argv,
    )


if __name__ == "__main__":
    main()
