# -*- coding: utf-8 -*-

from aiohttp import web

from eidaws.endpoint_proxy import create_app
from eidaws.endpoint_proxy.cli import build_parser
from eidaws.endpoint_proxy.utils import setup_logger
from eidaws.endpoint_proxy.version import __version__


def main(argv=None):

    parser = build_parser(prog="eida-endpoint-proxy")
    args = parser.parse_args(args=argv)
    args = vars(args)

    logger = setup_logger(args["path_logging_conf"], capture_warnings=True)
    logger.info(f"Version v{__version__}")
    logger.debug(f"Service configuration: {args}")

    app = create_app(config_dict=args)
    logger.info(f"Application routes: {list(app.router.routes())}")

    # run standalone app
    web.run_app(
        app, host=args["hostname"], port=args["port"], path=args["unix_path"],
    )
    parser.exit(message="Stopped\n")


if __name__ == "__main__":
    main()
