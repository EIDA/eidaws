# -*- coding: utf-8 -*-

import collections
import logging
import logging.config
import logging.handlers  # needed for handlers defined in logging.conf
import warnings
import yaml

from aiohttp import TCPConnector
from jsonschema import validate, ValidationError

from eidaws.endpoint_proxy.settings import PROXY_BASE_ID
from eidaws.utils.app import ConfigurationError


def get_config(path_config=None, cli_config={}, defaults={}, json_schema=None):

    user_config = {PROXY_BASE_ID: {}}

    if path_config is None:
        assert defaults, "Using empty default configuration not supported."

        return {"config": defaults}

    try:
        with open(path_config) as ifd:
            _user_config = yaml.safe_load(ifd)

        if _user_config is not None and isinstance(
            _user_config.get(PROXY_BASE_ID),
            (collections.abc.Mapping, collections.abc.MutableMapping),
        ):
            user_config = _user_config

    except yaml.YAMLError as err:
        warnings.warn(f"Exception while parsing configuration file: {err}")
    except FileNotFoundError as err:
        warnings.warn(f"Configuration file not found ({err}). Using defaults.")

    config_dict = {
        PROXY_BASE_ID: {
            "config": collections.ChainMap(
                cli_config, user_config[PROXY_BASE_ID], defaults,
            )
        }
    }

    if json_schema is not None:
        try:
            validate(
                instance=dict(config_dict[PROXY_BASE_ID]["config"]),
                schema=json_schema,
            )
        except ValidationError as err:

            raise ConfigurationError(str(err))

    return config_dict


def setup_http_conn_pool(app):

    config = app[PROXY_BASE_ID]["config"]
    conn = TCPConnector(limit=config["connection_limit"],)

    async def close_endpoint_http_conn_pool(app):
        await conn.close()

    app.on_cleanup.append(close_endpoint_http_conn_pool)
    app[PROXY_BASE_ID]["http_conn_pool"] = conn
    return conn


def setup_logger(path_logging_conf=None, capture_warnings=False):
    """
    Initialize the logger of the application.
    """
    logging.basicConfig(level=logging.WARNING)

    LOGGER = PROXY_BASE_ID

    if path_logging_conf is not None:
        try:
            logging.config.fileConfig(path_logging_conf)
            logger = logging.getLogger(LOGGER)
            logger.info(
                "Using logging configuration read from "
                f"{path_logging_conf!r}."
            )
        except Exception as err:
            print(
                f"WARNING: Setup logging failed for {path_logging_conf!r} "
                f"with error: {err!r}."
            )
            logger = logging.getLogger(LOGGER)
    else:
        logger = logging.getLogger(LOGGER)
        logger.addHandler(logging.NullHandler())

    logging.captureWarnings(bool(capture_warnings))

    return logger
