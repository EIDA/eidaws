# -*- coding: utf-8 -*-

import copy

from eidaws.utils.error import Error


def prepare_cli_config(
    args, remove_none_defaults=True, attrs_to_remove=["config"]
):
    args_view = vars(args)
    if remove_none_defaults:
        cli_config = {k: v for k, v in args_view.items() if v is not None}
    else:
        cli_config = copy.deepcopy(args_view)

    for attr in attrs_to_remove:
        try:
            del cli_config[attr]
        except KeyError:
            pass

    return cli_config


class AppError(Error):
    """Base application error ({})."""


class ConfigurationError(AppError):
    """Configuration error: {}"""
