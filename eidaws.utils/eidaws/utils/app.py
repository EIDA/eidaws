# -*- coding: utf-8 -*-

import argparse
import copy
import sys

import configargparse

from eidaws.utils.error import Error, ExitCodes


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


class CustomParser(configargparse.ArgumentParser):
    """
    Custom argument parser.
    """

    def error(self, message):
        """
        Display both an error and print the help.
        :param str message: Error message to be displayed
        """
        sys.stderr.write("USAGE ERROR: %s\n" % message)
        self.print_help()
        sys.exit(ExitCodes.EXIT_ERROR)

    def format_help(self):
        return argparse.ArgumentParser.format_help(self)
