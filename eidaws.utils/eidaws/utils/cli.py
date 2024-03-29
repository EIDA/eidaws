# -*- coding: utf-8 -*-

import argparse
import collections
import functools
import os
import sys

import configargparse

from eidaws.utils.config import interpolate_environment_variables
from eidaws.utils.error import ExitCodes


def between(num, num_type=int, minimum=None, maximum=None):
    try:
        num = num_type(num)
        if minimum is not None and num < minimum:
            raise ValueError
        if maximum is not None and num > maximum:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentError(f"Invalid {num_type.__name__}: {num}")

    return num


def positive_num_or_none(num, num_type=int):
    if num is None:
        return None
    return between(num, num_type, minimum=0)


positive_int = functools.partial(between, num_type=int, minimum=0)
positive_int_exclusive = functools.partial(between, num_type=int, minimum=1)
positive_float = functools.partial(between, num_type=float, minimum=0)
positive_int_or_none = functools.partial(positive_num_or_none, num_type=int)
positive_float_or_none = functools.partial(
    positive_num_or_none, num_type=float
)
percent = functools.partial(between, num_type=float, minimum=0, maximum=100)
port = functools.partial(between, num_type=int, minimum=1, maximum=65535)


class NullConfigFileParser(configargparse.ConfigFileParser):
    def parse(self, stream):
        return collections.OrderedDict()


class InterpolatingYAMLConfigFileParser(configargparse.YAMLConfigFileParser):
    SECTIONS = None

    def __init__(self, sections=None):
        self._sections = sections or self.SECTIONS

    def get_syntax_description(self):
        return (
            super().get_syntax_description()
            + " Environment variables are interpolated, automatically."
        )

    def _parse_section(self, parsed_obj, section, stream):
        try:
            if not isinstance(
                parsed_obj[section],
                (
                    collections.abc.Mapping,
                    collections.abc.MutableMapping,
                ),
            ):

                raise configargparse.ConfigFileParserException(
                    "The config file doesn't appear to "
                    "contain 'key: value' pairs (aka. a YAML mapping). "
                    "yaml.load('%s') returned type '%s' instead of "
                    "type 'dict' in section %r."
                    % (
                        getattr(stream, "name", "stream"),
                        type(parsed_obj).__name__,
                        section,
                    )
                )

            # interpolate environment variables
            interpolated = interpolate_environment_variables(
                parsed_obj, os.environ, section
            )

        except KeyError:
            return {}
        else:
            return interpolated[section]

    def parse(self, stream):
        yaml = self._load_yaml()

        try:
            parsed_obj = yaml.safe_load(stream)
        except Exception as e:
            raise configargparse.ConfigFileParserException(
                "Couldn't parse config file: %s" % e
            )

        if not isinstance(parsed_obj, dict):
            raise configargparse.ConfigFileParserException(
                "The config file doesn't appear to "
                "contain 'key: value' pairs (aka. a YAML mapping). "
                "yaml.load('%s') returned type '%s' instead of type 'dict'."
                % (
                    getattr(stream, "name", "stream"),
                    type(parsed_obj).__name__,
                )
            )

        if isinstance(self._sections, str):
            parsed_obj = self._parse_section(
                parsed_obj, stream, self._sections
            )
        elif isinstance(self._sections, (list, tuple)):
            _maps = [
                self._parse_section(parsed_obj, stream, s)
                for s in reversed(self._sections)
            ]

            parsed_obj = collections.ChainMap(*_maps)
        else:
            parsed_obj = interpolate_environment_variables(
                parsed_obj, os.environ
            )

        result = collections.OrderedDict()
        for key, value in parsed_obj.items():
            if isinstance(value, list):
                result[key] = value
            else:
                result[key] = str(value)

        return result


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
