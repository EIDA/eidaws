# -*- coding: utf-8 -*-

import collections
import os
import re
import sys

from string import Template

import configargparse

from eidaws.utils.app import ConfigurationError
from eidaws.utils.error import Error


class InterpolatingYAMLConfigFileParser(configargparse.YAMLConfigFileParser):
    SECTIONS = None

    def __init__(self, sections=None):
        self._sections = sections or self.SECTIONS

    def get_syntax_description(self):
        msg = (
            "The config file uses YAML syntax and must represent a YAML "
            "'mapping' (for details, see "
            "http://learn.getgrav.org/advanced/yaml). Environment variables "
            "are interpolated, automatically."
        )
        return msg

    def _parse_section(self, parsed_obj, section, stream):
        try:
            if not isinstance(
                parsed_obj[section],
                (collections.abc.Mapping, collections.abc.MutableMapping,),
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


# NOTE(damb): Modified from
# https://github.com/docker/compose/tree/master/compose/config


class InvalidInterpolation(Error):
    """{}"""


UnsetRequiredSubstitution = InvalidInterpolation


class Interpolator:
    def __init__(self, templater, mapping):
        self.templater = templater
        self.mapping = mapping

    def interpolate(self, string):
        try:
            return self.templater(string).substitute(self.mapping)
        except ValueError:
            raise InvalidInterpolation(string)


def interpolate_environment_variables(
    config, environment, section=None, converter=None
):
    interpolator = Interpolator(TemplateWithDefaults, environment)

    def process_item(name, config_dict):
        return dict(
            (
                key,
                interpolate_value(
                    name, key, val, section, interpolator, converter
                ),
            )
            for key, val in (config_dict or {}).items()
        )

    if section:
        return dict(
            (name, process_item(name, config_dict or {}))
            for name, config_dict in config.items()
        )

    # flat configuration
    return dict(
        (key, interpolate_value(None, key, val, None, interpolator, converter))
        for key, val in config.items()
    )


def get_config_path(config_key, section):
    return f"{section}/{config_key}"


def interpolate_value(
    name, config_key, value, section, interpolator, converter
):
    try:
        return recursive_interpolate(
            value,
            interpolator,
            get_config_path(config_key, section),
            converter,
        )
    except InvalidInterpolation as err:
        raise ConfigurationError(
            f"Invalid interpolation format for {config_key!r} option "
            f'in {section} "{name}": "{err}"'
        )
    except UnsetRequiredSubstitution as err:
        raise ConfigurationError(
            f'Missing mandatory value for "{config_key}" option '
            f'interpolating {value} in {section} "{name}": {err}'
        )


def recursive_interpolate(obj, interpolator, config_path, converter):
    def append(config_path, key):
        return f"{config_path}/{key}"

    converter = converter or null_converter

    if isinstance(obj, str):
        return converter.convert(config_path, interpolator.interpolate(obj))
    if isinstance(obj, dict):
        return dict(
            (
                key,
                recursive_interpolate(
                    val, interpolator, append(config_path, key), converter
                ),
            )
            for (key, val) in obj.items()
        )
    if isinstance(obj, list):
        return [
            recursive_interpolate(val, interpolator, config_path, converter)
            for val in obj
        ]
    return converter.convert(config_path, obj)


class TemplateWithDefaults(Template):
    pattern = r"""
        %(delim)s(?:
            (?P<escaped>%(delim)s) |
            (?P<named>%(id)s)      |
            {(?P<braced>%(bid)s)}  |
            (?P<invalid>)
        )
        """ % {
        "delim": re.escape("$"),
        "id": r"[_a-z][_a-z0-9]*",
        "bid": r"[_a-z][_a-z0-9]*(?:(?P<sep>:?[-?])[^}]*)?",
    }

    @staticmethod
    def process_braced_group(braced, sep, mapping):
        if ":-" == sep:
            var, _, default = braced.partition(":-")
            return mapping.get(var) or default
        elif "-" == sep:
            var, _, default = braced.partition("-")
            return mapping.get(var, default)

        elif ":?" == sep:
            var, _, err = braced.partition(":?")
            result = mapping.get(var)
            if not result:
                raise UnsetRequiredSubstitution(err)
            return result
        elif "?" == sep:
            var, _, err = braced.partition("?")
            if var in mapping:
                return mapping.get(var)
            raise UnsetRequiredSubstitution(err)

    # Modified from python2.7/string.py
    def substitute(self, mapping):
        # Helper function for .sub()

        def convert(mo):
            named = mo.group("named") or mo.group("braced")
            braced = mo.group("braced")
            if braced is not None:
                sep = mo.group("sep")
                if sep:
                    return self.process_braced_group(braced, sep, mapping)

            if named is not None:
                val = mapping[named]
                if isinstance(val, bytes):
                    val = val.decode("utf-8")
                return "%s" % (val,)
            if mo.group("escaped") is not None:
                return self.delimiter
            if mo.group("invalid") is not None:
                self._invalid(mo)
            raise ValueError(
                "Unrecognized named group in pattern", self.pattern
            )

        return self.pattern.sub(convert, self.template)


PATH_JOKER = "[^/]+"
FULL_JOKER = ".+"


def re_path(*args):
    return re.compile("^{}$".format("/".join(args)))


def re_path_basic(section, name):
    return re_path(section, PATH_JOKER, name)


def to_boolean(s):
    if not isinstance(s, str):
        return s
    s = s.lower()
    if s in ["y", "yes", "true", "on"]:
        return True
    elif s in ["n", "no", "false", "off"]:
        return False
    raise ValueError(f"{s!r} is not a valid boolean value")


def to_int(s):
    if not isinstance(s, str):
        return s

    # We must be able to handle octal representation for `mode` values notably
    if sys.version_info[0] >= 3 and re.match("^0[0-9]+$", s.strip()):
        s = "0o" + s[1:]
    try:
        return int(s, base=0)
    except ValueError:
        raise ValueError(f"{s!r} is not a valid integer")


def to_float(s):
    if not isinstance(s, str):
        return s

    try:
        return float(s)
    except ValueError:
        raise ValueError(f"{s!r} is not a valid float")


def to_str(o):
    if isinstance(o, (bool, float, int)):
        return f"{o}"
    return o


class ConversionMap:
    MAP = {}

    def convert(self, path, value):
        for rexp in self.MAP.keys():
            if rexp.match(path):
                try:
                    return self.MAP[rexp](value)
                except ValueError as err:
                    raise ConfigurationError(
                        "Error while attempting to convert {} to "
                        "appropriate type: {}".format(
                            path.replace("/", "."), err
                        )
                    )
        return value


null_converter = ConversionMap()
