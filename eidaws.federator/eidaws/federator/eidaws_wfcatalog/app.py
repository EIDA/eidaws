# -*- coding: utf-8 -*-

import sys

from eidaws.federator.eidaws_wfcatalog import SERVICE_ID, create_app
from eidaws.federator.settings import (
    FED_DEFAULT_TMPDIR,
    FED_DEFAULT_BUFFER_ROLLOVER_SIZE,
    FED_DEFAULT_SPLITTING_FACTOR,
)
from eidaws.federator.utils.app import (
    _main,
    config as default_config,
    config_schema as default_config_schema,
)


PROG = "eida-federator-wfcatalog-json"

DEFAULT_CONFIG = default_config()
DEFAULT_CONFIG.setdefault("tempdir", FED_DEFAULT_TMPDIR)
DEFAULT_CONFIG.setdefault("buffer_rollover_size", FED_DEFAULT_BUFFER_ROLLOVER_SIZE)
DEFAULT_CONFIG.setdefault("splitting_factor", FED_DEFAULT_SPLITTING_FACTOR)

config_schema = default_config_schema
config_schema["properties"]["tempdir"] = {
    "oneOf": [{"type": "null"}, {"type": "string", "pattern": "^/"}]
}
config_schema["properties"]["buffer_rollover_size"] = {
    "type": "integer",
    "minimum": 0,
}
config_schema["properties"]["splitting_factor"] = {
    "type": "integer",
    "minimum": 2,
}


def main(argv):
    _main(
        SERVICE_ID,
        create_app,
        prog=PROG,
        argv=argv,
        default_config=DEFAULT_CONFIG,
        config_schema=config_schema,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
