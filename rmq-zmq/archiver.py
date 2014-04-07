#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Archiver documentation.

This scripts implements the archivers using ZeroMQ receiving,
processing and storing incoming messages.

Example:
    Archivers usage example as follows:
    usage: python archiver.py [-h] [-c [CONFIG_FILE]] [-v [VHOST]]
                   [-q [QUEUE]] [-rk [ROUTING_KEY [ROUTING_KEY ...]]]
                   [-r [RULES]] [-oa] [-fo [FORMAT_OUTPUT]]
                   [-so [STORAGE_OUTPUT]] [-oo [output_level]]

Arguments:
  -c,  --config_file    config file name within config/ folder to be used
  -v,  --vhost          vhost to be used (included in a config section)
  -q,  --queue          queue name to be used
  -rk, --routing_key    routing keys to be used
  -r,  --rules          rules to be used
  -oa, --omit_archive   flag to omit raw data archiving
  -fo, --format_output  format of validated data
  -so, --storage_output format of raw data
  -oo, --omit_output    value to omit validated data archiving (
                            0 = no output, 1 = only valid ,2 = only invalid)

  -tm, --test_mode      flag for testing mode

"""

__author__ = "Nicolas Estrada"
__version__ = "0.0.1"