import os

import clint
import yaml

from jsonhandler import json


DEFAULT_BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FILE_TYPE = 'json'
AVAILABLE_PARSERS = ('yaml', 'json')
AVAILABLE_MODULES = {
    'yaml': yaml,
    'json': json
}

# Environment related variables
DEFAULT_CONFIG_FOLDER = 'config'
DEFAULT_ENVIRONMENT = 'production'
ENVIRONMENTS = {
    'development': 'development',
    'production': ''
}

# Check environment variables and flags to choose the correct
# path for the config files
# if 'WORKING_ENV' in os.environ:
#     working_env = os.environ['WORKING_ENV'].lower()
# elif '--environment' in clint.args.flags:
#     working_env = clint.args.grouped['--environment'][0].lower()
# else:
working_env = DEFAULT_ENVIRONMENT

try:
    environment = ENVIRONMENTS[working_env]
except KeyError:
    print "WARNING: The environment doesn't exist. Falling back to the production environment."
    environment = ENVIRONMENTS[DEFAULT_ENVIRONMENT]

CONFIG_PATH = os.path.join(DEFAULT_CONFIG_FOLDER, environment)


def load(config_file=None, file_type=None,
         section=None, omit_extension=False,
         config_path=CONFIG_PATH, base_path=DEFAULT_BASE_PATH):

    # First, we try to get the extension of the file
    _, file_extension = os.path.splitext(config_file)

    if len(file_extension) > 0:
        extension = file_extension[1:]

        # If the extension is in one of the available parsers
        # we will use it
        if extension in AVAILABLE_PARSERS:
            file_type = extension

        has_extension = True
    else:
        has_extension = False

    if not file_type:
        file_type = DEFAULT_FILE_TYPE
    elif file_type not in AVAILABLE_PARSERS:
        raise ValueError("File type '{0}' is not valid".format(file_type))

    # Check if the config_file parameter is an absolute path
    if not os.path.isabs(config_file):
        path = os.path.join(
            base_path,
            config_path, config_file)
    else:
        path = config_file

    if not has_extension and not omit_extension:
        path = "{0}.{1}".format(path, file_type)

    # Opens the file
    with open(path, 'r') as file_content:
        # Here we parse the file depending on the
        # file type indicated
        if file_type in AVAILABLE_MODULES:
            data = AVAILABLE_MODULES[file_type].load(file_content)

    # If we need only a section of the config file
    # it's returned. In other cases, we return the
    # entire config
    return data if section is None else data[section]
