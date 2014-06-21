from python_utils import logging_manager

from validations import VALIDATION_ALIASES


class MessageValidator(object):

    @property
    def validations(self):
        return self._validations

    @validations.setter
    def validations(self, value):
        self._validations = {}

        # This part is done to do faster lookups in the
        # validation functions. Only applied to validations
        # of type "enum"
        for key, definition in value.items():
            self._validations[key] = definition

            try:
                validation_type = definition.get('type')
            except AttributeError:
                continue

            if validation_type == 'enum':
                try:
                    options = definition['options']
                except TypeError:
                    continue

                if isinstance(options, list):
                    options = frozenset(options)
                else:
                    tmp = {}

                    for parent, valid_childs in options.items():
                        tmp[parent] = frozenset(valid_childs)

                    options = tmp

                self._validations[key]['options'] = options

    def __init__(self, rules, logger=None):
        self.validations = rules['validations']
        self.valid_keys = set(self.validations.keys())

        if 'required' not in rules:
            required = rules['validations'].keys()
        else:
            required = rules['required']

        self.simple_required = []
        self.grouped_required = []

        # Separate the single and grouped required fields
        for requirement in required:
            if isinstance(requirement, basestring):
                self.simple_required.append(requirement)
            else:
                # All the requirements here are iterables
                self.grouped_required.append(tuple(requirement))

        # Transform the lists on tuples, so they are static
        self.simple_required = tuple(self.simple_required)
        self.grouped_required = tuple(self.grouped_required)

        if logger is None:
            self.logger = logging_manager.start_logger(
                'validate_module',
                use_root_logger=False)
        else:
            self.logger = logger

    def _optimize_rules(self):
        for key, definition in self.validations.items():
            try:
                options = definition['options']

                if isinstance(options, list):
                    options = frozenset(options)
                else:
                    tmp = {}

                    for parent, valid_childs in options.items():
                        tmp[parent] = frozenset(valid_childs)

                    options = tmp

                self.validations[key]['options'] = options
            except (KeyError, TypeError):
                pass

    def check_required_keys(self, data):
        for key in self.simple_required:
            if key not in data:
                self.logger.debug("Required key [{0}] not found on data: {1}".format(key, data))
                return False

        for key_group in self.grouped_required:
            for key in key_group:
                if key in data:
                    break
            else:
                return False

        return True

    def validate(self, data):

        if not self.check_required_keys(data):
            return False

        keys_on_data = set(data.keys())

        # Get the present keys on the data that have rules defined
        keys_to_validate = keys_on_data.intersection(self.valid_keys)
        # Get the keys that doesn't have rules defined
        not_present_keys = keys_on_data.difference(self.valid_keys)

        if not_present_keys:
            self.logger.warning("No rules defined for keys {0}".format(not_present_keys))

        is_valid = False

        for key in keys_to_validate:
            value = data[key]
            validation = self.validations[key]

            # If one rule is a dictionary, it have options
            if isinstance(validation, dict):
                validation_func = VALIDATION_ALIASES[validation['type']]
                validation_options = validation['options']

                # If is instance of dict, analyze it
                if isinstance(validation_options, dict):
                    # Recover it from the actual data
                    parent_value = data.get(validation['depends_on'])

                    if parent_value is not None:
                        valid_options = validation_options[parent_value]
                        is_valid = validation_func(
                            value,
                            options=valid_options)
                    else:
                        self.logger.debug(
                            "Dependecy not satisfied. Key: '{0}' Depends on: '{1}' Raw Data: {2}".format(
                                key,
                                validation['depends_on'],
                                data))
                        return False

                elif isinstance(validation_options, list):
                    is_valid = validation_func(
                        value,
                        options=validation_options)
                else:
                    log_msg = "Validation rule '{0}' for key '{1}' doesn't have valid options: [{2}]".format(
                        validation,
                        key,
                        validation_options)

                    self.logger.error(log_msg)
                    raise ValueError(log_msg)

            elif isinstance(validation, basestring):
                is_valid = VALIDATION_ALIASES[validation](value)
            else:
                log_msg = "Validation '{0}' ({1}) for key '{2}' is not a valid rule".format(
                    validation,
                    type(validation),
                    key)

                self.logger.error(log_msg)
                raise ValueError(log_msg)

            # This makes sure that at the first failed validation
            # the method will return, saving
            if not is_valid:
                self.logger.debug(
                    'Key {0} is not valid: [{0}: {1}]'.format(
                        key,
                        data.get(key)))

                return False

        # At this point, it is valid
        self.logger.debug('Data is valid! [Data: {0}]'.format(data))
        return True


class MessageProcessor(object):

    def __init__(self, rules, logger=None):

        self.validations_keys = tuple(rules['validations'].keys())

        if logger is None:
            self.logger = logging_manager.start_loger(
                'validate_module',
                use_root_logger=False)
        else:
            self.logger = logger

    def process(self, data):
        clean_data = {}

        for key in self.validations_keys:
            try:
                clean_data[key] = data[key]
            except KeyError:
                # Expected for grouped required fields or optionals
                self.logger.debug(
                    'Key not found! [Data: {0}, key: {1}]'.format(data, key))

        return clean_data
