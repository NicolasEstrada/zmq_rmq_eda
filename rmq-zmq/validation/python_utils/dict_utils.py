import re


def find_string_pattern(dictionary, rule):
    # Always start at the root of the dictionary
    actual_value = dictionary
    # The result to be returned
    results = []
    # It's only one result?
    single_result = True

    # This implies "exact or all" matches, so
    # Regex is not necessary
    for key in rule['key'].split('.'):
        if isinstance(actual_value, dict):
            if key in actual_value:
                actual_value = actual_value[key]
            elif key == '*':
                for key, value in actual_value.items():
                    results.append((key, value))

                single_result = False
            else:
                raise KeyError
        # If the actual level isn't a dictionary, it implies
        # that we are still trying to find a key, and we are in
        # a dead end
        else:
            raise KeyError

    if single_result:
        results.append((rule['map'], actual_value))

    return results


def find_regex_pattern(dictionary, rule):
    # Always start at the root of the dictionary
    actual_value = dictionary

    # First, compile all patterns in a list
    keys_to_match = [re.compile(keys) for keys in rule['key']]

    # Now, iterate over the patterns, because each pattern
    # represents a level in the dictionary
    for regex in keys_to_match:
        # Check if the actual level is a dictionary
        # if not, it implies that the search failed
        if isinstance(actual_value, dict):
            found = False

            # Now, for each key in the actual level...
            for key in actual_value:
                # Apply the regex
                if regex.match(key):
                    # If it match, break and continue to the
                    # next level
                    actual_value = actual_value[key]
                    found = True
                    break

            # If the key cannot be found, just cry about that
            if not found:
                raise KeyError
        # If the actual level isn't a dictionary, it implies
        # that we are still trying to find a key, and we are in
        # a dead end
        else:
            raise KeyError

    return [(rule['map'], actual_value)]


def map_dictionary(data, rules):
    results = []

    for rule in rules:
        keys = rule['key']

        # If the rule is a string, then is taken almost
        # literally for matching keys. Each part of the
        # string is a key in one level of the dictionary
        if isinstance(keys, (str, unicode)):
            results += find_string_pattern(data, rule)

        # If the rule is a list, it implies the use of Regex,
        # so the search is more complex
        elif isinstance(keys, list):
            # Just for compatibility, this function returns a list
            results += find_regex_pattern(data, rule)

    return results


def dicts_to_rows(data, keys):
    """Use this function to convert dictionaries into rows.
    Input:
        - data: List of dictionaries.
        - keys: List of keys to get from the dictionaries in input_stream

    Output: List of tuples.
    """

    # For each element in the input_stream, create a tuple with each of the
    # requested values from the dictionary, and return a list with all the results
    rows = []

    for dictionary in data:
        row = []
        for key in keys:
            row.append(dictionary[key])

        rows.append(tuple(row))

    return rows


def sum_dicts(a_dict, b_dict):
    """
    This function sums the values from b to a dictionary.

    Input:  a_dict: Dictionary to add values.
            b_dict: Dictionary to merge into a_dict

    Output: None.
    """
    for key in b_dict:
        if isinstance(b_dict[key], dict):
            if not key in a_dict:
                a_dict[key] = {}
            if b_dict[key]:
                sum_dicts(a_dict[key], b_dict[key])
        else:
            if key in a_dict:
                a_dict[key] += b_dict[key]
            else:
                a_dict[key] = b_dict[key]
