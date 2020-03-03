import re


def remove_non_integers_from_string(string_to_replace):
    return re.sub(r"\D", "", string_to_replace)
