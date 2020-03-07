import re


def remove_non_integers_from_string(string_to_replace):
    new_string = re.sub(r"\D", "", string_to_replace)

    if new_string:
        return new_string
    else:
        return "0"
