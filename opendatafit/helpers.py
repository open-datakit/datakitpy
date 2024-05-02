"""Miscellaneous helper functions"""


def find(array, key, value):
    """Equivalent of JS find() helper"""
    for i in array:
        if i[key] == value:
            return i

    return None


def find_by_name(array, name):
    """Given an array of objects with a "name" key, return the first object
    matching the name argument
    """
    return find(array, "name", name)
