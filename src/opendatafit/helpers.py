"""Miscellaneous helper functions"""
import pandas as pd


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


def has_user_defined_index(df):
    """Check if a DataFrame has a user-defined index"""
    return (
        not isinstance(df.index, pd.RangeIndex)
        or not (df.index == pd.RangeIndex(start=0, stop=len(df))).all()
    )
