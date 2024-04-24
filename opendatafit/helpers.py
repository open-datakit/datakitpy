"""opendata.fit helper functions"""


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


def tabular_data_resource_to_dataframe(resource):
    """Convert tabular data resource to pandas DataFrame preserving order"""
    df = pd.DataFrame.from_dict(resource["data"])
    # Reorder columns by schema field order
    cols = [field["name"] for field in resource["schema"]["fields"]]
    df = df[cols]
    return df


def dataframe_to_json(df):
    """Convert pandas DataFrame to JSON record format"""
    return df.to_dict(orient="records")
