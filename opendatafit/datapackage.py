"""opendata.fit datapackage helper functions"""


import pandas as pd


def find(array, key, value):
    """Equivalent of JS find() helper"""
    for i in array:
        if i[key] == value:
            return i

    return None


def find_by_name(array, name):
    return find(array, "name", name)


def get_algorithm_io_resource(datapackage, algorithm_name, io_type, io_name):
    """Get an algorithm input resource by name

    Parameters
    ----------
    datapackage: `dict`
        Bindfit datapackage object
    algorithm: `string`
        Name of algorithm
    io_type: `string` - "input" | "output"
        Whether to return an input or output resource
    input_name: `string`
        Name of input/output to return

    Returns
    -------
    resource: `dict` or `dict of dicts` or None
        Resource object, dict of resource objects or None if no matching
        resources
    """
    algorithm = find_by_name(datapackage["algorithms"], algorithm_name)
    io = find_by_name(algorithm[io_type + "s"], io_name)

    if io["type"] == "algorithmParams":
        return {
            key: find_by_name(datapackage["resources"], resource_name)
            for key, resource_name in io["parameterSpace"].items()
        }
    else:
        return find_by_name(datapackage["resources"], io["resource"])


def tabular_data_resource_to_dataframe(resource):
    """Convert Frictionless tabular data resource to pandas DataFrame"""
    df = pd.DataFrame.from_dict(resource["data"])
    # Reorder columns by schema field order
    cols = [field["name"] for field in resource["schema"]["fields"]]
    df = df[cols]
    return df


def dataframe_to_tabular_data_resource(df, resource):
    """Convert pandas DataFrame to Frictionless tabular data resource"""
    return resource.update({"data": df.to_dict(orient="records")})
