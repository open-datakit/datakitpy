"""Object definitions for loading and using data from Frictionless Resources"""


import pandas as pd


class TabularDataResource:
    data: pd.DataFrame  # Resource data as pandas DataFrame
    _resource: dict  # Resource metadata in Frictionless JSON format

    def __init__(self, resource: dict) -> None:
        """Load tabular data resource from JSON dict"""

        print("resource:", resource["name"])
        print("schema:", resource["schema"])

        # Load data into pandas DataFrame
        data = resource.pop("data")
        self._data = pd.DataFrame.from_dict(data)

        # Reorder columns by schema field order
        cols = [field["name"] for field in resource["schema"]["fields"]]
        self._data = self._data[cols]

        self._resource = resource

        # Convert pandas DataFrame to JSON record format
        # return df.to_dict(orient="records")
