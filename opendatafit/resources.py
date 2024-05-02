"""Object definitions for loading and using data from Frictionless Resources"""


import json
import pandas as pd
from typing import Union, Optional


class TabularDataResource:
    data: pd.DataFrame  # Resource data as pandas DataFrame

    _resource: dict  # Resource metadata in Frictionless JSON format
    _filepath: str  # Path to the resource JSON
    _schema_filepath: Union[str, None]  # Path to external schema JSON

    def __init__(
        self, filepath: str, schema_filepath: Optional[str] = None
    ) -> None:
        """Load tabular data resource from file"""

        with open(filepath, "r") as f:
            self._resource = json.load(f)

        # Load schema from separate file if provided
        if schema_filepath is not None:
            with open(schema_filepath, "r") as f:
                self._resource["schema"] = json.load(f)

        # Load data into pandas DataFrame
        data = self._resource.pop("data")
        self._data = pd.DataFrame.from_dict(data)

        # Reorder columns by schema field order
        cols = [field["name"] for field in self._resource["schema"]["fields"]]
        self._data = self._data[cols]

        # Convert pandas DataFrame to JSON record format
        # return df.to_dict(orient="records")
