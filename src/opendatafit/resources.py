"""Object definitions for loading and using data from Frictionless Resources"""


from copy import deepcopy
import pandas as pd


class TabularDataResource:
    _data: pd.DataFrame  # Resource data in labelled pandas DataFrame format
    _resource: dict  # Resource metadata in Frictionless JSON format

    def __init__(self, resource: dict) -> None:
        """Load tabular data resource from JSON dict"""
        # Load data into pandas DataFrame
        data = pd.DataFrame.from_dict(resource.pop("data"))

        # Set data index and column information from schema
        if resource["schema"]:
            if not data.empty:
                cols = [
                    field["name"] for field in resource["schema"]["fields"]
                ]
                if set(cols) == set(data.columns):
                    # Reorder columns by schema field order
                    data = data[cols]
                    # Set index to primary key column
                    data.set_index(
                        resource["schema"]["primaryKey"], inplace=True
                    )

                    # TODO: Check for and catch any errors due to mismatched
                    # shape
                else:
                    # This should not happen if we've received a properly
                    # validated resource
                    raise ValueError(
                        "Data columns and schema fields do not match."
                    )
        else:
            raise ValueError(
                "{} resource schema is empty".format(resource["name"])
            )

        # Save data
        self._data = data

        # Save resouce metadata
        self._resource = resource

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, data: pd.DataFrame) -> None:
        """Set data, updating column/index information to match schema"""
        # Check if data column information matches schema
        schema_cols = [
            field["name"] for field in self._resource["schema"]["fields"]
        ]

        # Update data column/index information to match schema if required
        if list(data.columns) != schema_cols:
            print("old cols", list(data.columns))
            print("new cols", schema_cols)

            # Update index and column labels
            print("data before", data)
            data = data.reset_index()
            data.columns = schema_cols
            data.set_index(
                self._resource["schema"]["primaryKey"], inplace=True
            )
            print("data after", data)

        # Update data
        self._data = data

    def __bool__(self) -> bool:
        """False if data table is empty, True if not"""
        return not self._data.empty

    def to_dict(self) -> dict:
        """Return dict of resource data in JSON record row format"""
        # Convert data from DataFrame to JSON record row format
        resource_dict = deepcopy(self._resource)
        # Reset index first to workaround index=True not working with to_dict
        resource_dict["data"] = self._data.reset_index().to_dict(
            orient="records", index=True
        )
        return resource_dict
