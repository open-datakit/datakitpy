"""Object definitions for loading and using data from Frictionless Resources"""


from copy import deepcopy
import pandas as pd


class TabularDataResource:
    _data: pd.DataFrame  # Resource data in labelled pandas DataFrame format
    _resource: dict  # Resource metadata in Frictionless JSON format

    def __init__(self, resource: dict) -> None:
        """Load tabular data resource from JSON dict"""
        # When initialising a tabular data resource there are two possibilites:
        # receiving an *empty resource* or a *populated resource*.
        #
        # Empty resource - metaschema is defined, data and schema are empty
        # Populated resource - metaschema, data and schema are all defined
        #
        # Both empty and populated resources MUST have a metaschema defined to
        # be loaded.

        # Load data into pandas DataFrame
        data = pd.DataFrame.from_dict(resource.pop("data"))

        if resource["metaschema"]:
            if resource["schema"] and not data.empty:
                # Populated resource

                # TODO: Validate schema against metaschema
                # TODO: Validate data against schema

                # Set data column order and index from schema
                cols = [
                    field["name"] for field in resource["schema"]["fields"]
                ]

                if set(cols) == set(data.columns):
                    # Reorder columns by schema field order
                    data = data[cols]

                    # Set index to primary key column(s)
                    data.set_index(
                        resource["schema"]["primaryKey"], inplace=True
                    )
                else:
                    # Data and column names do not match - this should not
                    # happen if we've received a properly validated
                    # resource
                    raise ValueError(
                        (
                            "{} resource data columns and"
                            "schema fields do not match"
                        ).format(resource["name"])
                    )
            elif data.empty:
                # Unpopulated resource, nothing to do
                pass
            else:
                # Resource has either data or schema properties missing
                raise ValueError(
                    "Populated resource {} missing data or schema".format(
                        resource["name"]
                    )
                )
        else:
            raise ValueError(
                "{}: tabular data resource metaschema cannot be empty".format(
                    resource["name"]
                )
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
        if not self:
            # Unpopulated resource with no schema - set schema from metaschema
            self._resource["schema"] = deepcopy(self._resource["metaschema"])

            # TODO: Remove index properties here?

        # Update resource data to match existing schema
        schema_cols = [
            field["name"] for field in self._resource["schema"]["fields"]
        ]

        if list(data.columns) != schema_cols:
            # Update index and column labels
            data = data.reset_index()
            data.columns = schema_cols
            data.set_index(
                self._resource["schema"]["primaryKey"], inplace=True
            )

        # Update data
        self._data = data

    def __bool__(self) -> bool:
        """True if resource is populated, False if not.

        Raises error if populated resource is missing either data or schema.
        """
        if self._resource["schema"] and not self._data.empty:
            # Populated resource
            return True
        elif self._data.empty:
            # Unpopulated resource
            return False
        else:
            # Resource has either data or schema properties missing
            raise ValueError(
                "Populated resource {} missing data or schema".format(
                    self._resource["name"]
                )
            )

    def to_dict(self) -> dict:
        """Return dict of resource data in JSON record row format"""
        # Convert data from DataFrame to JSON record row format
        resource_dict = deepcopy(self._resource)
        # Reset index first to workaround index=True not working with to_dict
        resource_dict["data"] = self._data.reset_index().to_dict(
            orient="records", index=True
        )
        return resource_dict
