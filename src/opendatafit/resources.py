"""Object definitions for loading and using data from Frictionless Resources"""


from copy import deepcopy
import pandas as pd

from .helpers import dataframe_has_index


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
                    if "primaryKey" in resource["schema"]:
                        data.set_index(
                            resource["schema"]["primaryKey"], inplace=True
                        )
                else:
                    # Data and column names do not match - this should not
                    # happen if we've received a properly validated
                    # resource
                    raise ValueError(
                        (
                            "{} resource data columns and "
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
            # Unpopulated resource, generate new schema from metaschema

            # Declare schema fields array matching number of actual data fields
            if dataframe_has_index(data):
                schema_fields = [None] * len(data.reset_index().columns)
            else:
                schema_fields = [None] * len(data.columns)

            # Update fields based on metaschema
            # TODO: Do we need to copy/deepcopy here?
            for metaschema_field in self._resource["metaschema"]["fields"]:
                metaschema_field = deepcopy(metaschema_field)

                # Get the indices this metaschema field applies to
                index = metaschema_field.pop("index")

                if ":" in index:
                    # Index is slice notated

                    # Parse slice notation
                    s = slice(
                        *(
                            int(part) if part else None
                            for part in index.split(":")
                        )
                    )

                    # Update schema fields selected in the slice

                    # Create array of fields to be updated
                    schema_fields_update = [
                        deepcopy(metaschema_field)
                        for i in range(len(schema_fields[s]))
                    ]

                    # Make field names unique
                    for i, schema_field in enumerate(schema_fields_update):
                        schema_field["name"] = schema_field["name"] + str(i)

                    # Set fields
                    schema_fields[s] = schema_fields_update
                else:
                    # Index is an integer, set field directly
                    schema_fields[int(index)] = metaschema_field

            # Set resource schema
            self._resource["schema"] = {
                "fields": schema_fields,
            }

            # Add primaryKey to schema if set
            if "primaryKey" in self._resource["metaschema"]:
                self._resource["schema"]["primaryKey"] = self._resource[
                    "metaschema"
                ]["primaryKey"]

        # Schema exists - merge data and schema labels
        # Add data column names as human-readable titles to schema
        # TODO: Does it make sense to do this? Or should we let metaschema
        # override data column labels?

        # Merge resource data labels and existing schema
        if dataframe_has_index(data):
            data_columns = data.reset_index().columns
        else:
            data_columns = data.columns

        for i, column in enumerate(data_columns):
            self._resource["schema"]["fields"][i]["title"] = column

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

        if dataframe_has_index(self.data):
            # Include index in output dict
            # reset_index() workaround for index=True not working with to_dict
            resource_dict["data"] = self._data.reset_index().to_dict(
                orient="records", index=True
            )
        else:
            # Don't include default index in output dict
            resource_dict["data"] = self._data.to_dict(
                orient="records", index=True
            )

        return resource_dict
