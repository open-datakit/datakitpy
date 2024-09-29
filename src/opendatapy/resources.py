"""Object definitions for loading and using data from Frictionless Resources"""

from copy import deepcopy
import pandas as pd
import numpy as np


def has_default_index(df):
    """Check if a DataFrame has the default RangeIndex"""
    if pd.Index(np.arange(0, len(df))).equals(df.index) and not df.index.name:
        return True
    else:
        return False


def data_to_dict(data: pd.DataFrame) -> dict:
    """Convert DataFrame to dict"""
    # Check if the dataframe has a user-defined index
    # This checks if the index matches the auto-generated plain pandas index
    if has_default_index(data):
        # Plain index - don't include in dict

        # Replace any NaNs that pandas inserts sometimes for some reason
        return data.replace({np.nan: None}).to_dict(orient="records")
    else:
        # User-defined index - include in dict
        return (
            data.reset_index()
            .replace({np.nan: None})
            .to_dict(orient="records")
        )


class TabularDataResource:
    _data: pd.DataFrame  # Resource data in labelled pandas DataFrame format
    _resource: dict  # Resource metadata in Frictionless JSON format

    def __init__(self, resource: dict, metaschema: dict = None) -> None:
        """Load tabular data resource from JSON dict"""
        # Load data into pandas DataFrame
        data = pd.DataFrame.from_dict(resource.pop("data"))

        # Save remaining resouce metadata
        self._resource = resource

        # Save metaschema
        self._metaschema = metaschema

        if resource["schema"] and not data.empty:
            # Populated resource

            # TODO: Validate schema against metaschema
            # TODO: Validate data against schema

            # Set data column order and index from schema
            cols = [field["name"] for field in resource["schema"]["fields"]]

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
                        f"{resource['name']} resource data columns "
                        f"{data.columns} and schema fields {cols} do not "
                        "match"
                    ).format(resource["name"])
                )
        elif data.empty:
            # Unpopulated resource, nothing to do
            pass
        else:
            # Resource has either data or schema properties missing
            raise ValueError(
                f"Populated resource {resource['name']} missing data or schema"
            )

        # Save data
        self._data = data

    @property
    def name(self) -> str:
        return self._resource["name"]

    @property
    def profile(self) -> str:
        return self._resource["profile"]

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, data: pd.DataFrame) -> None:
        """Set data, updating column/index information to match schema"""
        # If the resource is not yet populated and no schema is set, generate
        # a new schema from the metaschema before proceeding
        if not self and not self._resource["schema"]:
            # Unpopulated resource, generate new schema before proceeding
            self._generate_schema(data)

        # Schema exists

        # Remove user-defined index if defined
        if not has_default_index(data):
            data = data.reset_index()

        # Set schema field titles from data column names
        data_columns = data.columns

        for i, column in enumerate(data_columns):
            self._resource["schema"]["fields"][i]["title"] = column

        # Update data column names to match schema names (not titles)
        schema_cols = [
            field["name"] for field in self._resource["schema"]["fields"]
        ]

        if list(data.columns) != schema_cols:
            data.columns = schema_cols

        # Set index to specified primary key(s)
        data.set_index(self._resource["schema"]["primaryKey"], inplace=True)

        # Update data
        self._data = data

    def to_dict(self) -> dict:
        """Return dict of resource data in Frictionless Resource format

        Data returned inline in JSON record row format"""
        resource_dict = deepcopy(self._resource)
        resource_dict["data"] = data_to_dict(self._data)
        return resource_dict

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

    def __str__(self) -> str:
        return str(self._data)

    def _generate_schema(self, data) -> None:
        """Generate and set resource schema from metaschema and data"""
        # Declare schema fields array matching number of actual data fields
        if not has_default_index(data):
            schema_fields = [None] * len(data.reset_index().columns)
        else:
            schema_fields = [None] * len(data.columns)

        # Update fields based on metaschema
        # TODO: Do we need to copy/deepcopy here?
        for metaschema_field in self._metaschema["schema"]["fields"]:
            metaschema_field = deepcopy(metaschema_field)

            # Get the indices this metaschema field applies to
            index = metaschema_field.pop("index")

            if ":" in index:
                # Index is slice notated

                # Parse slice notation
                s = slice(
                    *(int(part) if part else None for part in index.split(":"))
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
                try:
                    schema_fields[int(index)] = metaschema_field
                except IndexError:
                    raise IndexError(
                        (
                            "Error while setting data: can't generate schema "
                            "from metaschema. Can't set schema field with "
                            "metaschema index {}: field index out of range. "
                            "Does your data match the metaschema? "
                            "Resource name: {}, "
                            "Metaschema fields: {}, "
                            "Data: {}, "
                        ).format(
                            index,
                            self._resource["name"],
                            [
                                field["name"]
                                for field in self._metaschema["schema"][
                                    "fields"
                                ]
                            ],
                            data,
                        )
                    )

        # Set resource schema
        self._resource["schema"] = {
            "fields": schema_fields,
        }

        # Add primaryKey to schema if set
        if "primaryKey" in self._metaschema["schema"]:
            self._resource["schema"]["primaryKey"] = self._metaschema[
                "schema"
            ]["primaryKey"]
