"""Object definitions for loading and using data from Frictionless Resources"""

from copy import deepcopy
import pandas as pd


from .helpers import has_user_defined_index


class TabularDataResource:
    _data: pd.DataFrame  # Resource data in labelled pandas DataFrame format
    _resource: dict  # Resource metadata in Frictionless JSON format

    def __init__(self, resource: dict) -> None:
        """Load tabular data resource from JSON dict"""
        # When initialising a tabular data resource there are two possibilites:
        # receiving an *empty resource* or a *populated resource*.
        #
        # Empty resource - format is defined, data and schema are empty
        # Populated resource - format, data and schema are all defined
        #
        # Both empty and populated resources MUST have a format defined to be
        # loaded.

        # Load data into pandas DataFrame
        data = pd.DataFrame.from_dict(resource.pop("data"))

        if resource["format"]:
            if resource["schema"] and not data.empty:
                # Populated resource

                # TODO: Validate schema against format
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
                    "Populated resource {} missing data or schema".format(
                        resource["name"]
                    )
                )
        else:
            raise ValueError(
                "{}: tabular data resource format cannot be empty".format(
                    resource["name"]
                )
            )

        # Save data
        self._data = data

        # Save resouce metadata
        self._resource = resource

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
        if not self:
            # Unpopulated resource, generate new schema before proceeding
            self._generate_schema(data)

        # Schema exists

        # Remove user-defined index if defined
        if has_user_defined_index(data):
            data = data.reset_index()

        # Set schema field titles from data column names
        data_columns = data.columns

        for i, column in enumerate(data_columns):
            print(self._resource["schema"])
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
        # Convert data from DataFrame to JSON record row format
        resource_dict = deepcopy(self._resource)

        # Include index in output dict
        # reset_index() workaround for index=True not working with to_dict
        resource_dict["data"] = self._data.reset_index().to_dict(
            orient="records", index=True
        )

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
        """Generate and set new resource schema from format and data"""
        # Declare schema fields array matching number of actual data fields
        if has_user_defined_index(data):
            schema_fields = [None] * len(data.reset_index().columns)
        else:
            schema_fields = [None] * len(data.columns)

        # Update fields based on format
        # TODO: Do we need to copy/deepcopy here?
        for format_field in self._resource["format"]["fields"]:
            format_field = deepcopy(format_field)

            # Get the indices this format field applies to
            index = format_field.pop("index")

            if ":" in index:
                # Index is slice notated

                # Parse slice notation
                s = slice(
                    *(int(part) if part else None for part in index.split(":"))
                )

                # Update schema fields selected in the slice

                # Create array of fields to be updated
                schema_fields_update = [
                    deepcopy(format_field)
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
                    schema_fields[int(index)] = format_field
                except IndexError:
                    raise IndexError(
                        (
                            "Error while setting data: can't generate schema "
                            "from format. Can't set schema field with format "
                            "index {}: field index out of range. Does your "
                            "data match the format? "
                            "Resource name: {}, "
                            "format fields: {}, "
                            "data: {}, "
                        ).format(
                            index,
                            self._resource["name"],
                            [
                                field["name"]
                                for field in self._resource["format"]["fields"]
                            ],
                            data,
                        )
                    )

        # Set resource schema
        self._resource["schema"] = {
            "fields": schema_fields,
        }

        # Add primaryKey to schema if set
        if "primaryKey" in self._resource["format"]:
            self._resource["schema"]["primaryKey"] = self._resource["format"][
                "primaryKey"
            ]
