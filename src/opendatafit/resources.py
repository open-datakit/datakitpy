"""Object definitions for loading and using data from Frictionless Resources"""

import os
import json
import time
from copy import deepcopy
import pandas as pd

from .helpers import find_by_name, has_user_defined_index


# Default base datapackage path
DEFAULT_BASE_PATH = os.getcwd()
RESOURCES = "resources"
METASCHEMAS = "metaschemas"
ALGORITHMS = "algorithms"
ARGUMENTS = "arguments"
VIEWS = "views"


def load_argument(
    algorithm_name: str,
    argument_name: str,
    argument_space_name: str = "default",
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load a specified argument"""
    # Get name of resource and metaschema from specified argument
    with open(
        f"{base_path}/{ARGUMENTS}/{algorithm_name}.{argument_space_name}.json",
        "r",
    ) as f:
        argument = find_by_name(json.load(f)["data"], argument_name)

    if argument is None:
        raise KeyError(
            (
                f"Can't find argument named {argument_name} in argument "
                f"space {argument_space_name}"
            )
        )

    return argument


def load_resource(
    resource_name: str,
    metaschema_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load a resource with the specified metaschema"""
    # Load resource with metaschema
    resource_path = f"{base_path}/{RESOURCES}/{resource_name}.json"

    resource = None

    with open(resource_path, "r") as resource_file:
        # Load resource object
        resource_json = json.load(resource_file)

        if "tabular-data-resource" in resource_json["profile"]:
            with open(
                f"{base_path}/{METASCHEMAS}/{metaschema_name}.json", "r"
            ) as metaschema_file:
                # Load metaschema into resource object
                resource_json["metaschema"] = json.load(metaschema_file)[
                    "schema"
                ]

                # Copy metaschema to resource schema if specified
                if resource_json["schema"] == "metaschema":
                    # Copy metaschema to schema
                    resource_json["schema"] = resource_json["metaschema"]
                    # Label schema as metaschema copy so we don't overwrite it
                    # when writing back to resource
                    resource_json["schema"]["type"] = "metaschema"

                    resource = TabularDataResource(resource=resource_json)
        else:
            raise NotImplementedError(
                f"Unknown resource profile \"{resource_json['profile']}\""
            )

    return resource


def write_resource(
    resource: dict,
    base_path: str = DEFAULT_BASE_PATH,
    debug: bool = False,
) -> None:
    """Write updated resource to file"""
    resource_path = f"{base_path}/{RESOURCES}/{resource['name']}.json"

    if debug:
        print(f"Writing to resource at {resource_path}")

    # Remove metaschema before writing
    # This should have been loaded by load_argument
    resource.pop("metaschema")

    if resource["schema"].get("type") == "metaschema":
        resource["schema"] = "metaschema"  # Don't write metaschema copy

    with open(resource_path, "w") as f:
        json.dump(resource, f, indent=2)

    # Update modified time in datapackage.json
    with open(f"{base_path}/datapackage.json", "r") as f:
        dp = json.load(f)

    dp["updated"] = int(time.time())

    with open(f"{base_path}/datapackage.json", "w") as f:
        json.dump(dp, f, indent=2)


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
            # Unpopulated resource, generate new schema before proceeding
            self._generate_schema(data)

        # Schema exists

        # Remove user-defined index if defined
        if has_user_defined_index(data):
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
        """Return dict of resource data in JSON record row format"""
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
        """Generate and set new resource schema from metaschema and data"""
        # Declare schema fields array matching number of actual data fields
        if has_user_defined_index(data):
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
                            "Error while setting data: can't generate "
                            "schema from metaschema. Can't set schema "
                            " field with metaschema index {}: field index "
                            "out of range. Does your data match the "
                            "metaschema? "
                            "Resource name: {}, "
                            "metaschema fields: {}, "
                            "data: {}, "
                        ).format(
                            index,
                            self._resource["name"],
                            [
                                field["name"]
                                for field in self._resource["metaschema"][
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
        if "primaryKey" in self._resource["metaschema"]:
            self._resource["schema"]["primaryKey"] = self._resource[
                "metaschema"
            ]["primaryKey"]
