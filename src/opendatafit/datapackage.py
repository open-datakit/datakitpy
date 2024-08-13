"""Helper functions for loading and writing resources in an ODS datapackage"""

import json
import os
import time

from .helpers import find_by_name
from .resources import TabularDataResource


# Default base datapackage path
DEFAULT_BASE_PATH = os.getcwd()
RESOURCES = "resources"
METASCHEMAS = "metaschemas"
ALGORITHMS = "algorithms"
ARGUMENTS = "arguments"
VIEWS = "views"


def load_argument_space(
    algorithm_name: str,
    argument_space_name: str = "default",
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load a specified argument space"""
    with open(
        f"{base_path}/{ARGUMENTS}/{algorithm_name}.{argument_space_name}.json",
        "r",
    ) as f:
        return json.load(f)


def write_argument_space(
    argument_space: dict,
    base_path: str = DEFAULT_BASE_PATH,
) -> None:
    """Write updated argument space to file"""
    with open(
        f"{base_path}/{ARGUMENTS}/{argument_space['name']}.json", "w"
    ) as f:
        json.dump(argument_space, f, indent=2)


def load_argument_interface(
    algorithm_name: str,
    argument_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load the algorithm interface definition for the specified argument"""
    with open(f"{base_path}/{ALGORITHMS}/{algorithm_name}.json", "r") as f:
        return find_by_name(json.load(f)["interface"], argument_name)


def load_argument(
    algorithm_name: str,
    argument_name: str,
    argument_space_name: str = "default",
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load a specified argument"""
    argument_space = load_argument_space(algorithm_name, argument_space_name)

    argument = find_by_name(argument_space["data"], argument_name)

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
) -> TabularDataResource | dict:
    """Load a resource with the specified metaschema"""
    # Load resource with metaschema
    resource_path = f"{base_path}/{RESOURCES}/{resource_name}.json"

    resource = None

    with open(resource_path, "r") as resource_file:
        # Load resource object
        resource_json = json.load(resource_file)

        # Load metaschema into resource object
        with open(
            f"{base_path}/{METASCHEMAS}/{metaschema_name}.json", "r"
        ) as metaschema_file:
            resource_json["metaschema"] = json.load(metaschema_file)["schema"]

        # Copy metaschema to resource schema if specified
        if resource_json["schema"] == "metaschema":
            # Copy metaschema to schema
            resource_json["schema"] = resource_json["metaschema"]
            # Label schema as metaschema copy so we don't overwrite it
            # when writing back to resource
            resource_json["schema"]["type"] = "metaschema"

        if resource_json["profile"] == "tabular-data-resource":
            resource = TabularDataResource(resource=resource_json)
        elif resource_json["profile"] == "parameter-tabular-data-resource":
            # TODO: Create ParameterResource object to handle this case
            resource = resource_json
        else:
            raise NotImplementedError(
                f"Unknown resource profile \"{resource_json['profile']}\""
            )

    return resource


def write_resource(
    resource: TabularDataResource | dict,
    base_path: str = DEFAULT_BASE_PATH,
) -> None:
    """Write updated resource to file"""
    if isinstance(resource, TabularDataResource):
        resource_json = resource.to_dict()
    else:
        resource_json = resource

    resource_path = f"{base_path}/{RESOURCES}/{resource_json['name']}.json"

    # Remove metaschema before writing
    # This should have been loaded by load_argument
    resource_json.pop("metaschema")

    if resource_json["schema"].get("type") == "metaschema":
        resource_json["schema"] = "metaschema"  # Don't write metaschema copy

    with open(resource_path, "w") as f:
        json.dump(resource_json, f, indent=2)

    # Update modified time in datapackage.json
    with open(f"{base_path}/datapackage.json", "r") as f:
        dp = json.load(f)

    dp["updated"] = int(time.time())

    with open(f"{base_path}/datapackage.json", "w") as f:
        json.dump(dp, f, indent=2)
