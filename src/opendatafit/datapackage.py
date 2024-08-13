"""Helper functions for loading and writing resources in an ODS datapackage"""

import json
import os
import time
from typing import Any

from .helpers import find_by_name
from .resources import TabularDataResource


DEFAULT_BASE_PATH = os.getcwd()  # Default base datapackage path
RESOURCES = "resources"
METASCHEMAS = "metaschemas"
ALGORITHMS = "algorithms"
ARGUMENTS = "arguments"
VIEWS = "views"


# Convenience dict mapping interface types to Python types
TYPE_MAP = {
    "string": str,
    "boolean": bool,
    "number": float | int,
}


# Exceptions


class OdsError(Exception):
    """Catch-all ODS exception"""

    pass


class EmptyResourceError(OdsError):
    """Raised when an empty resource causes an error"""

    def __init__(self, resource, message):
        self.resource = resource
        self.message = message
        super().__init__(self.message)


class InvalidProfileError(OdsError):
    """Raised when an invalid resource profile causes an error"""

    def __init__(self, profile, message):
        self.profile = profile
        self.message = message
        super().__init__(self.message)


class InvalidTypeError(OdsError):
    """Raised when an invalid type causes an error"""

    def __init__(self, actual_type, expected_type, message):
        self.actual_type = actual_type
        self.expected_type = expected_type
        self.message = message
        super().__init__(self.message)


class InvalidValueError(OdsError):
    """Raised when an invalid value causes an error"""

    def __init__(self, value, message):
        self.value = value
        self.message = message
        super().__init__(self.message)


class InvalidEnumValueError(OdsError):
    """Raised when an invalid enum value causes an error"""

    def __init__(self, value, allowed_values, message):
        self.value = value
        self.allowed_values = allowed_values
        self.message = message
        super().__init__(self.message)


# Views


def load_view(
    view_name: str,
    base_path: str = DEFAULT_BASE_PATH,
    check_resources: bool = False,  # Raise error if view resources empty
) -> dict:
    """Load the specified view"""
    with open(f"{VIEWS}/{view_name}.json", "r") as f:
        view = json.load(f)

    if check_resources:
        # Check resources required by the view are populated
        for resource_name in view["resources"]:
            with open(f"{RESOURCES}/{resource_name}.json", "r") as f:
                if not json.load(f)["data"]:
                    raise EmptyResourceError(
                        resource=resource_name,
                        message=(
                            "Can't load view with empty resource "
                            f"{resource_name}"
                        ),
                    )

    return view


# Argument spaces


def set_argument(
    argument_name: str,
    value: Any,
    algorithm_name: str,
    argument_space_name: str = "default",
    base_path: str = DEFAULT_BASE_PATH,
) -> None:
    """Set an argument value and check against interface definition"""
    # TODO

    # Load argument space
    argument_space = load_argument_space(
        algorithm_name, argument_space_name, base_path
    )

    # Load argument interface
    interface = get_interface_for_argument(
        algorithm_name, argument_name, base_path
    )

    # Check the argument isn't a resource that needs to be set directly
    profile = interface.get("profile")
    if profile in ["tabular-data-resource", "parameter-tabular-data-resource"]:
        raise InvalidProfileError(
            profile=profile,
            message=(
                f"Can't set argument with profile {profile} - edit the "
                "resource directly"
            ),
        )

    # Check the value matches the argument type defined in the interface
    expected_type = interface["type"]
    actual_type = type(value)
    # Note: specify False as fallback value for type_map.get here to avoid
    # "None"s leaking through
    if TYPE_MAP.get(expected_type, False) != actual_type:
        raise InvalidTypeError(
            expected_type=expected_type,
            actual_type=actual_type,
            message=(
                f"Argument value must be of type {expected_type}, but "
                f"set_argument got type {actual_type}"
            ),
        )

    # If this argument has an enum, check the value is allowed
    if interface.get("enum", False):
        allowed_values = [i["value"] for i in interface["enum"]]
        if value not in allowed_values:
            raise InvalidEnumValueError(
                value=value,
                allowed_values=allowed_values,
                message=(
                    f"Argument value must be one of {allowed_values}, but "
                    f"set_argument got value {value}"
                ),
            )

    # Check if nullable
    if not interface["null"]:
        if not value:
            raise InvalidValueError(
                value=value, message="Argument value cannot be null"
            )

    # Set value
    find_by_name(argument_space["data"], argument_name)["value"] = value

    # Write argument space
    write_argument_space(argument_space, base_path)


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


# Algorithms


def get_interface_for_argument(
    algorithm_name: str,
    argument_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load the algorithm interface definition for the specified argument"""
    algorithm = load_algorithm(algorithm_name, base_path)
    return find_by_name(algorithm["interface"], argument_name)


def load_algorithm(
    algorithm_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load an algorithm"""
    with open(f"{base_path}/{ALGORITHMS}/{algorithm_name}.json", "r") as f:
        return json.load(f)


# Arguments


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


# Resources


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
