"""Helpers for executing datapackages and loading and writing resources"""

import json
import os
import time
from docker import DockerClient

from .helpers import find_by_name
from .resources import TabularDataResource


DEFAULT_BASE_PATH = os.getcwd()  # Default base datapackage path
ALGORITHMS_DIR = "algorithms"
ARGUMENTS_DIR = "arguments"
RESOURCES_DIR = "resources"
METASCHEMAS_DIR = "metaschemas"
VIEWS_DIR = "views"


class ExecutionError(Exception):
    def __init__(self, message, logs):
        super().__init__(message)
        self.logs = logs


class ResourceError(Exception):
    def __init__(self, message, resource):
        super().__init__(message)
        self.resource = resource


def execute_datapackage(
    docker_client: DockerClient,
    algorithm_name: str,
    argument_space_name: str = "default",
    base_path: str = DEFAULT_BASE_PATH,
) -> str:
    """Execute a datpackage and return execution logs"""
    # Get execution container name from the argument space
    container_name = load_argument_space(
        algorithm_name, argument_space_name, base_path
    )["container"]

    return execute_container(
        docker_client=docker_client,
        container_name=container_name,
        environment={
            "ALGORITHM": algorithm_name,
            "ARGUMENT_SPACE": argument_space_name,
        },
        base_path=base_path,
    )


def execute_view(
    docker_client: DockerClient,
    view_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> str:
    """Execute a view and return execution logs"""
    view = load_view(view_name, base_path)

    # Check required resources are populated
    for resource_name in view["resources"]:
        with open(
            f"{base_path}/{RESOURCES_DIR}/{resource_name}.json", "r"
        ) as f:
            if not json.load(f)["data"]:
                raise ResourceError(
                    (
                        f"Can't render view with empty resource "
                        f"{resource_name}. Have you executed the datapackage?"
                    ),
                    resource=resource_name,
                )

    # Get container name from view
    container_name = view["container"]

    # Execute view
    return execute_container(
        docker_client=docker_client,
        container_name=container_name,
        environment={
            "VIEW": view_name,
        },
        base_path=base_path,
    )


def execute_container(
    docker_client: DockerClient,
    container_name: str,
    environment: dict,
    base_path: str = DEFAULT_BASE_PATH,
) -> str:
    """Execute a container"""
    # We have to detach to get access to the container object and its logs
    # in the event of an error
    container = docker_client.containers.run(
        image=container_name,
        volumes=[f"{base_path}:/usr/src/app/datapackage"],
        environment=environment,
        detach=True,
    )

    # Block until container is finished running
    result = container.wait()

    if result["StatusCode"] != 0:
        raise ExecutionError(
            "Execution failed with status code {result['StatusCode']}",
            logs=container.logs().decode("utf-8").strip(),
        )

    return container.logs().decode("utf-8").strip()


def load_view(
    view_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load a view"""
    with open(f"{base_path}/{VIEWS_DIR}/{view_name}.json", "r") as f:
        return json.load(f)


def load_argument_space(
    algorithm_name: str,
    argument_space_name: str = "default",
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load an argument space"""
    with open(
        (
            f"{base_path}/{ARGUMENTS_DIR}/{algorithm_name}."
            f"{argument_space_name}.json"
        ),
        "r",
    ) as f:
        return json.load(f)


def write_argument_space(
    argument_space: dict,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Write an argument space"""
    with open(
        f"{base_path}/{ARGUMENTS_DIR}/{argument_space['name']}.json",
        "w",
    ) as f:
        json.dump(argument_space, f, indent=2)


def load_resource(
    resource_name: str,
    metaschema_name: str | None = None,
    base_path: str = DEFAULT_BASE_PATH,
) -> TabularDataResource | dict:
    """Load a resource with the specified metaschema"""
    # Load resource with metaschema
    resource_path = f"{base_path}/{RESOURCES_DIR}/{resource_name}.json"

    resource = None

    with open(resource_path, "r") as resource_file:
        # Load resource object
        resource_json = json.load(resource_file)

        if metaschema_name is not None:
            # Load metaschema into resource object
            with open(
                f"{base_path}/{METASCHEMAS_DIR}/{metaschema_name}.json", "r"
            ) as metaschema_file:
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
        else:
            # TODO: Temporary mostly harmless hack in order to be able
            # to load resource data into views, where we don't know or care
            # about the metaschema
            # Longer-term we should deal with this by handling empty
            # metaschemas in TabularDataResources, but this will do for now
            resource_json["metaschema"] = {"hello": "world"}

        if (
            resource_json["profile"] == "tabular-data-resource"
            or resource_json["profile"] == "parameter-tabular-data-resource"
        ):
            # TODO: Create ParameterResource object to handle parameters
            resource = TabularDataResource(resource=resource_json)
        else:
            raise NotImplementedError(
                f"Unknown resource profile \"{resource_json['profile']}\""
            )

    return resource


def load_resource_by_argument(
    algorithm_name: str,
    argument_name: str,
    argument_space_name: str,
    base_path: str,
) -> TabularDataResource:
    """Convenience function for loading resource associated with argument"""
    # Load argument object to get resource and metaschema names
    argument_space = load_argument_space(
        algorithm_name, argument_space_name, base_path=base_path
    )

    argument = find_by_name(argument_space["data"], argument_name)

    if argument is None:
        raise KeyError(
            (
                f"Can't find argument named {argument_name} in argument "
                f"space {argument_space_name}"
            )
        )

    return load_resource(
        resource_name=argument["resource"],
        metaschema_name=argument["metaschema"],
        base_path=base_path,
    )


def write_resource(
    resource: TabularDataResource | dict,
    base_path: str = DEFAULT_BASE_PATH,
) -> None:
    """Write updated resource to file"""
    if isinstance(resource, TabularDataResource):
        resource_json = resource.to_dict()
    else:
        resource_json = resource

    resource_path = f"{base_path}/{RESOURCES_DIR}/{resource_json['name']}.json"

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
