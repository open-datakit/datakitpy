"""Helpers for executing datakits and loading and writing resources"""

import json
import os
import shutil
import time
import pandas as pd
from docker import DockerClient

from .helpers import find_by_name
from .resources import data_to_dict, TabularDataResource


DEFAULT_BASE_PATH = os.getcwd()  # Default base datakit path


# Path helper format strings


RESOURCES_DIR = "{base_path}/{run_name}/resources"
RESOURCE_FILE = "{base_path}/{run_name}/resources/{resource_name}.json"
VIEWS_DIR = "{base_path}/{algorithm_name}/views"
VIEW_ARTEFACTS_DIR = "{base_path}/{run_name}/views"
VIEW_FILE = "{base_path}/{algorithm_name}/views/{view_name}.json"
ALGORITHM_FILE = "{base_path}/{algorithm_name}/algorithm.json"
RELATIONSHIPS_FILE = "{base_path}/{algorithm_name}/relationships.json"
ALGORITHM_DIR = "{base_path}/{algorithm_name}"
RUN_DIR = "{base_path}/{run_name}"
RUN_FILE = "{base_path}/{run_name}/run.json"
METASCHEMA_FILE = (
    "{base_path}/{algorithm_name}/metaschemas/{metaschema_name}.json"
)
DATAKIT_FILE = "{base_path}/datakit.json"


# Custom exceptions


class ExecutionError(Exception):
    def __init__(self, message, logs):
        super().__init__(message)
        self.logs = logs


class ResourceError(Exception):
    def __init__(self, message, resource):
        super().__init__(message)
        self.resource = resource


# General helpers


def get_algorithm_name(run_name):
    """Get algorithm name from run name"""
    return run_name.split(".")[0]


# Private helpers


def _update_modified_time(base_path: str = DEFAULT_BASE_PATH) -> None:
    # Update modified time in datakit.json
    with open(DATAKIT_FILE.format(base_path=base_path), "r") as f:
        dp = json.load(f)

    dp["updated"] = int(time.time())

    with open(DATAKIT_FILE.format(base_path=base_path), "w") as f:
        json.dump(dp, f, indent=2)


# Datakit helpers


def execute_datakit(
    docker_client: DockerClient,
    run_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> str:
    """Execute a datakit and return execution logs"""
    # Get execution container name from the configuration
    container_name = load_run_configuration(run_name, base_path)["container"]

    return execute_container(
        docker_client=docker_client,
        container_name=container_name,
        environment={
            "RUN": run_name,
        },
        base_path=base_path,
    )


def execute_view(
    docker_client: DockerClient,
    run_name: str,
    view_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> str:
    """Execute a view and return execution logs"""
    view = load_view(
        run_name=run_name, view_name=view_name, base_path=base_path
    )

    # Check required resources are populated
    for resource_name in view["resources"]:
        with open(
            RESOURCE_FILE.format(
                base_path=base_path,
                run_name=run_name,
                resource_name=resource_name,
            ),
            "r",
        ) as f:
            if not json.load(f)["data"]:
                raise ResourceError(
                    (
                        f"Can't render view with empty resource "
                        f"{resource_name}. Have you executed the datakit?"
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
            "RUN": run_name,
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
        volumes=[f"{base_path}:/usr/src/app/datakit"],
        environment=environment,
        detach=True,
        user=os.getuid(),  # Run as current user (avoid permissions issues)
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
    run_name: str,
    view_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load a view"""
    with open(
        VIEW_FILE.format(
            base_path=base_path,
            algorithm_name=get_algorithm_name(run_name),
            view_name=view_name,
        ),
        "r",
    ) as f:
        return json.load(f)


def load_algorithm(
    algorithm_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load an algorithm configuration"""
    with open(
        ALGORITHM_FILE.format(
            base_path=base_path, algorithm_name=algorithm_name
        ),
        "r",
    ) as f:
        return json.load(f)


def write_algorithm(
    algorithm: dict,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Write an algorithm configuration"""
    with open(
        ALGORITHM_FILE.format(
            base_path=base_path, algorithm_name=algorithm["name"]
        ),
        "w",
    ) as f:
        json.dump(algorithm, f, indent=2)


def load_run_configuration(
    run_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load a run configuration"""
    with open(
        RUN_FILE.format(base_path=base_path, run_name=run_name),
        "r",
    ) as f:
        return json.load(f)


def write_run_configuration(
    run: dict,
    base_path: str = DEFAULT_BASE_PATH,
) -> None:
    """Write a run configuration"""
    with open(
        RUN_FILE.format(base_path=base_path, run_name=run["name"]),
        "w",
    ) as f:
        json.dump(run, f, indent=2)

    _update_modified_time(base_path=base_path)


def load_variable(
    run_name: str, variable_name: str, base_path: str = DEFAULT_BASE_PATH
):
    configuration = load_run_configuration(run_name, base_path=base_path)

    return find_by_name(
        configuration["data"]["inputs"] + configuration["data"]["outputs"],
        variable_name,
    )


def load_variable_signature(
    run_name: str, variable_name: str, base_path: str = DEFAULT_BASE_PATH
):
    signature = load_algorithm(
        algorithm_name=get_algorithm_name(run_name),
        base_path=base_path,
    )["signature"]

    return find_by_name(
        signature["inputs"] + signature["outputs"],
        variable_name,
    )


def load_datakit_configuration(
    base_path: str = DEFAULT_BASE_PATH,
) -> dict:
    """Load datakit configuration"""
    with open(
        DATAKIT_FILE.format(base_path=base_path),
        "r",
    ) as f:
        return json.load(f)


def write_datakit_configuration(
    datakit: dict,
    base_path: str = DEFAULT_BASE_PATH,
) -> None:
    """Write datakit configuration"""
    # Set last modified time to now
    datakit["updated"] = int(time.time())

    with open(
        DATAKIT_FILE.format(base_path=base_path),
        "w",
    ) as f:
        json.dump(datakit, f, indent=2)


def init_resource(
    run_name: str,
    resource_name: str,
    base_path: str = DEFAULT_BASE_PATH,
) -> None:
    """Initialise a resource for the specified run"""
    # Copy the resource scaffold from [algorithm]/resources/[resource] to
    # [algorithm.run]/resources/[resource]
    src = RESOURCE_FILE.format(
        base_path=base_path,
        run_name=get_algorithm_name(run_name),
        resource_name=resource_name,
    )

    dst = RESOURCE_FILE.format(
        base_path=base_path,
        run_name=run_name,
        resource_name=resource_name,
    )

    shutil.copyfile(src, dst)


def load_resource(
    run_name: str,
    resource_name: str,
    metaschema_name: str | None = None,
    base_path: str = DEFAULT_BASE_PATH,
    as_dict: bool = False,  # Load resource as raw dict
) -> TabularDataResource | dict:
    """Load a resource"""
    resource = None

    with open(
        RESOURCE_FILE.format(
            base_path=base_path, run_name=run_name, resource_name=resource_name
        ),
        "r",
    ) as resource_file:
        # Load resource object
        resource_json = json.load(resource_file)

        if as_dict:
            resource = resource_json
        elif (
            resource_json["profile"] == "tabular-data-resource"
            or resource_json["profile"] == "parameter-tabular-data-resource"
        ):
            # Load metaschema
            if metaschema_name is not None:
                with open(
                    METASCHEMA_FILE.format(
                        base_path=base_path,
                        algorithm_name=get_algorithm_name(run_name),
                        metaschema_name=metaschema_name,
                    ),
                    "r",
                ) as f:
                    metaschema = json.load(f)
            else:
                metaschema = None

            resource = TabularDataResource(
                resource=resource_json, metaschema=metaschema
            )
        else:
            # TODO: Create ParameterResource object to handle parameters
            raise NotImplementedError(
                f"Unknown resource profile \"{resource_json['profile']}\""
            )

    return resource


def load_resource_by_variable(
    run_name: str,
    variable_name: str,
    base_path: str,
    as_dict: bool = False,  # Load resource as raw dict
) -> TabularDataResource | dict:
    """Convenience function for loading resource associated with a variable"""
    # Load configuration to get resource and any applicable metaschema names
    variable = load_variable(run_name, variable_name, base_path)

    if variable is None:
        raise KeyError(
            (
                f"Can't find variable named {variable_name} in run "
                f"configuration {run_name}"
            )
        )

    return load_resource(
        run_name=run_name,
        resource_name=variable["resource"],
        metaschema_name=variable.get("metaschema"),
        base_path=base_path,
        as_dict=as_dict,
    )


def write_resource(
    run_name: str,
    resource: TabularDataResource | dict,
    base_path: str = DEFAULT_BASE_PATH,
) -> None:
    """Write updated resource to file"""
    if isinstance(resource, TabularDataResource):
        resource_json = resource.to_dict()
    else:
        resource_json = resource

    with open(
        RESOURCE_FILE.format(
            base_path=base_path,
            run_name=run_name,
            resource_name=resource_json["name"],
        ),
        "w",
    ) as f:
        json.dump(resource_json, f, indent=2)

    _update_modified_time(base_path=base_path)


def update_resource(
    run_name: str,
    resource_name: str,
    data: pd.DataFrame | None = None,
    schema: dict | None = None,
    base_path: str = DEFAULT_BASE_PATH,
) -> None:
    """Partially update a resource"""
    resource = load_resource(
        run_name=run_name,
        resource_name=resource_name,
        base_path=base_path,
        as_dict=True,
    )

    if data is not None:
        resource["data"] = data_to_dict(data)

    if schema is not None:
        resource["schema"] = schema

    write_resource(run_name=run_name, resource=resource, base_path=base_path)
