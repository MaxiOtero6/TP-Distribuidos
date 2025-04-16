import sys
from enum import Enum
from typing import List, Tuple, Dict, Optional

NAME: str = "Movies Analysis"
INSTANCE_SEPARATOR: str = "="
MOVIES_NETWORK_NAME: str = "movies_network"


def indent(text: str, level: int) -> str:
    return "  " * level + text


class WorkerType(Enum):
    FILTER = "filter"
    JOINER = "joiner"
    TOP = "top"
    OVERVIEW = "sentiment"
    MAP = "map"
    REDUCE = "reduce"

    def __str__(self) -> str:
        return self.value


class ServiceType(Enum):
    SERVER = "SERVER"
    CLIENT = "CLIENT"
    RABIT_MQ = "RABBITMQ"
    FILTER = "FILTER"
    JOINER = "JOINER"
    TOP = "TOP"
    OVERVIEW = "SENTIMENT"
    MAP = "MAP"
    REDUCE = "REDUCE"

    def to_service(self, id: int) -> "Service":
        match self:
            case ServiceType.SERVER:
                return Service(
                    container_name=f"server_{id}",
                    image="server:latest",
                    environment={"SERVER_PORT": str(8080 + id), "SERVER_ID": str(id)},
                    networks=[MOVIES_NETWORK_NAME],
                )
            case ServiceType.CLIENT:
                return Service(
                    container_name=f"client_{id}",
                    image="client:latest",
                    environment={"CLIENT_ID": str(id)},
                    networks=[MOVIES_NETWORK_NAME],
                )
            case ServiceType.RABIT_MQ:
                return Service(
                    container_name="rabbitmq",
                    image="rabbitmq:latest",
                    networks=[MOVIES_NETWORK_NAME],
                )
            case (
                ServiceType.FILTER
                | ServiceType.JOINER
                | ServiceType.TOP
                | ServiceType.OVERVIEW
                | ServiceType.MAP
                | ServiceType.REDUCE
            ):
                worker_name = self.value.split("_", 1)[0]
                return Service(
                    container_name=f"{worker_name}_{id}",
                    image=f"{worker_name}:latest",
                    environment={"WORKER_ID": str(id)},
                    networks=[MOVIES_NETWORK_NAME],
                )


class DockerCompose:
    name: str
    services: List["Service"]
    networks: List["Network"]

    def __init__(
        self, name: str, services: List["Service"], networks: List["Network"]
    ) -> None:
        self.name: str = name
        self.services: List[Service] = services
        self.networks: List[Network] = networks

    def __str__(self) -> str:
        lines: List[str] = []
        lines.append(f"name: {self.name}")
        lines.append("services:")
        for service in self.services:
            lines.append(str(service))

        lines.append("networks:")
        for network in self.networks:
            lines.append(str(network))

        return "\n".join(lines) + "\n"


class Service:
    container_name: str
    image: str
    entrypoint: Optional[str]
    networks: Optional[List[str]]
    environment: Optional[Dict[str, str]]
    depends_on: Optional[List[str]]
    volumes: Optional[Dict[str, str]]
    indent_level: int

    def __init__(
        self,
        container_name: str,
        image: str,
        entrypoint: Optional[str] = None,
        environment: Optional[Dict[str, str]] = None,
        networks: Optional[List[str]] = None,
        depends_on: Optional[List[str]] = None,
        volumes: Optional[Dict[str, str]] = None,
        indent_level: int = 1,
    ) -> None:
        self.container_name: str = container_name
        self.image: str = image
        self.entrypoint: Optional[str] = entrypoint
        self.environment: Optional[Dict[str, str]] = environment
        self.networks: Optional[List[str]] = networks
        self.depends_on: Optional[List[str]] = depends_on
        self.volumes: Optional[Dict[str, str]] = volumes
        self.indent_level: int = indent_level

    def __str__(self) -> str:
        level: int = self.indent_level
        lines: List[str] = []
        lines.append(indent(f"{self.container_name}:", level))
        lines.append(indent(f"container_name: {self.container_name}", level + 1))
        lines.append(indent(f"image: {self.image}", level + 1))
        if self.entrypoint:
            lines.append(indent(f"entrypoint: {self.entrypoint}", level + 1))

        if self.environment:
            lines.append(indent("environment:", level + 1))
            for key, value in self.environment.items():
                lines.append(indent(f"- {key}={value}", level + 2))

        if self.networks:
            lines.append(indent("networks:", level + 1))
            for network in self.networks:
                lines.append(indent(f"- {network}", level + 2))

        if self.depends_on:
            lines.append(indent("depends_on:", level + 1))
            for dep in self.depends_on:
                lines.append(indent(f"- {dep}", level + 2))

        if self.volumes:
            lines.append(indent("volumes:", level + 1))
            for key, value in self.volumes.items():
                lines.append(indent(f"- {key}:{value}", level + 2))

        return "\n".join(lines) + "\n"


class Network:
    network_name: str
    indent_level: int

    def __init__(self, network_name: str, indent_level: int = 1) -> None:
        self.network_name: str = network_name
        self.indent_level: int = indent_level

    def __str__(self) -> str:
        level: int = self.indent_level

        lines: List[str] = []
        lines.append(indent(f"{self.network_name}:", level))

        return "\n".join(lines) + "\n"


def get_args() -> Tuple[str, str]:
    try:
        return sys.argv[1], sys.argv[2]
    except IndexError:
        print(
            "Usage: python3 generate_ocmpose.py <path_to_instances_configuration> <path_to_output_docker_compose>"
        )
        sys.exit(1)


def read_instances(file_path: str) -> Dict["ServiceType", int]:
    instances_per_service: Dict[ServiceType, int] = {}

    with open(file_path, "r") as file:
        for line in file:
            try:
                service_type_str, count_str = line.strip().split(INSTANCE_SEPARATOR, 1)
                service_type: ServiceType = ServiceType(
                    service_type_str.strip().upper()
                )
                count: int = int(count_str.strip())

                instances_per_service[service_type] = count
            except (ValueError, KeyError):
                print(f"Invalid line in instances file: {line.strip()}")
                continue

    return instances_per_service


def generate_docker_compose(
    instances_per_service: Dict[ServiceType, int],
) -> DockerCompose:
    services: List[Service] = [
        service_type.to_service(id)
        for service_type, count in instances_per_service.items()
        for id in range(count)
    ]

    networks: List[Network] = [
        Network(
            network_name=MOVIES_NETWORK_NAME,
        )
    ]

    docker_compose = DockerCompose(
        name=NAME,
        services=services,
        networks=networks,
    )

    return docker_compose


def write_to_file(output_file: str, compose: DockerCompose) -> None:
    with open(output_file, "w") as file:
        file.write(str(compose))


def main() -> None:
    n_instances_path, output_file_path = get_args()
    instances_per_service = read_instances(n_instances_path)
    docker_compose: DockerCompose = generate_docker_compose(instances_per_service)
    write_to_file(output_file_path, docker_compose)


if __name__ == "__main__":
    main()
