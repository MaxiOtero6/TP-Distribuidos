import sys
from typing import List, Tuple, Dict, Optional

NAME: str = "Movies Analysis"

def indent(text: str, level: int) -> str:
    return "  " * level + text

class Service:
    def __init__(
        self,
        container_name: str,
        image: str,
        entrypoint: str,
        environment: Optional[Dict[str, str]],
        networks: Optional[List[str]],
        depends_on: Optional[List[str]],
        volumes: Optional[Dict[str, str]],
        indent_level: int = 1,
    ) -> None:
        self.container_name: str = container_name
        self.image: str = image
        self.entrypoint: str = entrypoint
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
    def __init__(self, network_name: str, driver: str, subnets: List[str], indent_level: int = 1) -> None:
        self.network_name: str = network_name
        self.driver: str = driver
        self.subnets: List[str] = subnets
        self.indent_level: int = indent_level

    def __str__(self) -> str:
        level: int = self.indent_level

        lines: List[str] = []
        lines.append(indent(f"{self.network_name}:", level))
        lines.append(indent("ipam:", level + 1))
        lines.append(indent(f"driver: {self.driver}", level + 2))
        lines.append(indent("config:", level + 2))
        for subnet in self.subnets:
            lines.append(indent(f"- subnet: {subnet}", level + 3))

        return "\n".join(lines) + "\n"

def get_args() -> Tuple[str, int]:
    try:
        output_file: str = sys.argv[1]
        n_clients: int = int(sys.argv[2])
    except IndexError:
        print("Usage: python3 generate_ocmpose.py <output_file> <n_clients>")
        sys.exit(1)
    except ValueError:
        print("Number of clients must be an integer")
        sys.exit(1)

    return output_file, n_clients

def generate_docker_compose_elements(n_clients: int) -> Tuple[List[Service], List[Network]]:
    server: Service = Service(
        container_name="server",
        image="server:latest",
        entrypoint="python3 /main.py",
        environment={"PYTHONUNBUFFERED": "1", "NUMBER_AGENCIES": str(n_clients)},
        networks=["docker_compose_network"],
        depends_on=[],
        volumes={"./server/config.ini": "/config.ini"},
        indent_level=1,
    )

    client_services: List[Service] = []
    for i in range(n_clients):
        container_name: str = f"client{i}"
        client_services.append(
            Service(
                container_name=container_name,
                image="client:latest",
                entrypoint="/client",
                environment={"CLI_ID": str(i)},
                networks=["docker_compose_network"],
                depends_on=["server"],
                volumes={
                    "./client/config.yaml": "/config.yaml",
                    f"./.data/agency-{i}.csv": "/agency.csv",
                },
                indent_level=1,
            )
        )

    network: Network = Network(
        network_name="docker_compose_network",
        driver="default",
        subnets=["172.25.125.0/24"],
        indent_level=1,
    )

    services: List[Service] = [server] + client_services
    networks: List[Network] = [network]

    return services, networks

def write_to_file(output_file: str, services: List[Service], networks: List[Network]) -> None:
    with open(output_file, "w") as f:
        f.write(f"name: {NAME}\n")
        
        f.write("services:\n")
        for service in services:
            f.write(str(service) + "\n")
        
        f.write("networks:\n")
        for network in networks:
            f.write(str(network) + "\n")

def main() -> None:
    output_file, n_clients = get_args()
    services, networks = generate_docker_compose_elements(n_clients)
    write_to_file(output_file, services, networks)

if __name__ == "__main__":
    main()
