#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <path_to_instances_configuration> <path_to_output_docker_compose>"
    exit 1
fi

python3 infra/generate_compose.py $1 $2

echo "Generated docker compose file $2"