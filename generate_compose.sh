#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <output_file_name> <number_of_clients>"
    exit 1
fi

python3 generate_compose.py $1 $2

echo "Generated docker compose file $1"