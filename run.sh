#!/bin/bash

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        -c|--config)
        config_file="$2"
        shift 2
        ;;
        -h|--help)
        help="true"
        shift
        ;;
        --log-level)
        log_level="$2"
        shift 2
        ;;
        *)    # unknown option
        shift
        ;;
    esac
done

if [ -n "$help" ]; then
    go run cmd/chirpstack-gateway-bridge/main.go --help
else
    go run cmd/chirpstack-gateway-bridge/main.go --config "$config_file" --log-level "${log_level:-4}"
fi
