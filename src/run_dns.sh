#! /usr/bin/bash

bash ./delete_logs.sh


## Acceptable CLI arguments
# --nservers
# --nclients
# --unrelieable
# --crash
# --dnsDuration

go run main.go \
    --nservers 5 \
    --nclerks 5 \
    --dnsDuration 100 \
    # --crash \
    # --unreliable \

