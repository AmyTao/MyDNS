#! /usr/bin/bash

bash ./delete_logs.sh


## Acceptable CLI arguments
# --nservers
# --nclients
# --unrelieable
# --numCpus
# --crash
# --dnsDuration

go run main.go \
    --nservers 5 \
    --nclerks 5 \
    --dnsDuration 1000 \
    --numCpus 4 \
    # --crash \
    # --unreliable \

