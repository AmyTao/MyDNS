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
    --dnsDuration 10 \
    --numCpus 4 \
    --unreliable \
    # --crash \

