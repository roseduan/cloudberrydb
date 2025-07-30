#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/cloudberry_env.sh"

if [ ! -d "$DATADIRS" ] || [ -z "$(ls -A "$DATADIRS")" ]; then
	exit 0
fi

cd ../../gpAux/gpdemo || {
	echo "Failed to cd into ../../gpAux/gpdemo"
	exit 1
}

echo "Destroying cluster and cleaning up data directory: $DATADIRS"
./demo_cluster.sh -d

if [ $? -ne 0 ]; then
	echo "Cluster destroy failed."
	exit 1
else
	echo "Cluster destroyed successfully."
fi
