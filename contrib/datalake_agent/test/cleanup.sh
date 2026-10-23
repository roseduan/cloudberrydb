#!/bin/bash

cd "$(dirname "$0")"

echo "🧹 Cleaning up test processes..."

# Stop exactly what start-with-mock-parent.sh started: the agent and its mock
# parent, by the pids it recorded.  Killing by port or by a command-line
# pattern, as this used to, also took down whatever else matched -- on a
# developer machine that includes the cluster's own dlagent, whose classpath
# contains "test".
if [ -f .agent.pids ]; then
    for pid in $(cat .agent.pids); do
        kill "$pid" 2>/dev/null || true
    done
    rm -f .agent.pids
fi

echo "✅ Cleanup completed!"
