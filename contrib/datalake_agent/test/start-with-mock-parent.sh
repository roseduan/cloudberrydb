#!/bin/bash

# Navigate to project root directory
cd "$(dirname "$0")/.."

# Start application with mocked parent process for testing and VS Code debugging

echo "Starting application with mocked parent process and debug mode..."
echo "Debug port: 5005"
echo ""

# The REST agent is no longer one fat jar: it is the dlagent-rest module jar
# plus its dependencies, the same classpath datalake_proxy launches it with
# (see install-layout.sh).  classpath-src.txt is maven's own list of those
# dependencies, as paths that exist, with dlagent-core first.
JAR_FILE="dlagent-rest/target/dlagent-rest-1.0.0.jar"
DEPS_FILE="dlagent-rest/target/classpath-src.txt"
MAIN_CLASS="cloud.elastic.dlagent.service.ServiceApplication"

# The pids of what this script starts, so cleanup.sh stops exactly these and
# nothing else on the machine.
PID_FILE="test/.agent.pids"

if [ ! -f "$JAR_FILE" ] || [ ! -f "$DEPS_FILE" ]; then
    echo "Agent not built: $JAR_FILE / $DEPS_FILE"
    echo "Building the project..."
    mvn clean package -DskipTests
fi

# Create a background process that simulates the parent
create_mock_parent() {
    # The agent's watchdog exits unless its parent's command line contains
    # "datalake proxy".  exec -a renames bash itself, which is a real binary
    # everywhere; renaming sleep does not work where sleep is a wrapper
    # script (coreutils on Rocky 9), whose interpreter then shows instead.
    # The trailing ':' keeps bash from exec'ing sleep in its place.
    (
        exec -a "datalake proxy test" bash -c 'sleep 3600; :'
    ) &
    
    MOCK_PARENT_PID=$!
    echo "Created mock parent process with PID: $MOCK_PARENT_PID"
    
    # Wait a moment for the process to be created
    sleep 1
    
    # Verify the mock parent exists
    if kill -0 $MOCK_PARENT_PID 2>/dev/null; then
        echo "Mock parent process is running"
        
        # Start the application with the mock parent PID
        echo "Starting application with parent PID: $MOCK_PARENT_PID"
        echo "Application will be available on port 8080"
        echo "VS Code can attach debugger to localhost:5005"
        echo "Press Ctrl+C to stop"
        echo ""
        
        java -Xms512m \
             -Xmx1024m \
             -XX:+ExitOnOutOfMemoryError \
             -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
             -Dspring.main.banner-mode=off \
             -Dlogging.level.root=WARN \
             -Dlogging.level.cn.cbdb.dlagent=DEBUG \
             -Dlogging.pattern.console="%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n" \
             -Dspring.jmx.enabled=false \
             -Dspring.profiles.active=test \
             -cp "$JAR_FILE:$(cat "$DEPS_FILE")" "$MAIN_CLASS" \
             --parent.pid=$MOCK_PARENT_PID \
             --logging.config= \
             --server.port=8080 &
        AGENT_PID=$!
        echo "$MOCK_PARENT_PID $AGENT_PID" > "$PID_FILE"
        wait $AGENT_PID
        
        # Clean up mock parent when application exits
        kill $MOCK_PARENT_PID 2>/dev/null
        rm -f "$PID_FILE"
    else
        echo "Failed to create mock parent process"
        exit 1
    fi
}

echo "Found JAR file: $JAR_FILE"
create_mock_parent
