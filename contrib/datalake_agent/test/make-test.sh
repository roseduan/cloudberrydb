#!/bin/bash

cd "$(dirname "$0")"

echo "🚀 Make Test - Complete Integration Test Suite"
echo ""

# Step 1: Cleanup any existing processes
echo "📋 Step 1: Cleaning up existing processes..."
./cleanup.sh

# Step 2: Start application
echo ""
echo "📋 Step 2: Starting application..."
./start-with-mock-parent.sh &
START_PID=$!

# Step 3: Wait for application to be ready
echo ""
echo "📋 Step 3: Waiting for application to be ready..."
cd ..
for i in {1..60}; do
    # --noproxy: the agent is local, and with http_proxy set curl would ask
    # the proxy instead, which never reports it ready.
    if curl --noproxy '*' -sf http://localhost:8080/actuator/health > /dev/null 2>&1; then
        echo "✅ Application is ready!"
        break
    fi
    sleep 2
    if [ $i -eq 60 ]; then
        echo "❌ Application failed to start within 120 seconds"
        cd test && ./cleanup.sh
        exit 1
    fi
    if [ $((i % 15)) -eq 0 ]; then
        echo "Still waiting... (${i}0s)"
    fi
done

# Step 4: Run tests
echo ""
echo "📋 Step 4: Running integration tests..."
cd test
./run-tests.sh
TEST_RESULT=$?

# Step 5: Cleanup
echo ""
echo "📋 Step 5: Cleaning up..."
./cleanup.sh

# Report results
echo ""
if [ $TEST_RESULT -eq 0 ]; then
    echo "🎉 All tests passed!"
else
    echo "❌ Tests failed!"
fi

exit $TEST_RESULT
