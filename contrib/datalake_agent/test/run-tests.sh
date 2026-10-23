#!/bin/bash

cd "$(dirname "$0")/.."

echo "🧪 Running integration tests..."

# Only dlagent-rest has this test, so build just it and what it depends on;
# across the whole reactor surefire fails every other module for running no
# test.  -Dtest overrides the pom's exclusion of *IntegrationTest.
mvn -pl dlagent-rest -am test -Dtest=IcebergRestControllerIntegrationTest \
    -Dsurefire.failIfNoSpecifiedTests=false
rc=$?

if [ $rc -ne 0 ]; then
    echo "❌ Integration tests failed (mvn exit code $rc)"
    exit $rc
fi

# The tests skip themselves where the storage stack they need is not up (see
# AgentAvailability); say so rather than let a run of skips pass for a run.
report=dlagent-rest/target/surefire-reports/cloud.elastic.dlagent.service.rest.IcebergRestControllerIntegrationTest.txt
if [ -f "$report" ]; then
    grep "Tests run" "$report"
    if grep -q "Skipped: [1-9]" "$report"; then
        echo "⚠️  Some tests were skipped: no agent on 8080 or no storage stack"
    fi
fi

echo "✅ Integration tests completed!"
