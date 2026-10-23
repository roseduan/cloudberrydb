# Datalake Agent Testing Guide

## Quick Start

### One-Command Testing (Recommended)
```bash
# From project root
make test

# Or directly
cd test && ./make-test.sh
```

### Manual Testing
```bash
# 1. Start application
cd test && ./start-with-mock-parent.sh

# 2. Run tests (in another terminal)
cd test && ./run-tests.sh

# 3. Cleanup
cd test && ./cleanup.sh
```

## Scripts

- **`make-test.sh`** - Complete test suite (cleanup → start → test → cleanup)
- **`start-with-mock-parent.sh`** - Start application with mock parent process
- **`run-tests.sh`** - Run integration tests only
- **`cleanup.sh`** - Kill all test processes

## Debug

Application starts with debug port 5005:
- IntelliJ: Remote JVM Debug → localhost:5005
- VS Code: Java debugger → localhost:5005

## Troubleshooting

**Port conflicts:**
```bash
cd test && ./cleanup.sh
```

**Application won't start:**
```bash
# Check the agent jar and its dependency list
ls -la ../dlagent-rest/target/dlagent-rest-1.0.0.jar ../dlagent-rest/target/classpath-src.txt

# Rebuild
cd .. && mvn clean package -DskipTests
```

**Tests fail:**
```bash
# Use complete test suite
make test
```

## Manual API Testing

```bash
# Health check
curl http://localhost:8080/actuator/health

# Create table
curl -X POST http://localhost:8080/api/v1/hiveCatalogLocation/namespaces/default/tables/create \
  -H "Content-Type: application/json" \
  -d '{"name": "test_table", "schema": {"type": "struct", "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]}}'
```
