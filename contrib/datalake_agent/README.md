# datalake agent

The JVM process that datalake_fdw talks to for catalog operations and the
data plane. datalake_proxy (a preloaded library) starts it with the
coordinator and restarts it if it exits.

There are two agents, chosen by `datalake.agent_transport`: `grpc`, the
default, or `rest`, the fallback. A build without grpc++ >= 1.50 (see
`grpcxx.mk`) has no gRPC support; there `rest` is the default and `grpc` is
refused at startup.

| Module          | What it is                                              |
|-----------------|---------------------------------------------------------|
| `dlagent-core`  | All transport-neutral logic, shared by both agents      |
| `dlagent-rest`  | The REST agent: a Spring Boot shell over `dlagent-core` |
| `dlagent-grpc`  | The gRPC agent: grpc-java + Guice over `dlagent-core`   |
| `dlagent-parity`| Tests that both agents give the same status and bytes   |

## Switching between gRPC and REST

The setting is `PGC_POSTMASTER`: datalake_proxy picks the agent once, when
the cluster starts, so a switch is a configuration change plus a restart.
Nothing else changes: no DDL, no catalog change, no SERVER, CATALOG,
VOLUME or foreign table to recreate.

To switch to REST, on the coordinator as gpadmin:

```bash
gpconfig -c datalake.agent_transport -v rest
gpstop -ar
psql -Atc 'SHOW datalake.agent_transport'      # rest
```

To go back to the default, remove the setting the same way:

```bash
gpconfig -r datalake.agent_transport
gpstop -ar
psql -Atc 'SHOW datalake.agent_transport'      # grpc
```

What follows the setting:

| | `grpc` (default) | `rest` |
|---|---|---|
| Agent main class | `cloud.elastic.dlagent.grpc.Main` | `cloud.elastic.dlagent.service.ServiceApplication` |
| Port | `datalake.agent_grpc_port`, default 3889 | 3888 (`server.port` in the agent's `application.properties`) |
| `datalake_proxy.restart_time` default | 5 s | 30 s (Spring Boot starts slower) |
| `datalake.rpc_max_message_size` | applies (default 64 MB) | not used |

The FDW finds either agent through `datalake.agent_server_url`: under
`rest` it is the whole address, under `grpc` only its host is used, with
`datalake.agent_grpc_port`. The two ports differ
on purpose: a leftover agent of the other kind fails to connect instead of
half-answering.

To check which agent is running after the restart (`pgrep -f` is not
enough on CentOS 7, which only matches the first 4 KB of the command line,
and the main class comes after a ~28 KB classpath):

```bash
grep -ls 'dlagent\.grpc\.Main' /proc/[0-9]*/cmdline                    # gRPC
grep -ls 'dlagent\.service\.ServiceApplication' /proc/[0-9]*/cmdline   # REST
```

## Installed layout

`make install` lays the agents out under `$GPHOME/lib/postgresql/java/`:

```
java/
  core/        dlagent-core-1.0.0.jar and every dependency both agents use
  rest/        dlagent-rest-1.0.0.jar and dependencies only REST uses
  grpc/        dlagent-grpc-1.0.0.jar and dependencies only gRPC uses
  core.cp  rest.cp  grpc.cp   the ordered classpath of each, relative to java/
```

datalake_proxy starts the agent with `java -cp <rest.cp or grpc.cp> <main
class>`, not with `java -jar`. **There is no `dlagent-1.0.0.jar` any more**,
and `make install` removes one left by an older install, because a stale
jar next to the thin jars could be loaded instead of them.

## Replacing the agent on a running cluster

What used to be "copy `dlagent-1.0.0.jar`" is now "copy the module jar that
changed", on the coordinator host:

| Changed                        | Copy                                                         |
|--------------------------------|--------------------------------------------------------------|
| only `dlagent-rest` code       | `dlagent-rest/target/dlagent-rest-1.0.0.jar` to `java/rest/` |
| only `dlagent-grpc` code       | `dlagent-grpc/target/dlagent-grpc-1.0.0.jar` to `java/grpc/` |
| `dlagent-core` code            | `dlagent-core/target/dlagent-core-1.0.0.jar` to `java/core/` |
| any dependency (a `pom.xml`)   | do not copy jars: run `make install` (it rewrites the `.cp`) |

Then stop the running agent; datalake_proxy starts the new one within
`datalake_proxy.restart_time` (30 s for REST, 5 s for gRPC):

```bash
pkill -f 'cloud.elastic.dlagent'     # both agents' main classes live here
```

## Running an agent by hand

From this directory after `mvn package -DskipTests`; the parent pid must be
a live process whose command line contains `datalake proxy` (the agent exits
otherwise):

```bash
# REST, port 8080
java -cp "dlagent-rest/target/dlagent-rest-1.0.0.jar:$(cat dlagent-rest/target/classpath-src.txt)" \
     cloud.elastic.dlagent.service.ServiceApplication --parent.pid=<pid> --server.port=8080

# gRPC, port 3889
java -cp "dlagent-grpc/target/dlagent-grpc-1.0.0.jar:$(cat dlagent-grpc/target/classpath-src.txt)" \
     cloud.elastic.dlagent.grpc.Main --parent.pid=<pid> --dlagent.grpc.port=3889
```

`test/start-with-mock-parent.sh` does the REST case with a mock parent.
