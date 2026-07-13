#!/usr/bin/env bash
# Managed start/stop/status for the datalake_rest_catalog REST gateway.
# Runs the installed uber-jar under JDK11+ (the jar targets release 11, so any
# JDK >= 11 works). Point DRC_JAVA_HOME at a JDK>=11 (default /opt/jdk17).
# Non-secret config comes from the jar's bundled gateway.properties; secrets
# (S3 creds) are sourced from an env file (see deploy/datalake_rest_catalog.env.example).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CMD="${1:-}"

# DRC_JAVA_HOME (falls back to legacy JAVA17_HOME, then /opt/jdk17) — any JDK >= 11.
DRC_JAVA_HOME="${DRC_JAVA_HOME:-${JAVA17_HOME:-/opt/jdk17}}"
JAVA="$DRC_JAVA_HOME/bin/java"
SERVER_PORT="${SERVER_PORT:-8181}"
# HTTPS defaults on in gateway.properties (issue #382 production readiness); the health check
# below needs to know which scheme/port to probe. SERVER_HTTPS_ENABLED mirrors the
# server.https.enabled config key (via its env override) so operators overriding the jar's
# default from the env file also flip the health check consistently.
SERVER_HTTPS_ENABLED="${SERVER_HTTPS_ENABLED:-true}"
SERVER_HTTPS_PORT="${SERVER_HTTPS_PORT:-8443}"
RUNDIR="${DRC_RUNDIR:-/tmp}"
PIDFILE="$RUNDIR/datalake_rest_catalog.pid"
LOGFILE="$RUNDIR/datalake_rest_catalog.log"
ENV_FILE="${DRC_ENV:-$SCRIPT_DIR/../deploy/datalake_rest_catalog.env}"

resolve_jar() {
  if [ -n "${DRC_JAR:-}" ]; then echo "$DRC_JAR"; return; fi
  local d; d="$(pg_config --pkglibdir 2>/dev/null || true)"
  if [ -n "$d" ] && [ -f "$d/java/datalake-rest-catalog-1.0.0.jar" ]; then
    echo "$d/java/datalake-rest-catalog-1.0.0.jar"
  fi
}

is_running() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

# Health-check URL: honors SERVER_HTTPS_ENABLED/SERVER_HTTPS_PORT so the check hits the scheme
# the gateway actually serves (default on, self-signed dev cert -> curl -k to skip verification).
health_check() {
  if [ "$SERVER_HTTPS_ENABLED" = "true" ]; then
    curl -sfk "https://localhost:$SERVER_HTTPS_PORT/v1/config" >/dev/null 2>&1
  else
    curl -sf "http://localhost:$SERVER_PORT/v1/config" >/dev/null 2>&1
  fi
}

start() {
  if is_running; then echo "already running (pid $(cat "$PIDFILE"))"; return 0; fi
  [ -x "$JAVA" ] || { echo "ERROR: java not found at $JAVA (set DRC_JAVA_HOME to a JDK>=11)"; exit 1; }
  # Require Java >= 11 (jar is release 11). Parse major from `java -version`:
  # "17.0.13" -> 17, "11.0.24" -> 11, "1.8.0_452" -> 8 (rejected).
  ver_line="$("$JAVA" -version 2>&1 | head -1)"
  ver_num="$(printf '%s' "$ver_line" | sed -E 's/.*version "([0-9]+)(\.([0-9]+))?.*/\1 \3/')"
  major="$(echo "$ver_num" | awk '{ if ($1 == 1) print $2; else print $1 }')"
  if [ -z "${major:-}" ] || [ "$major" -lt 11 ] 2>/dev/null; then
    echo "ERROR: Java >= 11 required, but $JAVA is: $ver_line"; exit 1
  fi
  # shellcheck disable=SC1090
  [ -f "$ENV_FILE" ] && . "$ENV_FILE"
  if [ -z "${S3_ACCESS_KEY_ID:-}" ] || [ -z "${S3_SECRET_ACCESS_KEY:-}" ]; then
    echo "ERROR: S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY not set."
    echo "       Create $ENV_FILE from deploy/datalake_rest_catalog.env.example."
    exit 1
  fi
  export S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY
  local jar; jar="$(resolve_jar)"
  [ -n "$jar" ] || { echo "ERROR: jar not found; run 'make install' or set DRC_JAR"; exit 1; }
  local scheme="http"; local checkport="$SERVER_PORT"
  if [ "$SERVER_HTTPS_ENABLED" = "true" ]; then scheme="https"; checkport="$SERVER_HTTPS_PORT"; fi
  echo "starting: $JAVA -jar $jar ($scheme port $checkport)"
  nohup "$JAVA" -jar "$jar" >"$LOGFILE" 2>&1 &
  echo $! > "$PIDFILE"
  local i
  for i in $(seq 1 30); do
    if health_check; then
      echo "started (pid $(cat "$PIDFILE"))"; return 0
    fi
    if ! is_running; then echo "ERROR: process died on startup; last log:"; tail -20 "$LOGFILE"; rm -f "$PIDFILE"; exit 1; fi
    sleep 1
  done
  echo "ERROR: not ready after 30s; last log:"; tail -20 "$LOGFILE"; exit 1
}

stop() {
  if ! is_running; then echo "not running"; rm -f "$PIDFILE" 2>/dev/null || true; return 0; fi
  local pid i; pid="$(cat "$PIDFILE")"
  echo "stopping pid $pid"; kill "$pid" 2>/dev/null || true
  for i in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  if kill -0 "$pid" 2>/dev/null; then echo "force kill"; kill -9 "$pid" 2>/dev/null || true; fi
  rm -f "$PIDFILE"; echo "stopped"
}

status() {
  if is_running; then
    echo "running (pid $(cat "$PIDFILE"))"
    if health_check; then echo "  /v1/config: OK"; else echo "  /v1/config: NOT responding"; fi
  else
    echo "stopped"
  fi
}

case "$CMD" in
  start)   start ;;
  stop)    stop ;;
  status)  status ;;
  restart) stop; start ;;
  *) echo "usage: $0 {start|stop|status|restart}"; exit 2 ;;
esac
