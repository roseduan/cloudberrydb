#!/bin/bash
#
# Differential test: the C site-config parser against the agent's
# SiteConfigLoader, which is the oracle.
#
# Both sides read the same fixture files and dump what they made of them;
# this script compares the dumps.  A disagreement means the database and the
# agent would read one deployed conf file differently -- the failure mode
# this whole design exists to prevent -- so it fails the run.
#
#   ./run.sh                 both halves, then compare
#   ./run.sh --java-only     regenerate the oracle dumps
#   ./run.sh --c-only        regenerate the database dumps
#
# The C half needs a running cluster with datalake_fdw installed; the Java
# half needs maven.  PGDATABASE/PGPORT etc. are honoured as usual.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES="$HERE/fixtures"
# Out of tree by default: the dumps are build output, and the source tree is
# not always writable by whoever runs this (a container user, say).
OUT="${SITEDIFF_OUT:-${TMPDIR:-/tmp}/sitediff-$(id -u)}"
AGENT_DIR="$(cd "$HERE/../../../datalake_agent" && pwd)"

run_java=yes
run_c=yes
case "${1:-}" in
	--java-only) run_c=no ;;
	--c-only)    run_java=no ;;
	"")          ;;
	*)           echo "unknown option: $1" >&2; exit 2 ;;
esac

mkdir -p "$OUT/java" "$OUT/c"

# A SQL string literal, empty input included -- an empty case field is a real
# case here (no server name at all), not an oversight.
sqlquote() {
	printf "'%s'" "$(printf '%s' "$1" | sed "s/'/''/g")"
}

if [ "$run_java" = yes ]; then
	echo "== dumping with the agent's SiteConfigLoader =="
	# SITEDIFF_MVN_ARGS is where "-o" goes when the build host cannot reach
	# the internal nexus.
	(cd "$AGENT_DIR" && mvn -q ${SITEDIFF_MVN_ARGS:-} -Dtest=SiteConfigDumpTest \
		-DfailIfNoTests=false \
		-Dsitediff.fixtures="$FIXTURES" \
		-Dsitediff.out="$OUT/java" test) || {
		echo "the Java dumper failed to run" >&2
		exit 1
	}
fi

if [ "$run_c" = yes ]; then
	echo "== dumping with pg_iceberg_site_config() =="
	while IFS=$'\t' read -r name dir server type expect; do
		case "$name" in ''|\#*) continue ;; esac
		[ "$server" = "-" ] && server=""
		[ "$type" = "-" ] && type=""

		# psql prints the result on stdout and any ERROR on stderr; keep them
		# apart so a failure is never mistaken for a value.
		if out=$(psql -X -At -v ON_ERROR_STOP=1 \
					-c "SELECT pg_catalog.pg_iceberg_site_config($(sqlquote "$server"), $(sqlquote "$type"), $(sqlquote "$FIXTURES/$dir"))" \
					2>"$OUT/c/$name.err"); then
			printf '%s\n' "$out" > "$OUT/c/$name.json"
		else
			# A SQL error arrives as "ERROR:  <text>" and is a legitimate
			# result: several cases expect both halves to reject the fixture.
			#
			# Anything else -- a refused connection, a database that does not
			# exist, a missing extension -- means this half never executed, and
			# recording it as an ERROR would satisfy those same error_category
			# cases: a C half that could not run once reported "8 passed, 25
			# failed" instead of failing outright, which is the one outcome a
			# differential test must never produce.  Stop the run and say why.
			msg=$(sed -n 's/^ERROR:  *//p' "$OUT/c/$name.err" | head -1)
			if [ -z "$msg" ]; then
				echo "the database half could not run case \"$name\":" >&2
				sed 's/^/    /' "$OUT/c/$name.err" >&2
				echo "not comparing: this is a broken run, not a disagreement" >&2
				exit 1
			fi
			printf 'ERROR\t%s\n' "$msg" > "$OUT/c/$name.json"
		fi
		rm -f "$OUT/c/$name.err"
	done < "$FIXTURES/cases.tsv"
fi

if [ "$run_java" = no ] || [ "$run_c" = no ]; then
	echo "one half only; skipping comparison"
	exit 0
fi

echo "== comparing =="
pass=0
fail=0
while IFS=$'\t' read -r name dir server type expect; do
	case "$name" in ''|\#*) continue ;; esac

	j="$OUT/java/$name.json"
	c="$OUT/c/$name.json"
	if [ ! -f "$j" ] || [ ! -f "$c" ]; then
		echo "FAIL $name: missing dump"
		fail=$((fail + 1))
		continue
	fi

	j_err=no; c_err=no
	grep -q '^ERROR	' "$j" && j_err=yes
	grep -q '^ERROR	' "$c" && c_err=yes

	case "$expect" in
	same)
		if cmp -s "$j" "$c"; then
			pass=$((pass + 1))
		else
			echo "FAIL $name:"
			diff -u "$j" "$c" | sed 's/^/    /'
			fail=$((fail + 1))
		fi
		;;
	error_category)
		if [ "$j_err" = yes ] && [ "$c_err" = yes ]; then
			pass=$((pass + 1))
		else
			echo "FAIL $name: expected both sides to fail (java=$j_err c=$c_err)"
			fail=$((fail + 1))
		fi
		;;
	c_rejects)
		if [ "$j_err" = no ] && [ "$c_err" = yes ]; then
			pass=$((pass + 1))
		else
			echo "FAIL $name: expected the agent to accept and C to reject" \
				 "(java_error=$j_err c_error=$c_err)"
			echo "    a change here means a declared divergence moved;" \
				 "update W1-equivalence-spec.md 8.4/8.6 before touching the code"
			fail=$((fail + 1))
		fi
		;;
	*)
		echo "FAIL $name: unknown expectation \"$expect\""
		fail=$((fail + 1))
		;;
	esac
done < "$FIXTURES/cases.tsv"

echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
