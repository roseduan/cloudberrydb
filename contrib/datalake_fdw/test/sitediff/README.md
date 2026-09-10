# sitediff — the site-config parser against the agent

`iceberg_site_config.c` reads `s3.conf` / `gphdfs.conf` on the coordinator so
the QEs can be handed a volume's configuration instead of being unable to see
it. The agent has always read those same files with its own parser. Two
parsers over one file is the arrangement this test exists to police: if they
ever disagree, a deployment reads one endpoint and writes to another, and
nothing in the system notices.

So the agent's `SiteConfigLoader` is the **oracle**. Both sides dump what they
made of the same fixtures and the dumps are compared. When they differ, the
agent is right and the C parser is the bug — unless the difference is one of
the ones declared below.

## Running it

```bash
cd contrib/datalake_fdw/test/sitediff
./run.sh                # both halves, then compare
./run.sh --c-only       # database half only
./run.sh --java-only    # oracle half only
```

Dumps land in `$TMPDIR/sitediff-$(id -u)/{java,c}` (override with
`SITEDIFF_OUT`), out of the source tree.

**The C half** needs a running cluster with datalake_fdw installed; it talks
to it through `psql`, so `PGPORT`/`PGDATABASE`/etc. apply as usual.

**The Java half** needs maven and the internal nexus, and one workaround.
`com.cbdb.gopherClient:gopherClient:4.0.29` is published with the wrong POM:
the file under those coordinates is the POM of an unrelated test project
(`gopherClientTest:1.0-SNAPSHOT`) and declares a dependency on
`gopherClient:1.0-SNAPSHOT`, which exists nowhere. Maven then looks for that
snapshot on oss.sonatype.org and the build dies on a connect timeout, naming a
version no file in the tree asks for. Until the artifact is republished, supply
the missing coordinate from the jar already shipped in thirdparty:

```bash
mvn install:install-file -Dmaven.repo.local=<repo> \
    -Dfile=$GPHOME/../thirdparty/share/java/gopherClient.jar \
    -DgroupId=com.cbdb.gopherClient -DartifactId=gopherClient \
    -Dversion=1.0-SNAPSHOT -Dpackaging=jar
SITEDIFF_MVN_ARGS='-Dmaven.repo.local=<repo>' ./run.sh
```

Both halves must run as the same user, since only one of them compares: the
Java half writes into the agent's `target/`, and the C half connects through
`psql`, so a root-built `target/` or a `pg_hba` that does not know root will
stop the other half. Where nexus is reachable but slow, put `-o` in
`SITEDIFF_MVN_ARGS` after a first successful build.

## What a case is

`fixtures/cases.tsv` — one row per case: name, fixture directory, server name,
volume type, expectation. A field written `-` is the empty string. The
fixture directories hold ordinary `s3.conf` / `gphdfs.conf` files; several
cases usually share one file, each naming a different section.

Expectations:

| | meaning |
|---|---|
| `same` | the dumps must be identical, byte for byte — a value or an error message |
| `error_category` | both must fail, but only the fact is contractual: Java's text is a wrapped JVM exception (`FileNotFoundException`, `ClassCastException`) that C cannot and should not reproduce |
| `c_rejects` | a **declared divergence**: the agent accepts the input, the C parser refuses it |

## The declared divergences

Four shapes are rejected here that the agent accepts. Each one's Java
rendering depends on something that cannot be pinned down as a wire format:

| shape | what Java produces | why not reproduce it |
|---|---|---|
| float (`1.50`, `1e3`, `+.5`, `.5e5`) | `Double.toString()` | shortest-round-trip decimal |
| timestamp (`2020-01-01`) | `Date.toString()` | the JVM's default timezone and locale |
| sexagesimal (`1:30`) | base-60 sum | computed in a 32-bit accumulator inside SnakeYAML |
| nested value (`{a: b}`) | `Map.toString()` | reproducible, but meaningless as a config value |

None of them is a legitimate endpoint, bucket, port or credential, and
quoting the value makes both sides agree exactly. Rejecting is fail-closed:
it cannot produce the silent misconfiguration this test is here to prevent.

The float row lists four spellings because only the first is obvious. The
FLOAT pattern in snakeyaml 2.2 — the jar the agent pins, and the constant
pool is readable with `unzip -p … Resolver.class` — carries an
exponent-without-a-point branch and a signed leading-point branch, so `1e3`,
`+.5` and `.5e5` are floats too: `1000.0`, `0.5` and `50000.0` after
`Double.toString()`. The older PyYAML-style pattern has neither branch, and
read from it the C resolver looks like it over-matches; narrowing it to match
would turn a rejection into a silent disagreement, which is why those three
have fixtures of their own.

**If a `c_rejects` case starts passing or failing differently, that is a
contract change**, not a test to fix: update
`.router/plans/410/W1-equivalence-spec.md` §8.4 / §8.6 first.

## Why the fixtures look strange

They are not realistic configurations; they are the places the two parsers
could drift. `01234567` is there because SnakeYAML reads it as octal and
rewrites a credential to `342391`. `path_style_access: "yes"` is there
because the quotes make it *false* while bare `yes` is *true*. Duplicate
sections are there because the agent takes the last and the older C reader in
`provider/common/config.c` takes the first. Each one is a documented line in
the equivalence spec.

## Measuring coverage of the parser

The pre-push coverage gate counts every new C line, including this suite's own
test sources, and it reads a single lcov tracefile. Neither half of the parser
is covered by a normal regression run, so the tracefile has to be assembled
from the two places the code actually executes: `iceberg_site_config.c` inside
a backend, and `iceberg_site_scalar.c` inside the standalone unit test.

```bash
# 1. Instrumented extension, installed, and a cluster restarted onto it --
#    the postmaster holds the old mapping otherwise and would keep running
#    the previous build with no warning at all.
make -C ../.. COPT='--coverage -O0' && make -C ../.. COPT='--coverage -O0' install
gpstop -ar -a

# 2. The C half of this suite: 36 fixtures through pg_iceberg_site_config().
./run.sh --c-only

# 3. The unit test, instrumented, which is what covers the scalar layer.
make -C ../unit CFLAGS='-Wall -Wextra -Werror -O0 -g --coverage' check

# 4. One tracefile out of both.
lcov --capture --directory ../.. -o /tmp/backend.info
lcov --capture --directory ../unit -o /tmp/unit.info
lcov -a /tmp/backend.info -a /tmp/unit.info -o /tmp/sitediff.info
COVERAGE_INFO=/tmp/sitediff.info git push
```

Step 3 is not optional bookkeeping: the scalar layer sits at 59% on the
fixtures alone and at 92% once the unit test is counted, because the fixtures
exercise the spellings a real conf file has while the unit test exercises the
ones YAML 1.1 permits.
