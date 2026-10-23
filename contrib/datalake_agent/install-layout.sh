#!/bin/sh
#
# Lay out pkglib/java as core/ + rest/ + grpc/, and emit the ordered classpath
# each transport is launched with.
#
# WHY A SCRIPT AND NOT MAKE RULES.  Two things here need set arithmetic and
# per-file decisions that make expresses badly: a dependency belongs to core/
# if core needs it (so it is stored once rather than in both transports), and
# each transport's classpath must list its jars IN MAVEN'S ORDER, wherever they
# ended up.  Both are three lines of shell and a page of make.
#
# Usage: install-layout.sh <install-dir>
set -eu

DEST="$1"
SRC="$(cd "$(dirname "$0")" && pwd)"
VERSION="${DLAGENT_VERSION:-1.0.0}"

CORE_DIR="$DEST/core"
REST_DIR="$DEST/rest"
GRPC_DIR="$DEST/grpc"

mkdir -p "$CORE_DIR" "$REST_DIR" "$GRPC_DIR"

core_jar="dlagent-core-$VERSION.jar"

#
# WHERE THE JARS COME FROM.
#
# classpath-src.txt is maven's own answer to "which files are this module's
# dependencies, right now": one line, ':'-separated, absolute, every entry an
# existing file.  Copying straight from it avoids a copy-dependencies step
# that would first duplicate the whole dependency tree into each module's
# target/deps -- four modules, four copies, 4.1 GB, and a `database` cache the
# object store refuses with 400 Bad Request.  See the parent pom's
# emit-source-classpath execution.
#
# Reading the list rather than globbing a directory also removes a way to be
# quietly wrong: a jar that is on the classpath but was not copied would
# leave a dangling .cp entry, and one that was copied without being on the
# classpath would be installed and never loaded.  With one list, neither can
# happen.
#
#
# WHERE EACH JAR COMES FROM, AND WHAT IT IS CALLED WHEN IT GETS HERE.
#
# Two files, written by maven from the same dependency list in the same order:
#
#   classpath.txt      the CONTRACT: "__DLAGENT_LIB__/<name>", the names the
#                      installed .cp files and the C side will use.
#   classpath-src.txt  the SOURCE:  absolute paths to those jars right now.
#
# They are walked in lockstep, so each jar is copied from where it is to the
# name the contract expects. The two are NOT interchangeable, and one pair
# shows why: jdk.tools:1.8 and com.sun:tools:1.8.0 are both system-scoped and
# both resolve to the one $JAVA_HOME/lib/tools.jar, so the source list says
# "tools.jar" twice while the contract says "jdk.tools-1.8.jar" and
# "tools-1.8.0.jar". Copying by source basename would install one file under a
# name the .cp files never mention and leave two entries pointing at nothing.
#
# This replaced a copy-dependencies execution that first duplicated every
# module's whole dependency tree into its own target/deps; inherited by four
# modules it wrote 4.1 GB, and the pipeline's `database` cache stopped fitting
# in one PUT (400 Bad Request), which left every test job without a source
# tree. Nothing needed the copies: this is the same list, read instead of
# materialised.
#
LIST="$(mktemp)"
NAMES="$(mktemp)"
PATHS="$(mktemp)"
trap 'rm -f "$LIST" "$LIST.fixed" "$NAMES" "$PATHS"' EXIT INT TERM

# `build-classpath` writes ONE line with no trailing newline, and `read` drops
# a final line that has none -- silently, so the loss is the last dependency in
# maven's order and nothing says so. Measured: lombok went missing from core/
# exactly this way. The `echo` terminates it; the sed drops what that adds.
split_cp() {
    { tr ':' '\n' < "$1"; echo; } | sed '/^[[:space:]]*$/d' > "$2"
}

# Emits "<installed name><tab><source path>" for one module.
src_list() {
    module="$1"
    names_file="$SRC/dlagent-$module/target/classpath.txt"
    paths_file="$SRC/dlagent-$module/target/classpath-src.txt"

    for f in "$names_file" "$paths_file"; do
        if [ ! -s "$f" ]; then
            echo "install-layout.sh: missing or empty $f" >&2
            echo "  (run 'mvn package' first; the install layout is built from it)" >&2
            exit 1
        fi
    done

    split_cp "$names_file" "$NAMES"
    sed -i 's#.*/##' "$NAMES"
    split_cp "$paths_file" "$PATHS"

    n_names=$(wc -l < "$NAMES")
    n_paths=$(wc -l < "$PATHS")
    if [ "$n_names" != "$n_paths" ]; then
        echo "install-layout.sh: $module lists $n_names names but $n_paths paths;" >&2
        echo "  the two files are supposed to be one list rendered twice" >&2
        exit 1
    fi

    paste "$NAMES" "$PATHS" > "$LIST"

    #
    # A SIBLING MODULE IS NOT ALWAYS A JAR YET.
    #
    # Maven resolves a reactor dependency to whatever exists in the session
    # that asks: `mvn package` has already built dlagent-core, so grpc's list
    # names its jar; `mvn test` has not, so the same list names the directory
    #
    #   .../dlagent-core/target/classes
    #
    # and `make check` runs `mvn -B test` right after `make install`, which
    # rewrites both files in that form. The contract file still says
    # "dlagent-core-1.0.0.jar" -- it renders artifacts by name -- so the jar
    # we want is that name in the same target directory. Resolving it here
    # keeps the installed layout the same whichever command wrote the list.
    #
    : > "$LIST.fixed"
    while IFS="$(printf '\t')" read -r base src; do
        if [ -d "$src" ]; then
            src="$(dirname "$src")/$base"
        fi
        if [ ! -f "$src" ]; then
            echo "install-layout.sh: $module needs a file that is not there:" >&2
            echo "  $src  (listed as $base)" >&2
            echo "  run 'mvn package' first; a bare 'mvn test' does not build it" >&2
            exit 1
        fi
        printf '%s\t%s\n' "$base" "$src" >> "$LIST.fixed"
    done < "$LIST"
    mv -f "$LIST.fixed" "$LIST"
}

# ---- core/: everything dlagent-core needs, plus dlagent-core itself --------
cp -f "$SRC/dlagent-core/target/$core_jar" "$CORE_DIR/"
src_list core
while IFS="$(printf '\t')" read -r base src; do
    cp -f "$src" "$CORE_DIR/$base"
done < "$LIST"

# ---- rest/ and grpc/: only what core does not already provide -------------
# Stored once is what keeps the installed size near the old single fat jar
# rather than double it.
for pair in "rest:$REST_DIR" "grpc:$GRPC_DIR"; do
    name="${pair%%:*}"
    dir="${pair#*:}"

    cp -f "$SRC/dlagent-$name/target/dlagent-$name-$VERSION.jar" "$dir/"
    src_list "$name"
    while IFS="$(printf '\t')" read -r base src; do
        # Already in core/, including dlagent-core itself.
        [ -e "$CORE_DIR/$base" ] && continue
        cp -f "$src" "$dir/$base"
    done < "$LIST"
done

# ---- the ordered classpath files -----------------------------------------
# Maven wrote each module's dependencies in resolution order with a placeholder
# prefix; rewrite the placeholder to wherever each jar actually landed.
#
# Entries are RELATIVE to $DEST (core/x.jar, rest/y.jar) and datalake_proxy
# prefixes its own pkglib/java at launch.  Absolute paths would pin the agent
# to the install prefix: a relocated install (CI runs dist/ from a different
# directory than it was built into, and so can a package) then hands java a
# classpath of files that do not exist, the agent exits at once, and every
# catalog call fails with "Could not connect to server".  The pre-split
# `-jar <pkglib>/java/dlagent-1.0.0.jar` never had that property.  The
# ORDER is the point: `bundle` 2.35.4 and `s3` 2.20.26 share 98.5% of their
# class names, so which one wins is decided by this list and must not be left
# to a wildcard's unspecified expansion: the REST path's behaviour must not
# change.
emit_classpath() {
    module="$1"
    own_dir="$2"
    out="$3"
    src_cp="$SRC/dlagent-$module/target/classpath.txt"

    own_rel="$(basename "$own_dir")"
    core_rel="$(basename "$CORE_DIR")"

    : > "$out"
    # The module's own jar first, as in a fat jar where its classes shadowed
    # any duplicate from a dependency.
    printf '%s/dlagent-%s-%s.jar' "$own_rel" "$module" "$VERSION" >> "$out"
    # The trailing echo: maven writes classpath.txt without a final newline,
    # and read drops an unterminated last line -- the last dependency would
    # silently fall off the classpath.  split_cp does the same.
    { tr ':' '\n' < "$src_cp"; echo; } | while IFS= read -r entry; do
        [ -n "$entry" ] || continue
        base="$(basename "$entry")"
        if [ -e "$CORE_DIR/$base" ]; then
            printf ':%s/%s' "$core_rel" "$base" >> "$out"
        else
            printf ':%s/%s' "$own_rel" "$base" >> "$out"
        fi
    done
    printf '\n' >> "$out"
}

emit_classpath core "$CORE_DIR" "$DEST/core.cp"
emit_classpath rest "$REST_DIR" "$DEST/rest.cp"
emit_classpath grpc "$GRPC_DIR" "$DEST/grpc.cp"

echo "installed dlagent layout under $DEST"
