#!/usr/bin/env bash
#
# coverage.sh — Code coverage tool for the database project.
#
# Subcommands:
#   capture   Capture coverage from .gcda files into a .info file
#   report    Generate diff coverage report for changed code
#
# Both subcommands run lcov/diff-cover inside Docker on macOS.
# Inside a CI container (/workspace layout detected), they run directly.
#
# ── Full workflow (local developer) ─────────────────────────────────
#
#   Step 1. Build with coverage enabled (inside the umbrella repo):
#
#       DATABASE_CONFIG_EXTRA_OPTIONS="--enable-coverage" make build-database
#
#           Or, if you only have the database repo, run Docker manually:
#
#       docker run --rm -t --platform linux/$(uname -m) \
#           -e DATABASE_CONFIG_EXTRA_OPTIONS="--enable-coverage" \
#           -v $(pwd):/workspace/database \
#           docker.hashdata.dev/hashdata-releng/centos-7:devel-cbdb-20260319 \
#           /bin/bash -lc "/workspace/scripts/devel/build-database.sh"
#
#   Step 2. Run the tests you care about:
#
#       MAKE_TEST_COMMAND="-s -k PGOPTIONS='-c optimizer=off' installcheck" \
#           make test-database
#
#   Step 3. Run this script to see diff coverage:
#
#       ./coverage.sh report              # compare against main
#       ./coverage.sh report dev-branch   # compare against a branch
#       ./coverage.sh report HEAD~3       # compare against a relative ref
#       ./coverage.sh report a1b2c3d      # compare against a specific commit SHA
#
# ── Capture subcommand ────────────────────────────────────────────────
#
#   ./coverage.sh capture [-o FILE]
#
#   Captures coverage from .gcda files into a single .info trace file.
#   Default output: build/coverage/coverage.info (relative to workspace)
#
# ── Report subcommand ─────────────────────────────────────────────────
#
#   ./coverage.sh report [target-branch-or-commit]
#
#   In local mode:  captures from .gcda, then generates report
#   In CI mode:     merges existing .info files, then generates report
#
#   Feature branch → diffs against merge-base with target branch
#   Main branch    → diffs against HEAD~1 (your last commit)
#
#   Uncommitted changes are not included in the diff — commit first
#   if you want them checked.
#
# ── Environment variables ─────────────────────────────────────────────
#
#   IMAGE   Docker image   (default: docker.hashdata.dev/...devel-cbdb-20260319)
#   ARCH    Architecture   (default: auto-detected via uname -m)

set -euo pipefail

DATABASE_DIR="$(cd "$(dirname "$0")" && pwd)"
GCOV_TOOL="/usr/local/toolchain/bin/gcov"
# lcov 2.x: duplicating "inconsistent" suppresses both errors AND warnings
# (single suppresses errors only; needed for conditional-compilation duplicates)
LCOV_IGNORE="--ignore-errors negative,negative,inconsistent,inconsistent,corrupt,corrupt,range,range"

# CI: build/ is at umbrella level (sibling of database/)
# Local: build/ is inside database/ (developer may not have umbrella)
if [ "${CI:-}" = "true" ]; then
    COVERAGE_DIR="$(dirname "$DATABASE_DIR")/build/coverage"
else
    COVERAGE_DIR="${DATABASE_DIR}/build/coverage"
fi

# ── Helpers ───────────────────────────────────────────────────────────

usage() {
    local in_help=false
    while IFS= read -r line; do
        case "$line" in
            "# ── Full workflow"*) in_help=true ;;
            "set -euo pipefail")  break ;;
        esac
        if $in_help; then
            line="${line#\#}"
            echo "${line# }"
        fi
    done < "$0"
}

is_ci() {
    [ "${CI:-}" = "true" ]
}

init_docker() {
    ARCH="${ARCH:-$(uname -m)}"
    IMAGE="${IMAGE:-docker.hashdata.dev/hashdata-releng/centos-7:devel-cbdb-20260319}"

    # Detect whether database/ is a standalone repo or a submodule.
    # This determines which volumes we mount so git works inside Docker.
    if [ -d "${DATABASE_DIR}/.git" ]; then
        GIT_LAYOUT="standalone"
    elif [ -f "${DATABASE_DIR}/.git" ]; then
        GIT_LAYOUT="submodule"
        UMBRELLA_DIR="$(cd "${DATABASE_DIR}" && git rev-parse --show-superproject-working-tree)"
    else
        echo "ERROR: ${DATABASE_DIR} is not a git repository"
        exit 1
    fi
}

# Run a command inside the dev container
docker_run() {
    local volumes=(-v "${COVERAGE_DIR}:/workspace/build/coverage")
    if [ "$GIT_LAYOUT" = "submodule" ]; then
        # Mount the umbrella root so submodule .git references resolve
        volumes+=(-v "${UMBRELLA_DIR}:/workspace")
    else
        volumes+=(-v "${DATABASE_DIR}:/workspace/database")
    fi

    docker run --rm --platform "linux/${ARCH}" \
        "${volumes[@]}" \
        "$IMAGE" /bin/bash -lc "$1"
}

# ── Capture ───────────────────────────────────────────────────────────

cmd_capture() {
    local output="build/coverage/coverage.info"

    while [ $# -gt 0 ]; do
        case "$1" in
            -o|--output) output="$2"; shift 2 ;;
            *) echo "Unknown option: $1"; exit 1 ;;
        esac
    done

    init_docker

    local gcda_count
    gcda_count=$(find "$DATABASE_DIR" -name '*.gcda' 2>/dev/null | wc -l | tr -d ' ')
    if [ "$gcda_count" -eq 0 ]; then
        echo "WARNING: No .gcda files found, skipping capture"
        return 0
    fi
    echo "Found $gcda_count .gcda files"

    mkdir -p "$COVERAGE_DIR" && chmod 777 "$COVERAGE_DIR"

    echo "=== Capturing coverage data ==="
    docker_run "
        lcov --capture --directory /workspace/database \
            --output-file /workspace/${output} \
            --gcov-tool ${GCOV_TOOL} \
            --no-external ${LCOV_IGNORE} --quiet \
        || echo 'Warning: No coverage data found'
    "
    echo "Written to ${COVERAGE_DIR}/$(basename "$output")"
}

# ── Report ────────────────────────────────────────────────────────────

check_uncommitted() {
    cd "$DATABASE_DIR"
    if ! git diff --quiet HEAD 2>/dev/null || ! git diff --cached --quiet 2>/dev/null; then
        echo "NOTE: You have uncommitted changes. Only committed changes will be included in the diff coverage report."
        echo ""
    fi
}

collect_coverage() {
    local coverage_info="${COVERAGE_DIR}/coverage.info"
    mkdir -p "$COVERAGE_DIR" && chmod 777 "$COVERAGE_DIR"

    if is_ci; then
        local info_count
        info_count=$(find "$COVERAGE_DIR" -name '*.info' -not -name 'coverage.info' 2>/dev/null | wc -l | tr -d ' ')
        if [ "$info_count" -eq 0 ]; then
            echo "No coverage .info files found in $COVERAGE_DIR"
            exit 0
        fi
        echo "Found $info_count .info files to merge"

        echo "=== Merging coverage trace files ==="
        local merge_args=""
        for f in "${COVERAGE_DIR}"/*.info; do
            if [ -f "$f" ] && [ "$f" != "$coverage_info" ]; then
                local basename
                basename=$(basename "$f")
                merge_args="$merge_args -a /workspace/build/coverage/$basename"
            fi
        done
        docker_run "lcov $merge_args -o /workspace/build/coverage/coverage.info $LCOV_IGNORE --quiet"
    else
        local gcda_count
        gcda_count=$(find "$DATABASE_DIR" -name '*.gcda' 2>/dev/null | wc -l | tr -d ' ')
        if [ "$gcda_count" -eq 0 ]; then
            echo "ERROR: No .gcda files found in $DATABASE_DIR"
            echo ""
            echo "Make sure you have:"
            echo "  1. Built with --enable-coverage inside the dev container"
            echo "  2. Run the tests you want coverage for"
            exit 1
        fi
        echo "Found $gcda_count .gcda files"

        echo "=== Capturing coverage data ==="
        docker_run "
            lcov --capture --directory /workspace/database \
                --output-file /workspace/build/coverage/coverage.info \
                --gcov-tool ${GCOV_TOOL} \
                --no-external ${LCOV_IGNORE} --quiet
        "
    fi
    echo "Coverage data written to $coverage_info"
}

generate_reports() {
    echo ""
    echo "=== Generating HTML report ==="
    docker_run "
        mkdir -p /workspace/build/coverage/report &&
        genhtml /workspace/build/coverage/coverage.info \
            -o /workspace/build/coverage/report \
            ${LCOV_IGNORE} --quiet
    "

    echo ""
    echo "=== Overall Coverage Summary ==="
    docker_run "lcov --summary /workspace/build/coverage/coverage.info ${LCOV_IGNORE}" 2>&1

    echo ""
    echo "=== Generating Cobertura XML ==="
    docker_run "
        lcov_cobertura /workspace/build/coverage/coverage.info \
            --base-dir /workspace/database \
            -o /workspace/build/coverage/cobertura.xml
    "
}

determine_compare_ref() {
    local target_branch="$1"
    cd "$DATABASE_DIR"

    local current_branch
    current_branch=$(git rev-parse --abbrev-ref HEAD)

    echo ""
    echo "=== Diff Coverage Setup ==="
    echo "Current branch : $current_branch"
    echo "Target branch  : $target_branch"

    if [ -n "${CI_MERGE_REQUEST_DIFF_BASE_SHA:-}" ]; then
        # CI MR pipeline: use the exact merge-base SHA from GitLab.
        # This is stable even if the target branch has new commits.
        echo "Scenario       : CI merge request (using DIFF_BASE_SHA)"
        git fetch origin "$CI_MERGE_REQUEST_DIFF_BASE_SHA" --depth=1 2>/dev/null || true
        COMPARE_REF="$CI_MERGE_REQUEST_DIFF_BASE_SHA"
    elif [ "$current_branch" = "$target_branch" ]; then
        echo "Scenario       : committed changes on $target_branch"
        COMPARE_REF="HEAD~1"
    else
        # Local feature branch: compute merge-base ourselves
        if ! git rev-parse --verify "origin/${target_branch}" &>/dev/null; then
            echo "Fetching origin/${target_branch}..."
            git fetch origin "${target_branch}:refs/remotes/origin/${target_branch}" --depth=50 || true
        fi

        local merge_base
        merge_base=$(git merge-base "origin/${target_branch}" HEAD 2>/dev/null || echo "")

        if [ -z "$merge_base" ]; then
            echo "WARNING: Cannot find merge-base with origin/${target_branch}, falling back to HEAD~1"
            merge_base="HEAD~1"
        fi

        echo "Scenario       : feature branch"
        COMPARE_REF="$merge_base"
    fi

    echo "Compare ref    : $COMPARE_REF"
}

run_diff_cover() {
    echo ""
    echo "=== Diff Coverage (changed lines vs ${COMPARE_REF}) ==="

    local exit_code=0
    docker_run "
        cd /workspace/database &&
        diff-cover /workspace/build/coverage/cobertura.xml \
            --compare-branch='${COMPARE_REF}' \
            --ignore-staged --ignore-unstaged \
            --html-report /workspace/build/coverage/diff-cover.html \
            --fail-under=80
    " || exit_code=$?

    echo ""
    echo "=== Reports ==="
    echo "  HTML report   : ${COVERAGE_DIR}/report/"
    echo "  Diff coverage : ${COVERAGE_DIR}/diff-cover.html"
    echo "  Cobertura XML : ${COVERAGE_DIR}/cobertura.xml"
    echo "  LCOV info     : ${COVERAGE_DIR}/coverage.info"

    if [ "$exit_code" -ne 0 ]; then
        echo ""
        echo "WARNING: Diff coverage is below 80% threshold."
        exit "$exit_code"
    fi
}

cmd_report() {
    local target_branch="${1:-main}"

    init_docker

    if is_ci; then
        echo "=== Coverage Report (CI mode) ==="
    else
        echo "=== Coverage Report (local mode) ==="
    fi
    echo "Docker image : $IMAGE"
    echo "Architecture : $ARCH"
    echo "Database dir : $DATABASE_DIR"
    echo ""

    check_uncommitted
    collect_coverage
    generate_reports

    if is_ci && [ "${CI_PIPELINE_SOURCE:-}" != "merge_request_event" ]; then
        echo ""
        echo "Skipping diff coverage (not a merge request pipeline)."
    else
        determine_compare_ref "$target_branch"
        run_diff_cover
    fi
}

# ── Entry point ───────────────────────────────────────────────────────

case "${1:-}" in
    -h|--help)
        usage
        ;;
    capture)
        shift
        cmd_capture "$@"
        ;;
    report)
        shift
        cmd_report "$@"
        ;;
    "")
        echo "Usage: coverage.sh <command> [options]"
        echo ""
        echo "Commands:"
        echo "  capture [-o FILE]        Capture coverage from .gcda files"
        echo "  report  [branch|commit]  Generate diff coverage report"
        echo ""
        echo "Run coverage.sh --help for the full workflow guide."
        ;;
    *)
        echo "Unknown command: $1"
        echo "Run coverage.sh --help for usage."
        exit 1
        ;;
esac
