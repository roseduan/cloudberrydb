#!/bin/bash
#
# Fail if the arrow submodule pin is not reachable from the arrow branch that
# .gitmodules says the submodule tracks.
#
# Arrow merge requests are squash-merged, so an MR's head commit stops being
# reachable from any branch the moment it merges.  A lightning MR prepared
# against an arrow MR that is still under review pins that head; unless the pin
# is moved to the squashed commit after the arrow MR merges, main ends up
# pointing at an orphaned commit.
#
# Nothing breaks for anyone cloning from code.hashdata.xyz, because GitLab
# serves objects that no ref advertises -- which is exactly why this goes
# unnoticed.  It breaks consumers that replicate arrow one branch at a time:
# mirrors, --single-branch clones and release pipelines never receive the
# object and cannot check the submodule out.
#
# See field-engineering/dev_collaboration#1014.

set -euo pipefail

SUBMODULE_PATH=contrib/apache-arrow/arrow

cd "$(git rev-parse --show-toplevel)"

pin=$(git ls-tree HEAD "$SUBMODULE_PATH" | awk '$2 == "commit" { print $3 }')
if [ -z "$pin" ]; then
	echo "error: no submodule gitlink at $SUBMODULE_PATH" >&2
	exit 1
fi

branch=$(git config -f .gitmodules "submodule.$SUBMODULE_PATH.branch" 2>/dev/null || true)
if [ -z "$branch" ]; then
	echo "error: .gitmodules does not say which branch $SUBMODULE_PATH tracks" >&2
	exit 1
fi

# Resolve the submodule URL the way git does, so the relative form in
# .gitmodules is taken against this clone's own remote.  The result can carry
# credentials, so it is never printed.
# Both streams are discarded: in CI the resolved URL carries the job token and
# "git submodule init" echoes it.
if ! git submodule init -- "$SUBMODULE_PATH" >/dev/null 2>&1; then
	echo "error: could not resolve the URL of $SUBMODULE_PATH" >&2
	exit 1
fi
url=$(git config "submodule.$SUBMODULE_PATH.url")

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
git init -q "$tmp"
git -C "$tmp" remote add origin "$url"
if [ -n "${CI_JOB_TOKEN:-}" ]; then
	git -C "$tmp" config credential.helper \
		'!f() { echo username=gitlab-ci-token; echo "password=${CI_JOB_TOKEN}"; }; f'
fi

# Commits are all this needs; skip the blobs when the git in use can.
fetch() {
	git -C "$tmp" fetch -q --filter=blob:none "$@" 2>/dev/null ||
		git -C "$tmp" fetch -q "$@"
}

fetch origin "+refs/heads/$branch:refs/arrow-pin-check/$branch"

# The pinned commit is fetched by SHA on purpose: an orphaned one is still
# servable, and we want to compare it rather than fail on a missing object.
if ! fetch origin "$pin"; then
	echo "error: arrow does not have the pinned commit at all." >&2
	echo "  pinned: $pin" >&2
	exit 1
fi

tip=$(git -C "$tmp" rev-parse "refs/arrow-pin-check/$branch")

if git -C "$tmp" merge-base --is-ancestor "$pin" "refs/arrow-pin-check/$branch"; then
	echo "ok: arrow pin $pin is reachable from '$branch' (tip $tip)"
	exit 0
fi

cat >&2 <<EOF
error: the arrow submodule pin is not reachable from arrow's '$branch'.

  pinned        : $pin
  arrow $branch tip : $tip

This is almost always a merge-request head that was squash-merged: the head
SHA stops being reachable from any branch, and consumers that replicate arrow
branch by branch (mirrors, --single-branch clones, release pipelines) cannot
check it out.  Cloning from code.hashdata.xyz still works, so you will not see
this locally.

Re-pin to the commit that carries the same tree on '$branch' -- for a squashed
merge request that is the squash commit the arrow MR page links -- and commit
the submodule change again:

  git -C $SUBMODULE_PATH fetch origin $branch
  git -C $SUBMODULE_PATH checkout <squashed-sha>
  git add $SUBMODULE_PATH

See field-engineering/dev_collaboration#1014.
EOF
exit 1
