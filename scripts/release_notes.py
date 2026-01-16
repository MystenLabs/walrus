# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

import argparse
from collections import defaultdict
import json
import os
import re
import subprocess
import sys
from typing import NamedTuple

RE_NUM = re.compile("[0-9_]+")

RE_PR = re.compile(
    r"^.*\(#(\d+)\)$",
    re.MULTILINE,
)

RE_HEADING = re.compile(
    r"#+ Release notes(.*)",
    re.DOTALL | re.IGNORECASE,
)

RE_CHECK = re.compile(
    r"^\s*-\s*\[.\]",
    re.MULTILINE,
)

RE_NOTE = re.compile(
    r"^\s*-\s*\[( |x)?\]\s*([^:]+):",
    re.MULTILINE | re.IGNORECASE,
)

# Regex to check if a PR has a checked release notes checkbox
RE_CHECKED = re.compile(r"^\s*-\s*\[x\]\s*", re.MULTILINE | re.IGNORECASE)

# Only commits that affect changes in these directories will be
# considered when generating release notes.
INTERESTING_DIRECTORIES = [
    "crates",
    "docs",
    "docker",
    "contracts",
    "testnet-contracts",
]

# Start release notes with these sections, if they contain relevant
# information (helps us keep a consistent order for impact areas we
# know about).
NOTE_ORDER = [
    "Storage node",
    "Aggregator",
    "Publisher",
    "CLI",
    "Backup node",
]

class Note(NamedTuple):
    checked: bool
    note: str

def parse_args():
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(
        description=(
            "Extract release notes from git commits. Check help for the "
            "`generate` and `check` subcommands for more information."
        ),
    )

    sub_parser = parser.add_subparsers(dest="command")

    generate_p = sub_parser.add_parser(
        "generate",
        description="Generate release notes from git commits.",
    )

    generate_p.add_argument(
        "from",
        help="The commit to start from (exclusive)",
    )

    generate_p.add_argument(
        "to",
        nargs="?",
        default="HEAD",
        help="The commit to end at (inclusive), defaults to HEAD.",
    )

    check_p = sub_parser.add_parser(
        "check",
        description=(
            "Check if the release notes section of a the PR is complete, "
            "i.e. that every impacted component has a non-empty note."
        ),
    )

    check_p.add_argument(
        "pr",
        nargs="?",
        help="The PR to check.",
    )

    list_prs_p = sub_parser.add_parser(
        "list-prs",
        description=(
            "Lists PRs with checked release notes between two commits, "
            "outputs JSON array for CI workflows."
        ),
    )

    list_prs_p.add_argument(
        "from",
        help="The commit to start from (exclusive)",
    )

    list_prs_p.add_argument(
        "to",
        nargs="?",
        default="HEAD",
        help="The commit to end at (inclusive), defaults to HEAD.",
    )

    get_notes_p = sub_parser.add_parser(
        "get-notes",
        description="Get formatted release notes for a specific PR.",
    )

    get_notes_p.add_argument(
        "pr",
        help="The PR to get notes for.",
    )

    return vars(parser.parse_args())

def git(*args):
    """Run a git command and return the output as a string."""
    return subprocess.check_output(["git"] + list(args)).decode().strip()


def gh_api(endpoint):
    """Make a GitHub API request and return the JSON response.

    Uses the WALRUS_REPO_TOKEN environment variable for authentication,
    or falls back to using the gh CLI tool.
    """
    gh_token = os.getenv('WALRUS_REPO_TOKEN')
    if gh_token:
        url = f"https://api.github.com{endpoint}"
        curl_command = [
            "curl", "-s",
            "-H", "Accept: application/vnd.github.groot-preview+json",
            "-H", f"Authorization: Bearer {gh_token}",
            url
        ]
        result = subprocess.run(
            curl_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        return json.loads(result.stdout) if result.stdout else None
    else:
        # Fall back to using gh CLI
        result = subprocess.run(
            ["gh", "api", endpoint],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        return json.loads(result.stdout) if result.stdout else None


def pr_has_release_notes(pr):
    """Check if a PR has checked release notes.

    Returns True if the PR body contains at least one checked release notes
    checkbox (i.e., `- [x]` pattern).
    """
    json_data = gh_api(f"/repos/MystenLabs/walrus/pulls/{pr}")
    if not json_data:
        return False
    body = json_data.get("body", "")
    if not body:
        return False

    # Check if there's a release notes section
    match = RE_HEADING.search(body)
    if not match:
        return False

    # Check if there's at least one checked checkbox in the release notes section
    notes_section = match.group(1)
    return bool(RE_CHECKED.search(notes_section))

def parse_notes(pr, notes):
    result = {}
    # verify notes param
    if not isinstance(notes, (str, bytes)) or not notes:
        return pr, result

    # Find the release notes section
    match = RE_HEADING.search(notes)
    if not match:
        return pr, result

    start = 0
    notes = match.group(1)

    while True:
        # Find the next possible release note
        match = RE_NOTE.search(notes, start)
        if not match:
            break

        checked = match.group(1)
        impacted = match.group(2)
        begin = match.end()

        # Find the end of the note, or the end of the commit
        match = RE_CHECK.search(notes, begin)
        end = match.start() if match else len(notes)

        result[impacted] = Note(
            checked=checked in "xX",
            note=notes[begin:end].strip(),
        )
        start = end

    return pr, result

def extract_notes_for_pr(pr):
    """Get release notes from a body of the PR

    Find the 'Release notes' section in PR body, and
    extract the notes for each impacted area (area that has been
    ticked).

    Returns a tuple of the PR number and a dictionary of impacted
    areas mapped to their release note. Each release note indicates
    whether it has a note and whether it was checked (ticked).

    """
    json_data = gh_api(f"/repos/MystenLabs/walrus/pulls/{pr}")
    if not json_data:
        return parse_notes(pr, None)
    notes = json_data.get("body")
    return parse_notes(pr, notes)

def extract_notes_for_commit(commit):
    """Get release notes from a commit message.

    Find the 'Release notes' section in the commit message, and
    extract the notes for each impacted area (area that has been
    ticked).

    Returns a tuple of the PR number and a dictionary of impacted
    areas mapped to their release note. Each release note indicates
    whether it has a note and whether it was checked (ticked).

    """
    json_data = gh_api(f"/repos/MystenLabs/walrus/commits/{commit}/pulls")
    if not json_data:
        return parse_notes(None, "")

    message = json_data[0].get("body") if json_data else ""
    pr = json_data[0].get("number") if json_data else None
    return parse_notes(pr, message)

def print_changelog(pr, log):
    if pr:
        print(f"https://github.com/MystenLabs/walrus/pull/{pr}:")
    print(log)

def do_check(pr):
    """Check if the release notes section of a given PR is complete.

    This means that every impacted component has a non-empty note,
    every note is attached to a checked checkbox, and every impact
    area is known.

    """
    pr, notes = extract_notes_for_pr(pr)
    issues = []
    for impacted, note in notes.items():
        if impacted not in NOTE_ORDER:
            issues.append(f" - Found unfamiliar impact area '{impacted}'.")

        if note.checked and not note.note:
            issues.append(f" - '{impacted}' is checked but has no release note.")

        if not note.checked and note.note:
            issues.append(
                f" - '{impacted}' has a release note but is not checked: {note.note}"
            )

    if not issues:
        return

    print(f"Found issues with release notes in PR {pr}:")
    for issue in issues:
        print(issue)
    sys.exit(1)

def do_generate(from_, to):
    """Generate release notes from git commits.

    This will extract the release notes from all commits between
    `from_` (exclusive) and `to` (inclusive), and print out a markdown
    snippet with a heading for each impact area that has a note,
    followed by a list of its relevant changelog.

    Only looks for commits affecting INTERESTING_DIRECTORIES.

    """
    results = defaultdict(list)

    root = git("rev-parse", "--show-toplevel")
    os.chdir(root)

    commits = git(
        "log",
        "--pretty=format:%H",
        f"{from_}..{to}",
        "--",
        *INTERESTING_DIRECTORIES,
    ).strip()

    if not commits:
        return

    for commit in commits.split("\n"):
        pr, notes = extract_notes_for_commit(commit)
        for impacted, note in notes.items():
            if note.checked:
                results[impacted].append((pr, note.note))

    # Print the impact areas we know about first
    for impacted in NOTE_ORDER:
        notes = results.pop(impacted, None)
        if not notes:
            continue

        print(f"## {impacted}")
        print()

        for pr, note in reversed(notes):
            print_changelog(pr, note)
            print()

    # Print any remaining impact areas
    for impacted, notes in results.items():
        print(f"## {impacted}\n")
        for pr, note in reversed(notes):
            print_changelog(pr, note)
            print()

def do_list_prs(from_, to):
    """List PRs with checked release notes between two commits.

    Outputs a JSON array of PR numbers that have checked release notes,
    suitable for use in CI workflows.
    """
    root = git("rev-parse", "--show-toplevel")
    os.chdir(root)

    commits = git(
        "log",
        "--pretty=format:%H",
        f"{from_}..{to}",
        "--",
        *INTERESTING_DIRECTORIES,
    ).strip()

    if not commits:
        print("[]")
        return

    prs_with_notes = []
    seen_prs = set()

    for commit in commits.split("\n"):
        json_data = gh_api(f"/repos/MystenLabs/walrus/commits/{commit}/pulls")
        if not json_data:
            continue

        for pr_data in json_data:
            pr_number = pr_data.get("number")
            if pr_number and pr_number not in seen_prs:
                seen_prs.add(pr_number)
                if pr_has_release_notes(pr_number):
                    prs_with_notes.append(pr_number)

    print(json.dumps(prs_with_notes))


def do_get_notes(pr):
    """Get formatted release notes for a specific PR.

    Extracts and prints the release notes that are checked,
    in the format "impacted: note".
    """
    pr, notes = extract_notes_for_pr(pr)
    for impacted, note in notes.items():
        if note.checked and note.note:
            print(f"{impacted}: {note.note}")


if __name__ == "__main__":
    args = parse_args()
    if args["command"] == "generate":
        do_generate(args["from"], args["to"])
    elif args["command"] == "check":
        do_check(args["pr"])
    elif args["command"] == "list-prs":
        do_list_prs(args["from"], args["to"])
    elif args["command"] == "get-notes":
        do_get_notes(args["pr"])
    else:
        print("No command specified. Use --help for usage information.")
        sys.exit(1)
