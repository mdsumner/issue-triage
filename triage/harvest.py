#!/usr/bin/env python3
"""Snapshot GitHub issues, PRs and comments for a repo into Parquet.

Uses the REST API via the `gh` CLI (must be installed and authenticated).
Read-only: nothing is ever written to the tracker.

Usage:
    python harvest.py                      # OSGeo/gdal, everything
    python harvest.py --repo OSGeo/gdal --since 2018-01-01 --out snap

Outputs (in --out):
    issues.parquet    one row per issue AND per PR (is_pr flag)
    comments.parquet  one row per issue/PR conversation comment
    links.parquet     references parsed from text: fixes #N, duplicate of #N
    meta.json         repo, snapshot time, counts

Cost: the repo-wide endpoints are used, so the request count is roughly
(issues + PRs)/100 + comments/100. For GDAL that is on the order of a few
hundred to ~1500 requests, well inside the 5000/hour authenticated limit.
"""
import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys

import pandas as pd


def gh_paginate(path):
    """Yield items from a paginated REST list endpoint, one dict at a time."""
    cmd = ["gh", "api", "--paginate", "-H", "Accept: application/vnd.github+json",
           path, "--jq", ".[]"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, bufsize=1)
    n = 0
    for line in proc.stdout:
        line = line.strip()
        if line:
            n += 1
            if n % 2000 == 0:
                print(f"  ... {n} items from {path.split('?')[0]}", file=sys.stderr)
            yield json.loads(line)
    if proc.wait() != 0:
        sys.exit(f"gh api failed for {path}")


def login(u):
    return (u or {}).get("login")


def harvest_issues(repo, since):
    q = f"repos/{repo}/issues?state=all&per_page=100&sort=created&direction=asc"
    if since:
        q += f"&since={since}T00:00:00Z"
    rows = []
    for it in gh_paginate(q):
        pr = it.get("pull_request")
        rows.append({
            "number": it["number"],
            "is_pr": pr is not None,
            "pr_merged_at": (pr or {}).get("merged_at"),
            "draft": it.get("draft"),
            "title": it.get("title") or "",
            "body": it.get("body") or "",
            "state": it.get("state"),
            "state_reason": it.get("state_reason"),
            "author": login(it.get("user")),
            "author_association": it.get("author_association"),
            "labels": [l["name"] for l in it.get("labels", [])],
            "milestone": (it.get("milestone") or {}).get("title"),
            "assignees": [login(a) for a in it.get("assignees", [])],
            "n_comments": it.get("comments", 0),
            "reactions_total": (it.get("reactions") or {}).get("total_count", 0),
            "created_at": it.get("created_at"),
            "updated_at": it.get("updated_at"),
            "closed_at": it.get("closed_at"),
            "closed_by": login(it.get("closed_by")),
            "locked": it.get("locked"),
        })
    df = pd.DataFrame(rows)
    for c in ["created_at", "updated_at", "closed_at", "pr_merged_at"]:
        df[c] = pd.to_datetime(df[c], utc=True)
    return df


def harvest_comments(repo, since):
    q = f"repos/{repo}/issues/comments?per_page=100&sort=created&direction=asc"
    if since:
        q += f"&since={since}T00:00:00Z"
    rows = []
    for c in gh_paginate(q):
        m = re.search(r"/issues/(\d+)$", c.get("issue_url", ""))
        rows.append({
            "comment_id": c["id"],
            "number": int(m.group(1)) if m else None,
            "author": login(c.get("user")),
            "author_association": c.get("author_association"),
            "body": c.get("body") or "",
            "created_at": c.get("created_at"),
        })
    df = pd.DataFrame(rows)
    if len(df):
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    return df


# "Fixes #123", "closes #45", "resolves https://github.com/OSGeo/gdal/issues/9"
FIX_RE = re.compile(
    r"\b(?:fix(?:e[sd])?|close[sd]?|resolve[sd]?)\s*:?\s+"
    r"(?:https://github\.com/[\w.-]+/[\w.-]+/issues/|#)(\d+)", re.I)
# "duplicate of #123", "dup of #123", "dupe of #123", "same as #123"
DUP_RE = re.compile(
    r"\b(?:duplicate(?:s|d)?(?:\s+of)?|dupe?\s+of|same\s+as)\s*:?\s+"
    r"(?:https://github\.com/[\w.-]+/[\w.-]+/issues/|#)(\d+)", re.I)


def extract_links(issues, comments):
    rows = []
    for r in issues.itertuples():
        text = f"{r.title}\n{r.body}"
        for m in FIX_RE.finditer(text):
            rows.append((r.number, int(m.group(1)), "fixes", "body", r.author_association))
        for m in DUP_RE.finditer(text):
            rows.append((r.number, int(m.group(1)), "duplicate_of", "body", r.author_association))
    for r in comments.itertuples():
        for m in DUP_RE.finditer(r.body):
            rows.append((r.number, int(m.group(1)), "duplicate_of", "comment", r.author_association))
        for m in FIX_RE.finditer(r.body):
            rows.append((r.number, int(m.group(1)), "fixes", "comment", r.author_association))
    df = pd.DataFrame(rows, columns=["src", "dst", "kind", "where", "who_association"])
    return df[df.src != df.dst].drop_duplicates()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="OSGeo/gdal")
    ap.add_argument("--since", default=None, help="YYYY-MM-DD; only items updated since")
    ap.add_argument("--out", default="snap")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    print("issues + PRs ...", file=sys.stderr)
    issues = harvest_issues(a.repo, a.since)
    issues.to_parquet(f"{a.out}/issues.parquet", index=False)
    print("comments ...", file=sys.stderr)
    comments = harvest_comments(a.repo, a.since)
    comments.to_parquet(f"{a.out}/comments.parquet", index=False)
    links = extract_links(issues, comments)
    links.to_parquet(f"{a.out}/links.parquet", index=False)

    iss = issues[~issues.is_pr]
    meta = {
        "repo": a.repo, "since": a.since,
        "snapshot_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "issues_open": int((iss.state == "open").sum()),
        "issues_closed": int((iss.state == "closed").sum()),
        "issues_closed_as_duplicate": int((iss.state_reason == "duplicate").sum()),
        "prs_open": int((issues.is_pr & (issues.state == "open")).sum()),
        "comments": len(comments),
        "links": links.kind.value_counts().to_dict(),
    }
    with open(f"{a.out}/meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
