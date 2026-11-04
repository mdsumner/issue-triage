#!/usr/bin/env python3
"""Step 2: metadata-only triage signals for open issues (no model).

Usage:
    python meta_sort.py --snap snap --out out

Writes out/open_meta.csv, one row per open issue, with:
    age_days, idle_days, n_comments, last_speaker (maintainer/reporter/other/none),
    maintainer_asked_info  maintainer's last comment asks for info and nobody replied
    maintainer_said_fixed  a maintainer comment says fixed/works in master etc.
    gdal_version           first "GDAL x.y(.z)" mentioned in title/body
    bucket                 a coarse suggested pile (see BUCKETS below)

The buckets are suggestions for a human to review in batches, nothing more.
"""
import argparse
import os
import re

import pandas as pd

MAINTAINER = {"OWNER", "MEMBER", "COLLABORATOR"}
ASK_INFO = re.compile(
    r"(can you (provide|attach|share|post|try|test|check|confirm)|"
    r"could you (provide|attach|share|post|try|test|check|confirm|make a new test)|"
    r"please (provide|attach|share|try|test|check|confirm)|"
    r"(minimal|self[- ]contained) (reproducer|example|dataset)|"
    r"need (a|the) (dataset|file|sample|reproducer)|"
    r"(cannot|can't|can not|unable to|could not|couldn't) reproduce|"
    r"which (gdal )?version|current gdal version is|still (an issue|happen)|retest|re-test)", re.I)
SAID_FIXED = re.compile(
    r"(fixed (in|by|per)|should be fixed|(is|was|has been|have been) (fixed|implemented|addressed|added)|"
    r"(partially )?implemented (per|in|by)|will fix (that|this)|now (uses|supports|handles)|"
    r"works (fine )?(in|with) (master|main|dev|3\.\d+)|no longer (reproduce|happen)|"
    r"resolved (in|by)|addressed (in|by)|superseded (by|per))", re.I)
NEEDS_FUNDING = re.compile(
    r"(funding|sponsor|contracted development|pull requests? (are|is) welcome|"
    r"(provide|submit|contribute)[^.]{0,40}pull request|perfect opportunity to contribute)", re.I)
CC_ONLY = re.compile(r"^\s*(cc|ping)\s+(@[\w-]+[\s,]*)+\s*$", re.I)
VERSION = re.compile(r"\bGDAL\s*v?(\d+\.\d+(?:\.\d+)?)", re.I)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snap", default="snap")
    ap.add_argument("--out", default="out")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    issues = pd.read_parquet(f"{a.snap}/issues.parquet")
    comments = pd.read_parquet(f"{a.snap}/comments.parquet")
    now = pd.Timestamp.now(tz="UTC")
    op = issues[(~issues.is_pr) & (issues.state == "open")].copy()

    cm = comments[comments.number.isin(op.number)].sort_values("created_at")
    last = cm.groupby("number").tail(1).set_index("number")
    mcm = cm[cm.author_association.isin(MAINTAINER)]
    last_m = mcm.groupby("number").tail(1).set_index("number")

    def last_speaker(r):
        if r.number not in last.index:
            return "none"
        lr = last.loc[r.number]
        if lr.author_association in MAINTAINER:
            return "maintainer"
        if lr.author == r.author:
            return "reporter"
        return "other"

    op["age_days"] = (now - op.created_at).dt.days
    op["idle_days"] = (now - op.updated_at).dt.days
    op["last_speaker"] = op.apply(last_speaker, axis=1)
    op["maintainer_asked_info"] = op.apply(
        lambda r: r.last_speaker == "maintainer"
        and bool(ASK_INFO.search(last.loc[r.number].body)), axis=1)
    fixed_nums = set(mcm[mcm.body.apply(lambda b: bool(SAID_FIXED.search(b or "")))].number)
    op["maintainer_said_fixed"] = op.number.isin(fixed_nums)
    fund_nums = set(mcm[mcm.body.apply(lambda b: bool(NEEDS_FUNDING.search(b or "")))].number)
    op["needs_funding_or_pr"] = op.number.isin(fund_nums) | op.labels.apply(
        lambda ls: any(re.search(r"funding", l, re.I) for l in ls))
    op["cc_only_last"] = op.apply(
        lambda r: r.last_speaker == "maintainer"
        and bool(CC_ONLY.match(last.loc[r.number].body or "")), axis=1)
    op["gdal_version"] = (op.title + "\n" + op.body).str.extract(VERSION, expand=False)
    op["is_feature"] = op.labels.apply(
        lambda ls: any(re.search(r"enhancement|feature", l, re.I) for l in ls))

    def bucket(r):
        if r.maintainer_said_fixed:
            return "A_maintainer_said_fixed"
        if r.maintainer_asked_info and r.idle_days > 180:
            return "B_info_requested_no_reply"
        if r.cc_only_last and r.idle_days > 180:
            return "B2_pinged_no_followup"
        if r.needs_funding_or_pr and r.idle_days > 365:
            return "F_needs_funding_or_contributor"
        if r.last_speaker == "none" and r.age_days > 365:
            return "C_never_answered_old"
        if r.is_feature and r.idle_days > 730:
            return "D_stale_feature_request"
        if r.idle_days > 730:
            return "E_stale_other"
        return "Z_active_or_recent"

    op["bucket"] = op.apply(bucket, axis=1)
    cols = ["number", "title", "bucket", "age_days", "idle_days", "n_comments",
            "last_speaker", "maintainer_asked_info", "maintainer_said_fixed",
            "needs_funding_or_pr", "cc_only_last", "gdal_version", "is_feature",
            "labels", "author"]
    op = op[cols].sort_values(["bucket", "idle_days"], ascending=[True, False])
    op["labels"] = op.labels.apply(lambda ls: ";".join(ls))
    op.to_csv(f"{a.out}/open_meta.csv", index=False)
    print(op.bucket.value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
