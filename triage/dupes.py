#!/usr/bin/env python3
"""Step 3 (dupes first): candidate duplicate pairs for open issues.

Each open issue is compared against all other issues (open and closed).
Text = title (weighted x3) + body with code blocks and long tracebacks kept
(error strings are very discriminating for GDAL), truncated.

Usage:
    python dupes.py --snap snap --out out                    # TF-IDF, no extra deps
    python dupes.py --snap snap --out out --embed            # sentence-transformers
    python dupes.py ... --k 5 --min-score 0.35

Writes:
    out/dupe_candidates.csv  open issue, neighbour, score, neighbour state/reason,
                             kind: open-open | open-vs-fixed | open-vs-duplicate |
                                   open-vs-notplanned
    out/dupe_calibration.txt recall@k on issues already closed as duplicates
                             whose canonical issue is known (from links.parquet)

Read the calibration first: it tells you what threshold to trust.
"open-vs-fixed" pairs are interesting in their own right: an open issue that
looks like one already closed-as-completed may simply be fixed.
"""
import argparse
import os
import re

import numpy as np
import pandas as pd



# Issue-template boilerplate that otherwise dominates similarity scores
TEMPLATE = [
    r"(?s)<!--.*?-->",
    r"(?im)^.*for example \(please modify !!!\).*$",
    r"(?im)^\s*#+\s*(expected behaviou?r and actual behaviou?r\.?|steps to reproduce the (problem|issue)\.?|"
    r"operating system|gdal version and provenance|what is the bug\??|versions and provenance|"
    r"additional context|feature description|expected behaviou?r|actual behaviou?r)\s*$",
    r"(?i)_no response_",
]


def prep(df, maxlen):
    body = df.body.fillna("")
    for pat in TEMPLATE:
        body = body.str.replace(pat, " ", regex=True)
    # keep code content but drop the fences themselves
    body = body.str.replace("```", " ", regex=False)
    t = (df.title.fillna("") + " ") * 3 + body
    return t.str.slice(0, maxlen).tolist()


def tfidf_vectors(texts):
    from sklearn.feature_extraction.text import TfidfVectorizer
    v = TfidfVectorizer(sublinear_tf=True, ngram_range=(1, 2), min_df=2,
                        max_df=0.5, token_pattern=r"(?u)\b[\w.:/-]{2,}\b",
                        max_features=300000)
    return v.fit_transform(texts)  # rows are L2-normalised


def embed_vectors(texts, model):
    from sentence_transformers import SentenceTransformer
    m = SentenceTransformer(model)
    return m.encode(texts, batch_size=64, show_progress_bar=True,
                    normalize_embeddings=True)


def topk(X, qidx, k):
    """Cosine top-k neighbours (excluding self) for the rows in qidx."""
    out_i, out_s = [], []
    for start in range(0, len(qidx), 512):
        q = qidx[start:start + 512]
        S = X[q] @ X.T
        S = S.toarray() if hasattr(S, "toarray") else np.asarray(S)
        S[np.arange(len(q)), q] = -1
        idx = np.argpartition(-S, kth=min(k, S.shape[1] - 1), axis=1)[:, :k]
        sc = np.take_along_axis(S, idx, axis=1)
        o = np.argsort(-sc, axis=1)
        out_i.append(np.take_along_axis(idx, o, axis=1))
        out_s.append(np.take_along_axis(sc, o, axis=1))
    return np.vstack(out_i), np.vstack(out_s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snap", default="snap")
    ap.add_argument("--out", default="out")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--min-score", type=float, default=0.30)
    ap.add_argument("--maxlen", type=int, default=6000)
    ap.add_argument("--embed", action="store_true")
    ap.add_argument("--model", default="all-MiniLM-L6-v2")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    iss = pd.read_parquet(f"{a.snap}/issues.parquet")
    iss = iss[~iss.is_pr].reset_index(drop=True)
    texts = prep(iss, a.maxlen)
    X = embed_vectors(texts, a.model) if a.embed else tfidf_vectors(texts)
    pos = {n: i for i, n in enumerate(iss.number)}

    # ---- calibration on known duplicates
    cal_lines = []
    lp = f"{a.snap}/links.parquet"
    if os.path.exists(lp):
        links = pd.read_parquet(lp)
        d = links[(links.kind == "duplicate_of")]
        d = d[d.src.isin(iss.number[iss.state == "closed"]) & d.dst.isin(pos)]
        d = d.drop_duplicates("src")
        if len(d):
            q = np.array([pos[s] for s in d.src])
            I, S = topk(X, q, max(a.k, 10))
            nums = iss.number.values[I]
            for k in (1, 3, 5, 10):
                hit = np.array([dst in row[:k] for dst, row in zip(d.dst, nums)])
                cal_lines.append(f"recall@{k}: {hit.mean():.2f}  (n={len(d)})")
            # score of the true canonical when found, vs top non-canonical
            true_s = [s[list(r).index(dst)] for dst, r, s in zip(d.dst, nums, S) if dst in r]
            if true_s:
                cal_lines.append("score of true canonical when in top10: "
                                 f"p10={np.percentile(true_s,10):.2f} "
                                 f"median={np.median(true_s):.2f}")
        else:
            cal_lines.append("no duplicate-closed issues with a parsed canonical link")
    with open(f"{a.out}/dupe_calibration.txt", "w") as f:
        f.write("\n".join(cal_lines) + "\n")
    print("\n".join(cal_lines))

    # ---- candidates for open issues
    q = np.where(iss.state == "open")[0]
    I, S = topk(X, q, a.k)
    rows = []
    for qi, (ii, ss) in zip(q, zip(I, S)):
        for j, s in zip(ii, ss):
            if s < a.min_score:
                continue
            o = iss.iloc[j]
            if o.state == "open":
                kind = "open-open"
            elif o.state_reason == "duplicate":
                kind = "open-vs-duplicate"
            elif o.state_reason == "not_planned":
                kind = "open-vs-notplanned"
            else:
                kind = "open-vs-fixed"
            rows.append({
                "open_number": int(iss.number[qi]), "open_title": iss.title[qi],
                "other_number": int(o.number), "other_title": o.title,
                "score": round(float(s), 3), "kind": kind,
                "other_closed_at": o.closed_at,
                "shared_labels": ";".join(sorted(set(iss.labels[qi]) & set(o.labels))),
            })
    c = pd.DataFrame(rows)
    if len(c):
        # one row per unordered open-open pair
        oo = c.kind == "open-open"
        key = c.apply(lambda r: tuple(sorted((r.open_number, r.other_number))), axis=1)
        c = c[~(oo & key.duplicated())]
        c = c.sort_values("score", ascending=False)
    c.to_csv(f"{a.out}/dupe_candidates.csv", index=False)
    print(c.kind.value_counts().to_string() if len(c) else "no candidates")


if __name__ == "__main__":
    main()
