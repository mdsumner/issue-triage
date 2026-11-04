#!/usr/bin/env python3
"""Build the static data files the issuescope page loads.

Input: a snapshot folder from harvest.py (issues.parquet, comments.parquet,
links.parquet, meta.json). Output (site/data/):
  items.parquet     one row per issue/PR: metadata, short snippet, doc length
  vocab.parquet     term_id, term, df
  postings.parquet  term_id, number, tf   (BM25 is computed in SQL in the page)
  links.parquet     src, dst, kind (fixes | duplicate_of), parsed from text
  xrefs.parquet     src, dst: any "#N" mention between threads
  bodies.parquet    number, body (full issue/PR body; loaded on demand)
  comments.parquet  number, author, author_association, created_at, body
  meta.json         snapshot time, repo, counts, tokenizer settings

The tokenizer MUST match tokenize() in site/app.js.
"""
import argparse, json, os, re
import duckdb, pandas as pd

TOKEN = re.compile(r"[a-z0-9_]{2,}")
STOP = set("""the and for that this with from have not are but was can when what
which there their will would should could into than then them they you your our
its also use used using does did has had been being any all some such only just
more most other same very each how why who where here get got like one two way
about after before over under while again further once both few own off out
issue issues gdal""".split())
TITLE_WEIGHT = 3
MAX_CHARS = 20000
TEMPLATE = [
    r"(?s)<!--.*?-->",
    r"(?im)^.*for example \(please modify !!!\).*$",
    r"(?im)^\s*#+\s*(expected behaviou?r and actual behaviou?r\.?|steps to reproduce the (problem|issue)\.?|"
    r"operating system|gdal version and provenance|what is the bug\??|versions and provenance|"
    r"additional context|feature description|expected behaviou?r|actual behaviou?r)\s*$",
    r"(?i)_no response_",
]


def clean(s):
    s = s or ""
    for p in TEMPLATE:
        s = re.sub(p, " ", s)
    return s


def tokens(s):
    return [t for t in TOKEN.findall(s.lower()) if t not in STOP and not t.isdigit() or (t.isdigit() and len(t) >= 3)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snap", required=True)
    ap.add_argument("--out", default="site/data")
    ap.add_argument("--max-df", type=float, default=0.4)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    con = duckdb.connect()
    S = a.snap
    con.sql(f"CREATE TABLE i AS SELECT * FROM read_parquet('{S}/issues.parquet')")
    con.sql(f"CREATE TABLE c AS SELECT * FROM read_parquet('{S}/comments.parquet')")
    docs = con.sql("""SELECT i.number, i.title, coalesce(i.body,'') body,
        coalesce(string_agg(c.body, chr(10) ORDER BY c.created_at), '') cbody
        FROM i LEFT JOIN c USING(number) GROUP BY ALL""").df()

    rows, dl, snippet = [], {}, {}
    for r in docs.itertuples():
        body = clean(r.body)
        text = ((r.title + " ") * TITLE_WEIGHT + body + "\n" + clean(r.cbody))[:MAX_CHARS]
        toks = tokens(text)
        dl[r.number] = len(toks)
        snippet[r.number] = re.sub(r"\s+", " ", body).strip()[:400]
        tf = {}
        for t in toks:
            tf[t] = tf.get(t, 0) + 1
        rows.extend((t, r.number, n) for t, n in tf.items())
    post = pd.DataFrame(rows, columns=["term", "number", "tf"])
    N = len(docs)
    df = post.groupby("term").size().rename("df").reset_index()
    df = df[(df.df >= 2) & (df.df <= a.max_df * N)].sort_values("term").reset_index(drop=True)
    df["term_id"] = df.index.astype("int32")
    post = post.merge(df[["term", "term_id"]], on="term")[["term_id", "number", "tf"]]
    post["number"] = post.number.astype("int32")
    post["tf"] = post.tf.clip(upper=32767).astype("int16")
    post = post.sort_values(["term_id", "number"])
    con.register("post", post)
    con.register("vocab", df[["term_id", "term", "df"]])
    con.sql(f"COPY (SELECT * FROM post ORDER BY term_id, number) TO '{a.out}/postings.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 200000)")
    con.sql(f"COPY vocab TO '{a.out}/vocab.parquet' (FORMAT parquet, COMPRESSION zstd)")

    meta_df = pd.DataFrame({"number": list(dl), "dl": list(dl.values()), "snippet": [snippet[n] for n in dl]})
    con.register("m", meta_df)
    con.sql(f"""COPY (SELECT i.number::INT number, i.is_pr, i.state, i.state_reason,
        i.pr_merged_at IS NOT NULL AS merged, coalesce(i.draft, false) draft, i.title, i.labels,
        i.author, i.author_association, timezone('UTC', i.created_at) created_at, timezone('UTC', i.updated_at) updated_at, timezone('UTC', i.closed_at) closed_at,
        i.n_comments::INT n_comments, m.dl::INT dl, m.snippet
        FROM i JOIN m USING(number)) TO '{a.out}/items.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    con.sql(f"""COPY (SELECT src::INT src, dst::INT dst, kind FROM read_parquet('{S}/links.parquet'))
        TO '{a.out}/links.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    con.sql(f"""COPY (WITH r AS (
          SELECT number src, CAST(m AS INT) dst FROM c, unnest(regexp_extract_all(body, '#(\\d{{2,6}})\\b', 1)) t(m)
          UNION ALL SELECT number, CAST(m AS INT) FROM i, unnest(regexp_extract_all(coalesce(body,''), '#(\\d{{2,6}})\\b', 1)) t(m))
        SELECT DISTINCT r.src::INT src, r.dst dst FROM r JOIN i ON i.number = r.dst WHERE r.src <> r.dst)
        TO '{a.out}/xrefs.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    con.sql(f"""COPY (SELECT number::INT number, coalesce(body, '') body FROM i ORDER BY number)
        TO '{a.out}/bodies.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    con.sql(f"""COPY (SELECT number::INT number, author, author_association, timezone('UTC', created_at) created_at, body FROM c ORDER BY number, created_at)
        TO '{a.out}/comments.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    meta = json.load(open(f"{S}/meta.json"))
    meta.update({"n_docs": N, "n_terms": len(df), "n_postings": len(post),
                 "avgdl": float(sum(dl.values()) / N), "title_weight": TITLE_WEIGHT,
                 "token_regex": TOKEN.pattern, "stopwords": sorted(STOP)})
    json.dump(meta, open(f"{a.out}/meta.json", "w"), indent=1)
    print({k: meta[k] for k in ("snapshot_utc", "n_docs", "n_terms", "n_postings", "avgdl")})


if __name__ == "__main__":
    main()
