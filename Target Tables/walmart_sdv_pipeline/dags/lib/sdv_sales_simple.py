# sdv_sales_simple.py (Route A: SDV + postprocess)
import os, json, duckdb, numpy as np, pandas as pd
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer

MD_CATALOG      = os.getenv("MD_CATALOG", "project_882")
MD_SOURCE_TABLE = os.getenv("MD_SOURCE_TABLE", "main.walmart_sales_like")
MD_TARGET_TABLE = os.getenv("MD_TARGET_TABLE", "main.walmart_sales_like_sdv")
NUM_ROWS        = int(os.getenv("NUM_ROWS", "3000"))
SEED_LIMIT      = int(os.getenv("SEED_LIMIT", "0"))  # 0 = use all rows
# Date strategy: 'bootstrap' = resample seed dates; 'uniform' = sample evenly between [min, max]
DATE_STRATEGY   = os.getenv("DATE_STRATEGY", "bootstrap")  

def _connect():
    """Connect to MotherDuck using token from environment."""
    token = os.environ["MOTHERDUCK_TOKEN"]
    return duckdb.connect(f"md:?motherduck_token={token}")

def _load_seed_df(con):
    """Load the seed dataset from MotherDuck."""
    fq = f"{MD_CATALOG}.{MD_SOURCE_TABLE}"
    sql = f"SELECT * FROM {fq}" if SEED_LIMIT <= 0 else f"SELECT * FROM {fq} LIMIT {SEED_LIMIT}"
    df = con.execute(sql).fetchdf()
    print(f"[LOAD] seed rows={len(df)} cols={list(df.columns)}")
    return df

def _to_num(s):
    """Convert a pandas Series to numeric safely."""
    if pd.api.types.is_numeric_dtype(s): 
        return pd.to_numeric(s, errors="coerce")
    s = s.astype(str).replace({"None": None, "nan": None})
    s = s.str.replace(r"[\$,]", "", regex=True).str.strip()
    s = s.replace({"": None})
    return pd.to_numeric(s, errors="coerce")

def _clean_seed_df(df):
    """Select useful columns, standardize types, and fix missing values."""
    keep = [c for c in ["order_id","store_id","order_date","dept","sku","price","qty","revenue","title","brand"] if c in df.columns]
    df = df[keep].copy()

    if "order_date" in df: df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    for c in ["price","revenue","qty","sku","store_id"]:
        if c in df: df[c] = _to_num(df[c])
    for c in ["dept","title","brand"]:
        if c in df: df[c] = df[c].astype(str).replace({"None": None, "nan": None})

    # Fix revenue if missing or non-positive
    if all(c in df for c in ["price","qty","revenue"]):
        m = df["revenue"].isna() | (df["revenue"] <= 0)
        df.loc[m, "revenue"] = df.loc[m, "price"] * df.loc[m, "qty"]

    # Drop rows missing essential columns
    essential = [c for c in ["price","qty","order_date"] if c in df]
    df = df.dropna(subset=essential)

    # Sanitize qty
    if "qty" in df:
        df["qty"] = df["qty"].clip(lower=1).round().astype(int)

    print(f"[CLEAN] rows={len(df)}")
    return df

def _build_metadata(df):
    """Construct SDV metadata for the dataset."""
    md = SingleTableMetadata()
    cols = {}
    if "order_id" in df: cols["order_id"] = {"sdtype":"id"}
    if "sku" in df:      cols["sku"]      = {"sdtype":"id"}
    if "store_id" in df: cols["store_id"] = {"sdtype":"numerical"}
    if "price" in df:    cols["price"]    = {"sdtype":"numerical"}
    if "qty" in df:      cols["qty"]      = {"sdtype":"numerical"}
    if "revenue" in df:  cols["revenue"]  = {"sdtype":"numerical"}
    if "order_date" in df: cols["order_date"] = {"sdtype":"datetime"}
    # Text columns are also modeled in SDV but will be replaced later during postprocessing
    if "dept" in df:     cols["dept"]     = {"sdtype":"text"}
    if "title" in df:    cols["title"]    = {"sdtype":"text"}
    if "brand" in df:    cols["brand"]    = {"sdtype":"text"}

    md.columns = cols
    if "order_id" in df: md.primary_key = "order_id"
    print("[META]", json.dumps(md.to_dict(), indent=2))
    return md

def _fit_and_sample(df, n):
    """Train SDV model and generate synthetic samples."""
    md = _build_metadata(df)
    model_cols = list(md.columns.keys())
    synth = GaussianCopulaSynthesizer(md)
    synth.fit(df[model_cols].copy())
    out = synth.sample(num_rows=n)
    print(f"[SDV] generated={len(out)}")
    return out

def _postprocess(syn: pd.DataFrame, seed: pd.DataFrame) -> pd.DataFrame:
    """Refine synthetic data to improve realism."""
    out = syn.copy()

    # --- 1) Categorical/text columns: resample from seed frequencies ---
    def sample_by_freq(series, k):
        s = series.dropna()
        if s.empty: return [None]*k
        vals, cnts = np.unique(s, return_counts=True)
        p = cnts / cnts.sum()
        return np.random.choice(vals, size=k, p=p)

    for c in ["dept","brand","title"]:
        if c in out.columns and c in seed.columns:
            out[c] = sample_by_freq(seed[c], len(out))

    # --- 2) order_date: replace invalid 1970 dates using the selected strategy ---
    if "order_date" in out and "order_date" in seed:
        seed_dates = pd.to_datetime(seed["order_date"], errors="coerce").dropna()
        if len(seed_dates) > 0:
            if DATE_STRATEGY == "uniform":
                dmin, dmax = seed_dates.min(), seed_dates.max()
                # Uniform sampling within the range
                days = (dmax - dmin).days
                idx = np.random.randint(0, max(days,1), size=len(out))
                out["order_date"] = dmin + pd.to_timedelta(idx, unit="D")
            else:  # bootstrap
                out["order_date"] = np.random.choice(seed_dates.values, size=len(out))

    # --- 3) Numerical adjustments ---
    for c in ["store_id","sku"]:
        if c in out and c in seed:
            pool = seed[c].dropna().astype(int)
            if len(pool) > 0:
                out[c] = np.random.choice(pool, size=len(out))
    if "qty" in out:
        out["qty"] = pd.to_numeric(out["qty"], errors="coerce").fillna(1).clip(lower=1).round().astype(int)
    if "price" in out and "price" in seed:
        s = pd.to_numeric(out["price"], errors="coerce")
        q1, q99 = np.nanpercentile(seed["price"], [1,99])
        s = s.clip(lower=max(q1, 0.01), upper=max(q99, q1+0.01))
        out["price"] = s
    if "revenue" in out and {"price","qty"}.issubset(out.columns):
        out["revenue"] = (out["price"] * out["qty"]).round(2)

    return out

def _save(con, df):
    """Write the synthetic dataset back to MotherDuck."""
    fq = f"{MD_CATALOG}.{MD_TARGET_TABLE}"
    con.register("sdv_tmp_df", df)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {MD_CATALOG}.main;")
    con.execute(f"CREATE OR REPLACE TABLE {fq} AS SELECT * FROM sdv_tmp_df;")
    con.unregister("sdv_tmp_df")
    n = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
    print(f"[SAVE] {fq} rows={n}")

def run_sales_job():
    """Main entry point: end-to-end SDV pipeline."""
    print(f"== CONFIG == catalog={MD_CATALOG} source={MD_SOURCE_TABLE} target={MD_TARGET_TABLE} rows={NUM_ROWS} seed_limit={SEED_LIMIT} date_strategy={DATE_STRATEGY}")
    con = _connect()
    try:
        seed = _load_seed_df(con)
        seed = _clean_seed_df(seed)
        syn  = _fit_and_sample(seed, NUM_ROWS)
        syn2 = _postprocess(syn, seed)
        _save(con, syn2)
    finally:
        con.close()

if __name__ == "__main__":
    run_sales_job()
