# /opt/airflow/dags/lib/sdv_walmart.py
# -*- coding: utf-8 -*-
#
# SDV Walmart Generator:
# Cleans source data, builds SDV metadata, trains a GaussianCopulaSynthesizer,
# generates synthetic data, and writes the result back to MotherDuck.

import os
import json
import logging
import pandas as pd
import numpy as np
import duckdb

from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer

log = logging.getLogger(__name__)

# ------------------------------
# Environment configuration
# ------------------------------
def _get_env():
    """Read configuration and tokens from environment variables."""
    md_catalog = os.getenv("MD_CATALOG", "project_882")
    md_source = os.getenv("MD_SOURCE_TABLE", "main.backup_raw_target_api")
    md_target = os.getenv("MD_TARGET_TABLE", "main.walmart_sdv")
    num_rows  = int(os.getenv("NUM_ROWS", "2000"))
    # Retrieve token (prefer MOTHERDUCK_TOKEN, fallback to MD_TOKEN)
    token     = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("MD_TOKEN")
    seed_lim  = os.getenv("SEED_LIMIT")
    seed_lim  = int(seed_lim) if seed_lim and seed_lim.isdigit() else None

    log.info("== DEBUG config == md_catalog=%s source=%s target=%s rows=%s seed_limit=%s",
             md_catalog, md_source, md_target, num_rows, seed_lim)
    if not token:
        raise RuntimeError("MotherDuck token not found (expected MOTHERDUCK_TOKEN or MD_TOKEN).")
    return md_catalog, md_source, md_target, num_rows, token, seed_lim


def _connect_md(token: str) -> duckdb.DuckDBPyConnection:
    """Establish a DuckDB connection to MotherDuck."""
    return duckdb.connect(f"md:?motherduck_token={token}")

# ------------------------------
# Load source table
# ------------------------------
def _load_source_df(con: duckdb.DuckDBPyConnection, catalog: str, tbl: str, seed_limit: int | None) -> pd.DataFrame:
    """Load source data from MotherDuck with optional row limit."""
    fq = f"{catalog}.{tbl}" if "." in tbl else f"{catalog}.main.{tbl}"
    log.info("[LOAD] Reading source table: %s", fq)

    # Use LIMIT for sampling; works consistently across DuckDB/MotherDuck versions
    sql = f"SELECT * FROM {fq} LIMIT {seed_limit}" if seed_limit and seed_limit > 0 else f"SELECT * FROM {fq}"
    df = con.execute(sql).fetchdf()
    log.info("[LOAD] Source rows=%s, cols=%s", len(df), list(df.columns))
    return df

# ------------------------------
# Data cleaning: handle nested values and type normalization
# ------------------------------
def _clean_source_df(df: pd.DataFrame) -> pd.DataFrame:
    """Convert nested fields to JSON strings and standardize column types."""
    df = df.copy()

    # (1) Convert list/dict/ndarray columns into JSON strings to avoid
    #     “unhashable type: numpy.ndarray” errors during SDV metadata detection.
    def _to_json_if_nested(x):
        if isinstance(x, (list, dict, np.ndarray)):
            try:
                return json.dumps(x, ensure_ascii=False)
            except Exception:
                return str(x)
        return x

    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, (list, dict, np.ndarray))).any():
            log.info("[COERCE] Serialized nested column '%s' -> JSON string", col)
            df[col] = df[col].apply(_to_json_if_nested)

    # (2) Attempt to convert likely numeric columns to numeric dtype
    likely_numeric_cols = [
        "tcin",
        "data_product_tcin",
        "data_product_fulfillment_scheduled_delivery_location_id",
        "data_product_fulfillment_product_id",
        "store_id",
        "qty",
        "price",
    ]
    for col in likely_numeric_cols:
        if col in df.columns:
            before_na = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors="ignore")
            after_na = df[col].isna().sum()
            if str(df[col].dtype).startswith(("int", "float")):
                log.info("[COERCE] Cast column '%s' -> numerical", col)
            elif after_na != before_na:
                log.info("[COERCE] Tried numeric cast on '%s' (no major effect)", col)

    # (3) Attempt to convert likely datetime columns to datetime dtype
    likely_datetime_cols = ["dt", "record_date", "load_date", "load_timestamp"]
    for col in likely_datetime_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                log.info("[COERCE] Cast column '%s' -> datetime", col)
            except Exception:
                pass

    return df

# ------------------------------
# Build SDV metadata (after nested columns are handled)
# ------------------------------
def _build_metadata(df: pd.DataFrame) -> SingleTableMetadata:
    """Detect and refine SDV metadata for the given dataframe."""
    md = SingleTableMetadata()
    md.detect_from_dataframe(df)

    # Adjust some common columns’ sdtypes:
    # - qty is often detected as categorical but should be numerical.
    if "qty" in df.columns:
        try:
            md.update_column("qty", sdtype="numerical")
            log.info("[META] Set 'qty' -> numerical")
        except Exception as e:
            log.warning("[META] Cannot set 'qty' numerical: %s", e)

    # - sku is more like text/ID; set as text for generality.
    if "sku" in df.columns:
        try:
            md.update_column("sku", sdtype="text")
            log.info("[META] Set 'sku' -> text")
        except Exception as e:
            log.warning("[META] Cannot set 'sku' text: %s", e)

    # If an 'id' column exists and appears sufficiently unique, mark as primary key.
    if "id" in df.columns:
        try:
            ser = df["id"]
            if ser.notna().any() and ser.nunique(dropna=True) >= int(len(df) * 0.95):
                md.set_primary_key("id")
                md.update_column("id", sdtype="id")
                log.info("[META] Set 'id' -> primary key & id sdtype")
        except Exception as e:
            log.warning("[META] Cannot set 'id' primary key: %s", e)

    log.info("[META] Final metadata: %s", json.dumps(md.to_dict(), ensure_ascii=False, indent=2))
    return md

# ------------------------------
# Train SDV model and generate samples
# ------------------------------
def _fit_and_sample(df: pd.DataFrame, metadata: SingleTableMetadata, n_rows: int) -> pd.DataFrame:
    """Fit SDV model and sample synthetic rows."""
    synth = GaussianCopulaSynthesizer(metadata)
    synth.fit(df)
    syn = synth.sample(n_rows)
    log.info("[SDV] Generated %d rows", len(syn))
    return syn

# ------------------------------
# Write back to MotherDuck
# ------------------------------
def _save_to_motherduck(con: duckdb.DuckDBPyConnection, catalog: str, target_tbl: str, df: pd.DataFrame):
    """Save the synthetic dataframe into MotherDuck as a new or replaced table."""
    fq_target = f"{catalog}.{target_tbl}" if "." in target_tbl else f"{catalog}.main.{target_tbl}"

    con.register("sdv_tmp_df", df)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.main;")
    con.execute(f"CREATE OR REPLACE TABLE {fq_target} AS SELECT * FROM sdv_tmp_df;")
    con.unregister("sdv_tmp_df")

    n = con.execute(f"SELECT COUNT(*) FROM {fq_target}").fetchone()[0]
    log.info("[SAVE] Wrote -> %s (rows=%s)", fq_target, n)
    return n

# ------------------------------
# Airflow entrypoint
# ------------------------------
def run_sdv_job(**kwargs):
    """Main entrypoint: load → clean → build metadata → fit → sample → save."""
    md_catalog, md_source, md_target, num_rows, token, seed_limit = _get_env()
    con = _connect_md(token)

    # 1) Load & clean
    src_df = _load_source_df(con, md_catalog, md_source, seed_limit)
    if src_df.empty:
        raise RuntimeError("Source dataframe is empty; aborting SDV fit.")

    clean_df = _clean_source_df(src_df)

    # 2) Build metadata
    metadata = _build_metadata(clean_df)

    # 3) Train & sample synthetic data
    syn_df = _fit_and_sample(clean_df, metadata, num_rows)

    # 4) Save to MotherDuck
    wrote = _save_to_motherduck(con, md_catalog, md_target, syn_df)

    # 5) Optionally save metadata JSON (ignore permission errors)
    try:
        meta_path = os.path.join(os.path.dirname(__file__), "walmart_metadata.json")
        metadata.save_to_json(meta_path)
        log.info("Metadata saved to %s", meta_path)
    except Exception as e:
        log.warning("Cannot save metadata json: %s", e)

    log.info("[SDV] Generated %d rows -> %s.%s", wrote, md_catalog, md_target)
    con.close()
    return None

