# ================================================================
# train_target_sdv.py
# Train an SDV CTGAN model on flattened Target Product Fulfillment data
# ================================================================
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata

# ------------------------------------------------
# Load dataset
# ------------------------------------------------
df = pd.read_csv("target_product_fulfillment.csv")
print(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns.")

# ------------------------------------------------
# Inject realistic 7-day rolling date logic
# ------------------------------------------------
today = date.today()
past_week = [today - timedelta(days=i) for i in range(7)]

df["record_date"] = np.random.choice(past_week, size=len(df))
df["load_date"] = today
df["load_timestamp"] = [
    datetime.utcnow() - timedelta(seconds=int(x))
    for x in np.random.randint(0, 7 * 24 * 3600, size=len(df))
]
df["min_delivery_date"] = pd.to_datetime(df["record_date"]) + pd.to_timedelta(
    np.random.randint(1, 3, size=len(df)), unit="D"
)
df["max_delivery_date"] = df["min_delivery_date"] + pd.to_timedelta(
    np.random.randint(1, 3, size=len(df)), unit="D"
)

print("Date preview:")
print(df[["record_date", "load_date", "min_delivery_date", "max_delivery_date"]].head())

# ------------------------------------------------
# Clean + fill missing values for stability
# # ------------------------------------------------
df = df.fillna({
    col: "" if df[col].dtype == "object" else 0
    for col in df.columns
})
df = df.replace([np.inf, -np.inf], np.nan)

# Fill invalid datetime values in 'cutoff_time' with a placeholder
df['cutoff_time'] = pd.to_datetime(df['cutoff_time'], errors='coerce')
df['cutoff_time'] = df['cutoff_time'].fillna(pd.to_datetime('1900-01-01'))


# ------------------------------------------------
# Detect metadata
# ------------------------------------------------
metadata = SingleTableMetadata()
metadata.detect_from_dataframe(df)

# Define sdtypes explicitly for key fields
for c in ["record_date", "load_date", "min_delivery_date", "max_delivery_date"]:
    if c in df.columns:
        metadata.update_column(c, sdtype="datetime")
for c in [col for col in df.columns if c.startswith("is_") or c.startswith("notify_")]:
    metadata.update_column(c, sdtype="boolean")

# ---- Force IDs & codes to categorical ----
id_columns = [
    "fulfillment_product_id",
    "product_id",
    "scheduled_delivery_location_id",
    "delivery_location",
    "store_id",
    "tcin_product_id",
    "product_tcin"
]
for c in id_columns:
    if c in df.columns:
        metadata.update_column(c, sdtype="categorical")
print("Metadata schema updated:")
print(metadata.to_dict()["columns"])

# ------------------------------------------------
# Train CTGAN model
# ------------------------------------------------
synth = CTGANSynthesizer(
    metadata,
    epochs=300,
    batch_size=500,
    verbose=True
)

print("Training CTGAN...")
synth.fit(df)
print("Training complete!")

# ------------------------------------------------
# Save model & metadata
# ------------------------------------------------
synth.save("target_full_synth.pkl")
metadata.save_to_json("target_full_metadata.json")
print("Model + metadata saved")
