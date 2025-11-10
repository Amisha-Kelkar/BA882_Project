-- ============================================
-- Auto-register dataset snapshot for stockout model
-- Target: project_882.mlops.dataset
-- dataset_id and data_version are generated dynamically
-- based on the current timestamp.
-- ============================================

INSERT INTO project_882.mlops.dataset (
  dataset_id,
  model_id,
  data_version,
  gcs_path,
  row_count,
  feature_count,
  created_at
)
SELECT
  -- Generate a unique dataset_id using current timestamp
  'stockout_ds_' || strftime(NOW(), '%Y%m%d%H%M%S') AS dataset_id,

  -- Link to the registered model
  'stockout_v1' AS model_id,

  -- Use current date as dataset version
  strftime(NOW(), '%Y-%m-%d') AS data_version,

  -- Logical path to the training dataset
  'project_882.ai_datasets.stockout_features' AS gcs_path,

  -- Count number of rows in the dataset
  COUNT(*) AS row_count,

  -- Count number of features (columns)
  (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_catalog = 'project_882'
      AND table_schema  = 'ai_datasets'
      AND table_name    = 'stockout_features'
  ) AS feature_count,

  -- Timestamp
  NOW() AS created_at

FROM project_882.ai_datasets.stockout_features
ON CONFLICT (dataset_id) DO UPDATE SET
  row_count     = EXCLUDED.row_count,
  feature_count = EXCLUDED.feature_count,
  created_at    = NOW();
