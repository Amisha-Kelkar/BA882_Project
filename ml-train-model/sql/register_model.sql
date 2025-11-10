-- ============================================
-- Register the stockout prediction model
-- Target: project_882.mlops.model
-- Note: model_id is fixed for this business model
--       and does not need to change every run.
-- ============================================

INSERT INTO project_882.mlops.model (
  model_id,
  name,
  business_problem,
  ticket_number,
  owner,
  created_at
)
VALUES (
  'stockout_v1',                           -- fixed unique model id
  'Stockout Predictor',                    -- model name
  'Predict stockouts within next 7 days',  -- business goal
  'HW-TEAM4',                              -- assignment or task ID
  'Team 4',                                -- your team or your name
  NOW()                                    -- timestamp
)
ON CONFLICT (model_id) DO NOTHING;         -- skip if model already exists
