-- Code to deploy cloud function for populating the product_fulfillment table
SA_EMAIL=$(gcloud functions describe load_api_to_motherduck --region=us-central1 --gen2 --format='value(serviceConfig.serviceAccountEmail)')

for secret in project_motherduck_token target_api_test_amisha; do   gcloud secrets add-iam-policy-binding $secret     --member="serviceAccount:${SA_EMAIL}"     --role="roles/secretmanager.secretAccessor"; done

-- Deployment

gcloud run deploy nearby-stores \
  --image "us-central1-docker.pkg.dev/ba882-team4/store-loader/nearby-stores:rev-$(date +%Y%m%d-%H%M%S)" \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --set-secrets MOTHERDUCK_TOKEN=project_motherduck_token:latest,RAPIDAPI_KEY=rapidapi_target_key:latest \
  --update-env-vars MD_CATALOG=project_882

gcloud functions deploy product-search-api-to-duckdb   \
        --gen2   --runtime=python311   \
        --region=us-central1   --source=.  \
        --entry-point=product-search-api-to-duckdb   --trigger-http   \
        --allow-unauthenticated   --set-env-vars=GOOGLE_CLOUD_PROJECT=ba882-team4   \
        --timeout=240s --memory=512MB

gcloud functions deploy load_api_to_motherduck   \
        --gen2   --runtime=python311   \
        --region=us-central1   --source=.  \
        --entry-point=load_api_to_motherduck   --trigger-http   \
        --allow-unauthenticated   --set-env-vars=GOOGLE_CLOUD_PROJECT=ba882-team4   \
        --timeout=240s --memory=512MB
