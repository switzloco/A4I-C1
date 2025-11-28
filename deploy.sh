#!/bin/bash
# Cloud Run Deployment Script for Education Insights Agent System

set -e  # Exit on error

echo "=========================================="
echo "🚀 Education Insights - Cloud Run Deploy"
echo "=========================================="

# Source environment variables
if [ -f "set_env.sh" ]; then
    echo "📝 Loading environment variables..."
    source set_env.sh
else
    echo "⚠️  set_env.sh not found. Using defaults..."
    export PROJECT_ID="${GCP_PROJECT:-your-project-id}"
    export REGION="us-west1"
    export REPO_NAME="instavibe-agents"
fi

# Configuration
SERVICE_NAME="education-insights-agent"
IMAGE_NAME="education-insights"
ARTIFACT_REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}"

echo ""
echo "Configuration:"
echo "  Project ID: ${PROJECT_ID}"
echo "  Region: ${REGION}"
echo "  Service: ${SERVICE_NAME}"
echo "  Image: ${ARTIFACT_REGISTRY}"
echo ""

# Confirm deployment
read -p "Deploy to Cloud Run? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Deployment cancelled"
    exit 1
fi

echo ""
echo "=========================================="
echo "📦 Step 1: Building Docker Image"
echo "=========================================="

# Build using Cloud Build (faster, uses Google's infrastructure)
gcloud builds submit \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --tag ${ARTIFACT_REGISTRY}:latest \
    --timeout=20m

echo ""
echo "=========================================="
echo "🚢 Step 2: Deploying to Cloud Run"
echo "=========================================="

# Load Maps API key if available
if [ -f "secrets/maps_api_key.txt" ]; then
    GOOGLE_MAPS_API_KEY=$(cat secrets/maps_api_key.txt | tr -d '\n')
    echo "✅ Google Maps API key loaded"
else
    echo "⚠️  No Google Maps API key found - maps will be disabled"
    GOOGLE_MAPS_API_KEY=""
fi

# Load Google AI API key for Gemini access
if [ -f "secrets/google_api_key.txt" ]; then
    GOOGLE_API_KEY=$(cat secrets/google_api_key.txt | tr -d '\n')
    echo "✅ Google AI API key loaded (for Gemini 2.0)"
else
    echo "⚠️  No Google AI API key found - using Vertex AI fallback"
    GOOGLE_API_KEY=""
fi

# Deploy to Cloud Run
gcloud run deploy ${SERVICE_NAME} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --image=${ARTIFACT_REGISTRY}:latest \
    --platform=managed \
    --allow-unauthenticated \
    --memory=2Gi \
    --cpu=1 \
    --timeout=300 \
    --max-instances=5 \
    --min-instances=0 \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID}" \
    --set-env-vars="GOOGLE_GENAI_USE_VERTEXAI=TRUE" \
    --set-env-vars="GOOGLE_CLOUD_LOCATION=${REGION}" \
    --set-env-vars="BIGQUERY_DATASET=education_data" \
    --set-env-vars="GOOGLE_MAPS_API_KEY=${GOOGLE_MAPS_API_KEY}" \
    --set-env-vars="GOOGLE_API_KEY=${GOOGLE_API_KEY}" \
    --service-account=${SERVICE_ACCOUNT_NAME}

echo ""
echo "=========================================="
echo "✅ Deployment Complete!"
echo "=========================================="

# Get the service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --format='value(status.url)')

echo ""
echo "🌐 Service URL: ${SERVICE_URL}"
echo ""
echo "Test endpoints:"
echo "  Health: ${SERVICE_URL}/health"
echo "  Chat UI: ${SERVICE_URL}/"
echo "  API Docs: ${SERVICE_URL}/docs"
echo ""
echo "=========================================="

