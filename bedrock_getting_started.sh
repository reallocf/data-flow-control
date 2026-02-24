#!/bin/bash

# ==== config ====
REGION="us-east-1"
PROFILE_NAME="data-flow-control-cgs2161-claude-opus-4-6"
MODEL_ARN="arn:aws:bedrock:us-east-1:920736616554:inference-profile/global.anthropic.claude-opus-4-6-v1"   # change if you prefer another FM
PROJECT_TAG="data-flow-control"
OWNER_TAG="cgs2161"
# ================

rm -f {out,payload}.json

aws sts get-caller-identity >/dev/null || { echo "AWS CLI not configured."; exit 1; }

# MODEL_ARN=$(aws bedrock list-foundation-models \
#  --region "$REGION" \
#  --query "modelSummaries[?modelId=='$MODEL_ID'].modelArn | [0]" \
#  --output text)

# echo "MODEL_ARN=$MODEL_ARN"
# [ "$MODEL_ARN" != "None" ] && [ -n "$MODEL_ARN" ] || { echo "Model not found in $REGION"; exit 1; }

aws bedrock create-inference-profile \
 --region "$REGION" \
 --inference-profile-name "$PROFILE_NAME" \
 --model-source copyFrom="$MODEL_ARN" \
 --tags '[{"key":"project","value":"'"$PROJECT_TAG"'"},{"key":"billing-tag1","value":"'"$OWNER_TAG"'"}]'

# capture your profile's ARN and ID
PROFILE_ARN=$(aws bedrock list-inference-profiles \
 --region "$REGION" \
 --type APPLICATION \
 --query "inferenceProfileSummaries[?inferenceProfileName=='$PROFILE_NAME'].inferenceProfileArn | [0]" \
 --output text)

PROFILE_ID=$(aws bedrock list-inference-profiles \
 --region "$REGION" \
 --type APPLICATION \
 --query "inferenceProfileSummaries[?inferenceProfileName=='$PROFILE_NAME'].inferenceProfileId | [0]" \
 --output text)
