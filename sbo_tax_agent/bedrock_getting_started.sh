#!/bin/bash

# ==== config ====
REGION="us-east-1"
PROFILE_NAME="cortex-np2948-claude"
MODEL_ID="us.anthropic.claude-haiku-4-5-20251001-v1:0"   # change if you prefer another FM
PROJECT_TAG="cortex"
OWNER_TAG="np2948"
# ================

rm -f {out,payload}.json

aws sts get-caller-identity >/dev/null || { echo "AWS CLI not configured."; exit 1; }

MODEL_ARN=$(aws bedrock list-foundation-models \
 --region "$REGION" \
 --query "modelSummaries[?modelId=='$MODEL_ID'].modelArn | [0]" \
 --output text)

echo "MODEL_ARN=$MODEL_ARN"
[ "$MODEL_ARN" != "None" ] && [ -n "$MODEL_ARN" ] || { echo "Model not found in $REGION"; exit 1; }

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

echo "PROFILE_ARN=$PROFILE_ARN"
echo "PROFILE_ID=$PROFILE_ID"

# describe (any of: name, arn, or id works)
aws bedrock get-inference-profile \
 --region "$REGION" \
 --inference-profile-identifier "$PROFILE_ARN"

# view tags
aws bedrock list-tags-for-resource \
 --region "$REGION" \
 --resource-arn "$PROFILE_ARN"

cat > payload.json <<'JSON'
{
 "anthropic_version": "bedrock-2023-05-31",
 "max_tokens": 120,
 "messages": [
   {"role":"user","content":[{"type":"text","text":"Say hello from my Application Inference Profile and keep it short."}]}
 ]
}
JSON

aws bedrock-runtime invoke-model \
 --region "$REGION" \
 --model-id "$PROFILE_ARN" \
 --body fileb://payload.json \
 --content-type application/json \
 --accept application/json \
 out.json

cat out.json
