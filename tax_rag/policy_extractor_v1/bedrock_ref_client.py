import json
import os

import boto3

region = os.getenv("BEDROCK_REGION", "us-east-1")
model_id = os.getenv("BEDROCK_MODEL_ID")


def complete(prompt, max_tokens=4000):
    if not model_id:
        raise RuntimeError("BEDROCK_MODEL_ID is not set")

    client = boto3.client("bedrock-runtime", region_name=region)
    response = client.converse(
        modelId=model_id,
        messages=[
            {
                "role": "user",
                "content": [{"text": prompt}],
            }
        ],
        inferenceConfig={
            "temperature": 0,
            "maxTokens": max_tokens,
        },
    )

    parts = response["output"]["message"]["content"]
    texts = [part["text"] for part in parts if "text" in part]
    if not texts:
        raise RuntimeError(json.dumps(parts, ensure_ascii=False))
    return "\n".join(texts).strip()


def complete_json(prompt, max_tokens=4000):
    text = complete(prompt, max_tokens=max_tokens)

    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        raise ValueError(text)

    return json.loads(text[start : end + 1])
