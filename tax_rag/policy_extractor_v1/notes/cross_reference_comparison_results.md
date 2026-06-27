# Cross-reference extractor comparison results

Scope:
- section 274
- section 212
- section 162

Integrated results:

| method | tp | fp | fn | precision | recall | F1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| REJ16-style | 110 | 4 | 39 | 0.9649 | 0.7383 | 0.8365 |
| LLM | 117 | 58 | 32 | 0.6686 | 0.7852 | 0.7222 |

Interpretation:
- REJ16-style extraction scores better as a whole by integrated F1.
- REJ16-style extraction has much higher precision, which makes it more stable for downstream policy generation.
- LLM extraction has higher recall, but the recall gain is modest compared with the increase in false positives.
- Insight: REJ16-style extraction should be the main reference extractor, while LLM extraction is better to function as an auxiliary candidate generator.
