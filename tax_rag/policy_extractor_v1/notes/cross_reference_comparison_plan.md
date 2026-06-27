# Cross-reference extractor comparison plan

Question:
- Which approach is better for identifying tax-code cross references: REJ16-style extraction or LLM extraction?

Scope:
- section 274
- section 212
- section 162

Evaluation:
- Build a normalized union of REJ16-style and LLM candidate rows for each section.
- Manually label each row as gold=1 or gold=0.
- Compare precision, recall, F1, and integrated micro-average F1.
- F1 is the harmonic mean of precision and recall. Integrated micro-average means pooling rows from all reviewed sections before calculating the final score.

Evaluation rule:
- Consider integrated F1 as the primary summary score.
- Interpret precision and recall distinctly to assess the balance between precise extraction and wider coverage.
- If scores are close, prefer REJ16-style because it is higher-precision and more stable for downstream policy generation.
- Prefer LLM when its recall gain evidently warrants the extra false positives.
- Keep LLM as an auxiliary candidate generator when extra coverage is useful.



