# Cross-reference extractor status
Earlier comparison numbers describe an older implementation and are not the current section 274 result.
Current reviewed section 274 result:
| method | gold | predicted | exact | wrong | missing | extra |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| REJ16-style | 123 | 123 | 123 | 0 | 0 | 0 |
This is an in-sample result because section 274 was used while adapting the structured rules.
The project direction is to use the structured REJ16-style method as the primary cross-reference layer. A model can be used as a fallback for unusual or ambiguous cases.
The next system step is to use resolved edges to expand the legal context supplied to policy generation.
