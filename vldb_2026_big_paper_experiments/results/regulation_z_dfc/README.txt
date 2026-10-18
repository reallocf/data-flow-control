Regulation Z generated DFC policies

31 individual PGN policy files, exported from build_regulation_z_53_policies().
These use the current v3 binding: SOURCE regz_context, SINK payment_allocations
AS allocation, ON FAIL REMOVE. Predicates are unchanged from the live catalog.

Pxx filenames preserve the proposition identifiers. Gaps are intentional:
P09-P12, P21-P22, P24, P30-P31 are supporting entries without standalone policies.
policies.json records all 31 definitions, legal authorities, limitations, the
nine supporting entries, and SHA-256 hashes of the source modules.

Each PGN file can be loaded individually with Policy.from_pgn().
All 31 exports were parsed and compared against the generated Policy objects.

These policies depend on authoritative derived regz_context rows and the
trusted preparation/workflow described in REGULATION_Z_V3.md and
REGULATION_Z_53_FULL_POLICY_CATALOG.md at the project root. They are not
self-contained substitutes for those derivations or a claim of full legal coverage.
The general workflow uses regz_acceptances instead; this export uses the v3 sink.
