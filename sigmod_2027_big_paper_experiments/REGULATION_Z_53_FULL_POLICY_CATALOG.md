# Regulation Z §1026.53: 40-proposition policy catalog

This replaces the earlier 29-policy appendix with **40 proposition-aligned entries**:
**31 executable Passant DFC policies and nine entries without an independent constraint**.
The IDs preserve the analysis's P01–P40 inventory. A catalog entry is not necessarily
an independently enforceable obligation. Permissions and scope exclusions must not become
new prohibitions merely to reach a count of 40 executable policies.

Source: [§1026.53 and its official interpretation](https://www.consumerfinance.gov/rules-policy/regulations/1026/53/).
These are case-study formalizations, not a determination of legal completeness. In particular,
several executable checks implement stated defaults rather than fully resolving the source's
open-ended language. The v3 benchmark now uses all 31 predicates with a benchmark-specific sink and extended
evidence; see `REGULATION_Z_V3.md`. Older five-policy results remain historical.

## Runnable implementation

- `src/vldb_experiments/strategies/regulation_z_53_full_policy_catalog.py`: 40 entries, legal locators, constraints, explanations, limitations, and PGN export.
- `src/vldb_experiments/strategies/regulation_z_53_runtime.py`: relational schema, implemented derivations, and transaction helpers.
- `tests/test_regulation_z_53_full_policy_catalog.py`: positive and negative execution cases using DuckDB and the actual Passant rewriter.

`FULL_REGULATION_Z_53_POLICY_SPECS` contains all 40 entries.
`build_full_regulation_z_53_policies()` builds the 31 executable policies.
Calling `build_policy()` or `to_pgn()` on a supporting entry raises an error;
there are no always-true placeholder policies.

Initialize an empty DuckDB connection with `create_schema(conn)`, load authoritative evidence
and a proposed batch, then call `submit_allocation(conn, batch_id)`. For a determined error,
load the required correction items and actual credit/dispatch evidence, then call
`complete_correction(conn, error_id, checkpoint_date)`.
Both helpers own their transaction. `False` means DFC rejected the proposed acceptance and
publication was rolled back. `EvidenceError` means evidence is incomplete or the input is
outside this adapter's supported model; it is not a finding of unlawful conduct.
The tests contain complete runnable evidence fixtures.

Each actual policy has source `regz_context`, sink `regz_acceptances`, and resolution `REMOVE`.
The adapter computes the source facts, then sends an acceptance-row INSERT through Passant.
If acceptance succeeds, all allocation rows and classifications are published atomically;
otherwise none are published. Reallocations preserve history and atomically replace the
current payment head. Correction completion is likewise gated. This avoids partially posting
an allocation after removing just its violating rows.

**Trusted boundary:** the adapter, evidence tables, and deterministic derivations are trusted.
Untrusted callers must not write directly to `regz_context`, `regz_acceptances`, posted tables,
or payment heads. This module does not install database permissions or intercept every
possible SQL write. The policies protect the supplied transaction workflow, not arbitrary
alternate database access.

## All 40 entries

“Check” means an executable constraint exists, including checks subject to the listed
assumptions. “Support” means its meaning is incorporated without an independent rejection rule;
P24 additionally flags incomplete external-definition coverage.

| ID | Proposition | Authority | Implementation | Meaning / assumption or limitation |
|---|---|---|---|---|
| P01 | Highest-APR priority | 53(a) | Check | Exhaust the highest effective-rate group before lower groups. **Limit:** Dispute protection takes precedence; deferred-window and request branches are separate. |
| P02 | Descending remainder priority | 53(a) | Check | Compare every remaining higher/lower rate pair; equal rates remain unordered. |
| P03 | Deferred-program applicability | 53(b)(1); comment 53(b)-1 | Check | Runtime derives qualifying programs from conditional accrued interest and the grace/zero exclusions. **Limit:** External contract facts are trusted; this checks the proposed program classification. |
| P04 | Ordinary default outside the window | 53(b)(1)(i) | Check | Active programs outside the priority window use ordinary ordering. |
| P05 | Deferred-window priority | 53(b)(1)(i) | Check | Detect ordinary allocations while eligible priority-program capacity remains. **Limit:** Simultaneously eligible deferred balances form one unordered priority group; disputed capacity comes last. |
| P06 | Remainder after deferred priority | 53(b)(1)(i) | Check | The ordinary balances left after the priority group retain descending effective-rate order. |
| P07 | Optional deferred-program request | 53(b)(1)(ii) | Check | A request may allocate among any account balances while a qualifying program applies. **Limit:** Targets need not themselves be deferred. Only recorded, specific instructions are supported. |
| P08 | Optional secured-balance request | 53(b)(2) | Check | Every positive requested amount must target a secured balance. **Limit:** Security-interest facts are externally established. |
| P09 | Excess-payment scope | comment 53-1 | Support | Only excess allocation enters rate-priority comparisons; minimum allocations are separate. **Limit:** Minimum accounting is supplied, not inferred by subtracting the full minimum from each payment. |
| P10 | Minimum amount outside scope | comment 53-1 | Support | Accept the supplied minimum portion without deriving a minimum-payment formula. **Limit:** Other applicable law is outside this adapter. |
| P11 | Minimum allocation outside scope | comment 53-1 | Support | Rate priority does not constrain minimum allocations; dispute protection still examines the whole payment. |
| P12 | Optional minimum allocation method | comment 53-1 | Support | Either a rate-ordered or other minimum allocation is allowed. **Limit:** A permission is preserved by absence of an additional restriction. |
| P13 | Permitted snapshot date | comment 53-2 | Check | Both amounts and rates are read from the selected complete snapshot. **Limit:** Snapshots must be authoritative complete account snapshots. |
| P14 | Consistent reference-day practice | comment 53-2 | Check | Compute the date from the effective practice's cycle-end, credit-date, or offset convention. **Limit:** Operational choice, not a complete interpretation of 'generally'. |
| P15 | Permitted practice adjustments | comment 53-2 | Check | Changed conventions require a recorded authorization and effective date. **Limit:** Cannot determine legal acceptability of 'from time to time'; authorization evidence does not resolve that legal standard. |
| P16 | Residual after historical snapshot | comment 53-2.ii | Check | After dispute/window priorities, exhaust historical lots before later lots. **Limit:** Generalization of the single later-purchase example; later lots use their payment-date APR. |
| P17 | Minimize disputed reduction | comment 53-3 | Check | Compute reduction for minimum plus excess against all outstanding undisputed capacity. **Limit:** Dispute protection overrides request and program priority. Dispute merits are external. |
| P18 | Allow unavoidable disputed residual | comment 53-3.ii | Check | When disputed reduction is unavoidable, require exhaustion of all other capacity. P17 separately rejects avoidable disputed reduction. |
| P19 | Error-correction credit | comment 53-3.ii | Check | At completion, check each required principal/charge item against actual posted credits. **Limit:** Required charge attribution and checkpoint deadline come from external section 1026.13 requirements; this does not initiate a credit. |
| P20 | Correction notice dispatch | comment 53-3.ii | Check | Require actual timely dispatch to the specified recipient, linked to the determination. **Limit:** Notice content certification is trusted external evidence; no autonomous dispatch or deadline scheduler. |
| P21 | Equal-rate discretion | comment 53-4 | Support | Strict rate comparisons impose no FIFO/pro-rata tie breaker; stronger dispute/program rules remain. **Limit:** Uses the enumerated exceptions, not a claim to resolve every possible meaning of 'generally'. |
| P22 | Grouped or separate balances | comment 53-4 | Support | Presentation may group rows; the underlying lots retain program, security, and dispute attributes. **Limit:** Do not merge away facts needed by other checks. |
| P23 | Zero allocation APR for deferred interest | comment 53-4 | Check | Compare proposed effective APRs with the zero-or-snapshot-rate derivation. **Limit:** The contractual interest accrual rate is not changed. |
| P24 | Incorporated deferred-interest definition | comment 53(b)-1 | Support | Local terms implement the explicit conditional-interest description. **Limit:** Full equivalence with section 1026.16(h)(2) and its commentary is not implemented or claimed. |
| P25 | Interim payments irrelevant to applicability | comment 53(b)-1 | Check | Program classification does not depend on its interim-payment-required attribute. |
| P26 | Grace-period exclusion | comment 53(b)-1 | Check | The classifier excludes grace periods. **Limit:** Checks a proposed classification; does not add an allocation-priority obligation. |
| P27 | Temporary-zero exclusion | comment 53(b)-1 | Check | The classifier excludes temporary zero APR unless accrued interest may become payable. **Limit:** External rate-change legality is not checked. |
| P28 | Correct counted cycles | comment 53(b)-2 | Check | Compute the start from the latest two cycle due dates no later than expiration, not a supplied expected value. **Limit:** At least two actual cycles through the expiration cycle must be supplied. |
| P29 | Early-expiration partial cycle | comment 53(b)-2 | Check | Priority remains in effect after the two counted cycles until expiration. **Limit:** Expiration dates are inclusive contract payoff dates in this model. |
| P30 | Issuer may decline requests | comment 53(b)-3.i | Support | Default mode remains available even when a valid request exists. **Limit:** No policy requires the issuer to elect the request branch. |
| P31 | Decline requests failing issuer requirements | comment 53(b)-3.i | Support | Default mode remains available regardless of why the issuer declined. **Limit:** This permission does not require a separate rejection policy or mandate issuer requirements. |
| P32 | No request uses default | comment 53(b)-3.i | Check | A missing or unsubmitted request cannot activate an exception. **Limit:** Complete request capture is assumed. |
| P33 | Unfulfillable request and replacement | comment 53(b)-3.i | Check | Check sums, nonnegative amounts, targets, capacities, and submitted replacement links. **Limit:** Operational definition of inability; other meanings of 'cannot comply' remain unresolved. |
| P34 | Affirmative specific contact | comment 53(b)-3.ii.A | Check | Recognize oral/electronic/written contact with explicit structured instructions. **Limit:** Interpretation of a conversation into instructions is external evidence processing. |
| P35 | Completed submitted request form | comment 53(b)-3.ii.B | Check | An actual completed/submitted form, not default coupon text, supports the exception. |
| P36 | Request to reallocate previous payment | comment 53(b)-3.ii.C | Check | Verify a prior accepted default allocation and a later submitted specific request. **Limit:** Caller supplies the authoritative pre-payment balance snapshot for replacement; replacement is atomic. |
| P37 | Preprinted agreement is insufficient | comment 53(b)-3.iii.A | Check | Reject reliance on agreement deemed consent; an independent contact is a different evidence record. |
| P38 | Untouched preselection is insufficient | comment 53(b)-3.iii.B | Check | Reject reliance on an untouched issuer-selected checkbox. |
| P39 | Default payment coupon is insufficient | comment 53(b)-3.iii.C | Check | Reject reliance on default coupon text or selection. |
| P40 | Imposed acceptance is insufficient | comment 53(b)-3.iii.D | Check | Reject requests based on imposed acceptance. **Limit:** Whether a condition was imposed is externally evidenced. |

## Required evidence and explicit computations

There is no caller-supplied `is_compliant` or expected-violation flag. The adapter computes
numeric comparison results from these relations:

| Facts | Relations / calculation |
|---|---|
| Account scope and payment | `regz_accounts` identifies a consumer open-end credit-card plan not secured by a home; `regz_payments` supplies credit date, cycle end, total, and the minimum portion attributable to this payment. |
| Complete account history | `regz_snapshot_sets` certifies snapshot presence; `regz_balances` supplies dated atomic lots, amount, APR, protected amount, security, and program. Missing balance rows cannot be discovered from the marker alone: completeness is trusted. |
| Programs and classifications | `regz_programs` supplies contractual accrual, conditional waiver, dates, grace and temporary-zero terms. A qualifying program accrues interest waived on payoff, excludes grace periods, and excludes temporary zero offers without retroactive interest. Interim-payment requirements do not affect this computation. Proposed `regz_program_classifications` are compared with it before posting. |
| Reference-day practice | `regz_practices` supplies effective conventions. Expected date is cycle end, credited date, or cycle end plus the recorded offset. Convention changes require recorded authorization. This does not decide the legal meaning of “generally” or permissible change frequency. |
| Window and effective APR | `regz_cycles` supplies actual contiguous cycles and due dates. The latest two cycles whose due dates are no later than expiration determine the window start. Priority continues through the inclusive contractual expiration date. Qualifying active deferred programs receive zero allocation APR outside their priority treatment. |
| Allocation comparisons | `regz_proposals` and complete `regz_proposal_lines` specify minimum and excess separately. After minimum placement, lots are split into remaining disputed and undisputed capacity. Pairwise exhaustion checks compare deferred priority, historical versus later lots, and strictly higher APRs; ties are unrestricted. Dispute protection is checked against the whole payment. |
| Requests | `regz_requests` records submission, evidence basis, affirmative action, issuer election, imposed conditions, and explicit supersession. `regz_request_lines` supplies specific amounts and targets. The adapter checks sums, capacity, target security where required, replacement links, and exact allocation agreement. Default allocation remains available when the issuer declines. |
| Corrections | `regz_error_obligations` supplies determination, deadline, and recipient; `regz_error_items` supplies each required principal/charge amount. Credits are summed separately per item through the earlier of checkpoint and deadline. A qualifying dispatch must identify the error, recipient, correction type, timely actual sent date, and nonempty content-evidence reference. |

Contract terms, scope classifications, dispute merits, security interests, affirmative consumer
conduct, whether a condition was imposed, notice-content sufficiency, and legally required
correction amounts/deadlines require authoritative external evidence. A row recording an event
is assumed to attest that it actually occurred. DFC does not establish that truth from the row's
existence. Natural-language instructions must be converted into structured specific instructions
by a trusted process; no LLM verifier is implemented here.

## Choices and remaining gaps

- **P14/P15:** following a recorded practice and requiring authorization for changes are operational defaults. Whether a deviation is consistent with “generally,” or changes occur acceptably “from time to time,” remains unresolved. These checks must not be reported as full legal enforcement.
- **P24:** the incorporated definition in §1026.16(h)(2) and its commentary is not fully implemented. Local program terms implement only the explicit description used here.
- **P33:** inability is modeled as invalid totals, nonexistent targets, insufficient capacity, or an explicit submitted superseding request. Other reasons an issuer “cannot comply” require further interpretation.
- **P19/P20:** completion requires actual credit and dispatch evidence, but this does not force the issuer to invoke the completion workflow or act by a deadline. A scheduler/escalation mechanism and external §1026.13 determinations are required for an end-to-end timely-action guarantee.
- **P05/P16/P17/P29:** simultaneous priority programs share an unordered group; historical lots precede later lots after dispute/window treatment; dispute protection overrides request and program priorities; payoff expiration dates are inclusive. These are explicit model choices, not independently validated legal conclusions.
- **P09–P12, P21–P22, P30–P31:** permissions and scope are respected by the absence of inappropriate restrictions. They do not produce additional standalone constraints. P24 is the ninth non-executable entry.

Supported input restrictions are separate from legal restrictions: money uses integer cents;
new principal uses a new lot ID; existing lots cannot grow between the selected snapshot and
credit date; snapshots and allocation lines must be complete; enough cycle history must be
supplied; requests specify the entire excess allocation; and the whole payment must fit the
outstanding balances. Credit-balance overpayments, partial request instructions with implicit
remainders, or other unsupported cases require another workflow. Reallocation evidence must
use the authoritative original pre-payment snapshot, not balances already reduced by the
previous allocation. Program activity is evaluated at the payment's credited date.

The adapter validates conservation, capacity, complete evidence, and nonnegative allocations
as input integrity conditions. These are not extra regulatory propositions or a larger denominator.
The implementation replaces the old appendix schema rather than migrating an existing database.

## Validation and reporting

Run from `sigmod_2027_big_paper_experiments`:

```sh
source setup_local_smokedduck.sh
.venv/bin/python -m pytest tests/test_regulation_z_53_full_policy_catalog.py tests/test_regulation_z_v3_benchmark.py tests/test_regulation_z_payment_allocation_topk.py -q
.venv/bin/python -m ruff check src/ tests/
```

The tests exercise real DFC acceptance/rejection, alternative lawful choices in the modeled
cases, proposed classifications, historical snapshots, deferred windows, request evidence and
replacement, actual correction evidence, and atomic publication. Passing these tests does not
validate legal completeness or prove every possible query path is protected. The 31/40 count
is a count of executable proposition-associated checks, not a claim of 77.5% complete legal
coverage; several checks depend on the defaults and gaps above.
