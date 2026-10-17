"""Proposition-aligned catalog; supporting propositions are not fake constraints.

The runnable adapter and explicit derivations live in regulation_z_53_runtime.
The v3 benchmark also reuses these predicates with its own update sink.
"""

from __future__ import annotations

from dataclasses import dataclass

from data_flow_control import Policy, Resolution


@dataclass(frozen=True)
class RegulationZ53PolicySpec:
    number: int
    name: str
    authority: str
    constraint: str | None
    explanation: str
    limitation: str = ""

    @property
    def proposition_id(self) -> str:
        return f"P{self.number:02d}"

    @property
    def sources(self) -> tuple[str, ...]:
        return ("regz_context",) if self.constraint else ()

    def build_policy(self) -> Policy:
        if self.constraint is None:
            raise ValueError(
                f"{self.proposition_id} is supporting meaning, not an independent check"
            )
        return Policy(
            sources=list(self.sources),
            sink="regz_acceptances",
            constraint=self.constraint,
            on_fail=Resolution.REMOVE,
            description=f"{self.proposition_id}: {self.name}",
        )

    def to_pgn(self) -> str:
        if self.constraint is None:
            raise ValueError(f"{self.proposition_id} has no standalone PGN")
        return (
            "SOURCE regz_context\nSINK regz_acceptances\n"
            f"CONSTRAINT\n    {self.constraint}\nON FAIL REMOVE"
        )


def _allocation(expression: str) -> str:
    # Preserve the OR as one operand when Passant appends policies with AND.
    return f"(regz_context.kind <> 'allocation' OR ({expression}))"


def _default(expression: str) -> str:
    return _allocation(f"regz_context.method <> 'default' OR ({expression})")


def _request(expression: str) -> str:
    return _allocation(f"regz_context.method = 'default' OR ({expression})")


# Every numeric ID is the ID in the frozen 40-proposition inventory, not a new
# decomposition chosen to match the implementation. None means no independent
# rejection rule; explanation records the actual supporting implementation.
_ROWS = (
    (
        1,
        "Highest-APR priority",
        "53(a)",
        _default("regz_context.highest_violations = 0"),
        "Exhaust the highest effective-rate group before lower groups.",
        "Dispute protection takes precedence; deferred-window and request branches are separate.",
    ),
    (
        2,
        "Descending remainder priority",
        "53(a)",
        _default("regz_context.remaining_violations = 0"),
        "Compare every remaining higher/lower rate pair; equal rates remain unordered.",
        "",
    ),
    (
        3,
        "Deferred-program applicability",
        "53(b)(1); comment 53(b)-1",
        _allocation("regz_context.classification_mismatches = 0"),
        "Runtime derives qualifying programs from conditional accrued interest and the grace/zero exclusions.",
        "External contract facts are trusted; this checks the proposed program classification.",
    ),
    (
        4,
        "Ordinary default outside the window",
        "53(b)(1)(i)",
        _default(
            "regz_context.active_programs = 0 OR regz_context.window_programs > 0 OR regz_context.ordinary_violations = 0"
        ),
        "Active programs outside the priority window use ordinary ordering.",
        "",
    ),
    (
        5,
        "Deferred-window priority",
        "53(b)(1)(i)",
        _default("regz_context.deferred_violations = 0"),
        "Detect ordinary allocations while eligible priority-program capacity remains.",
        "Simultaneously eligible deferred balances form one unordered priority group; disputed capacity comes last.",
    ),
    (
        6,
        "Remainder after deferred priority",
        "53(b)(1)(i)",
        _default("regz_context.window_programs = 0 OR regz_context.ordinary_violations = 0"),
        "The ordinary balances left after the priority group retain descending effective-rate order.",
        "",
    ),
    (
        7,
        "Optional deferred-program request",
        "53(b)(1)(ii)",
        _allocation(
            "regz_context.method <> 'deferred_request' OR (regz_context.active_programs > 0 AND regz_context.issuer_elected AND regz_context.request_mismatches = 0)"
        ),
        "A request may allocate among any account balances while a qualifying program applies.",
        "Targets need not themselves be deferred. Only recorded, specific instructions are supported.",
    ),
    (
        8,
        "Optional secured-balance request",
        "53(b)(2)",
        _allocation(
            "regz_context.method <> 'secured_request' OR (regz_context.unsecured_request_targets = 0 AND regz_context.issuer_elected AND regz_context.request_mismatches = 0)"
        ),
        "Every positive requested amount must target a secured balance.",
        "Security-interest facts are externally established.",
    ),
    (
        9,
        "Excess-payment scope",
        "comment 53-1",
        None,
        "Only excess allocation enters rate-priority comparisons; minimum allocations are separate.",
        "Minimum accounting is supplied, not inferred by subtracting the full minimum from each payment.",
    ),
    (
        10,
        "Minimum amount outside scope",
        "comment 53-1",
        None,
        "Accept the supplied minimum portion without deriving a minimum-payment formula.",
        "Other applicable law is outside this adapter.",
    ),
    (
        11,
        "Minimum allocation outside scope",
        "comment 53-1",
        None,
        "Rate priority does not constrain minimum allocations; dispute protection still examines the whole payment.",
        "",
    ),
    (
        12,
        "Optional minimum allocation method",
        "comment 53-1",
        None,
        "Either a rate-ordered or other minimum allocation is allowed.",
        "A permission is preserved by absence of an additional restriction.",
    ),
    (
        13,
        "Permitted snapshot date",
        "comment 53-2",
        _allocation(
            "regz_context.snapshot_date >= regz_context.cycle_end AND regz_context.snapshot_date <= regz_context.credited_date"
        ),
        "Both amounts and rates are read from the selected complete snapshot.",
        "Snapshots must be authoritative complete account snapshots.",
    ),
    (
        14,
        "Consistent reference-day practice",
        "comment 53-2",
        _allocation("regz_context.snapshot_date = regz_context.expected_snapshot_date"),
        "Compute the date from the effective practice's cycle-end, credit-date, or offset convention.",
        "Operational choice, not a complete interpretation of 'generally'.",
    ),
    (
        15,
        "Permitted practice adjustments",
        "comment 53-2",
        _allocation("regz_context.unauthorized_changes = 0"),
        "Changed conventions require a recorded authorization and effective date.",
        "Cannot determine legal acceptability of 'from time to time'; authorization evidence does not resolve that legal standard.",
    ),
    (
        16,
        "Residual after historical snapshot",
        "comment 53-2.ii",
        _default("regz_context.snapshot_violations = 0"),
        "After dispute/window priorities, exhaust historical lots before later lots.",
        "Generalization of the single later-purchase example; later lots use their payment-date APR.",
    ),
    (
        17,
        "Minimize disputed reduction",
        "comment 53-3",
        _allocation(
            "regz_context.protected_reduction <= regz_context.unavoidable_protected_reduction"
        ),
        "Compute reduction for minimum plus excess against all outstanding undisputed capacity.",
        "Dispute protection overrides request and program priority. Dispute merits are external.",
    ),
    (
        18,
        "Allow unavoidable disputed residual",
        "comment 53-3.ii",
        _allocation(
            "regz_context.unavoidable_protected_reduction = 0 OR regz_context.protected_reduction = 0 OR regz_context.other_capacity_remaining = 0"
        ),
        "When disputed reduction is unavoidable, require exhaustion of all other capacity. P17 separately rejects avoidable disputed reduction.",
        "",
    ),
    (
        19,
        "Error-correction credit",
        "comment 53-3.ii",
        "(regz_context.kind <> 'correction' OR regz_context.missing_credit_cents = 0)",
        "At completion, check each required principal/charge item against actual posted credits.",
        "Required charge attribution and checkpoint deadline come from external section 1026.13 requirements; this does not initiate a credit.",
    ),
    (
        20,
        "Correction notice dispatch",
        "comment 53-3.ii",
        "(regz_context.kind <> 'correction' OR regz_context.notice_count > 0)",
        "Require actual timely dispatch to the specified recipient, linked to the determination.",
        "Notice content certification is trusted external evidence; no autonomous dispatch or deadline scheduler.",
    ),
    (
        21,
        "Equal-rate discretion",
        "comment 53-4",
        None,
        "Strict rate comparisons impose no FIFO/pro-rata tie breaker; stronger dispute/program rules remain.",
        "Uses the enumerated exceptions, not a claim to resolve every possible meaning of 'generally'.",
    ),
    (
        22,
        "Grouped or separate balances",
        "comment 53-4",
        None,
        "Presentation may group rows; the underlying lots retain program, security, and dispute attributes.",
        "Do not merge away facts needed by other checks.",
    ),
    (
        23,
        "Zero allocation APR for deferred interest",
        "comment 53-4",
        _allocation("regz_context.effective_apr_mismatches = 0"),
        "Compare proposed effective APRs with the zero-or-snapshot-rate derivation.",
        "The contractual interest accrual rate is not changed.",
    ),
    (
        24,
        "Incorporated deferred-interest definition",
        "comment 53(b)-1",
        None,
        "Local terms implement the explicit conditional-interest description.",
        "Full equivalence with section 1026.16(h)(2) and its commentary is not implemented or claimed.",
    ),
    (
        25,
        "Interim payments irrelevant to applicability",
        "comment 53(b)-1",
        _allocation("regz_context.interim_payment_omissions = 0"),
        "Program classification does not depend on its interim-payment-required attribute.",
        "",
    ),
    (
        26,
        "Grace-period exclusion",
        "comment 53(b)-1",
        _allocation("regz_context.grace_classifications = 0"),
        "The classifier excludes grace periods.",
        "Checks a proposed classification; does not add an allocation-priority obligation.",
    ),
    (
        27,
        "Temporary-zero exclusion",
        "comment 53(b)-1",
        _allocation("regz_context.zero_offer_classifications = 0"),
        "The classifier excludes temporary zero APR unless accrued interest may become payable.",
        "External rate-change legality is not checked.",
    ),
    (
        28,
        "Correct counted cycles",
        "comment 53(b)-2",
        _allocation("regz_context.window_start_mismatches = 0"),
        "Compute the start from the latest two cycle due dates no later than expiration, not a supplied expected value.",
        "At least two actual cycles through the expiration cycle must be supplied.",
    ),
    (
        29,
        "Early-expiration partial cycle",
        "comment 53(b)-2",
        _default("regz_context.early_expiry_violations = 0"),
        "Priority remains in effect after the two counted cycles until expiration.",
        "Expiration dates are inclusive contract payoff dates in this model.",
    ),
    (
        30,
        "Issuer may decline requests",
        "comment 53(b)-3.i",
        None,
        "Default mode remains available even when a valid request exists.",
        "No policy requires the issuer to elect the request branch.",
    ),
    (
        31,
        "Decline requests failing issuer requirements",
        "comment 53(b)-3.i",
        None,
        "Default mode remains available regardless of why the issuer declined.",
        "This permission does not require a separate rejection policy or mandate issuer requirements.",
    ),
    (
        32,
        "No request uses default",
        "comment 53(b)-3.i",
        _allocation("regz_context.request_present OR regz_context.method = 'default'"),
        "A missing or unsubmitted request cannot activate an exception.",
        "Complete request capture is assumed.",
    ),
    (
        33,
        "Unfulfillable request and replacement",
        "comment 53(b)-3.i",
        _request(
            "regz_context.request_feasibility_errors = 0 AND regz_context.superseding_requests = 0"
        ),
        "Check sums, nonnegative amounts, targets, capacities, and submitted replacement links.",
        "Operational definition of inability; other meanings of 'cannot comply' remain unresolved.",
    ),
    (
        34,
        "Affirmative specific contact",
        "comment 53(b)-3.ii.A",
        _request(
            "regz_context.request_basis <> 'contact' OR (regz_context.affirmative AND regz_context.instruction_count > 0 AND regz_context.contact_channel)"
        ),
        "Recognize oral/electronic/written contact with explicit structured instructions.",
        "Interpretation of a conversation into instructions is external evidence processing.",
    ),
    (
        35,
        "Completed submitted request form",
        "comment 53(b)-3.ii.B",
        _request(
            "regz_context.request_basis <> 'form' OR (regz_context.form_completed AND regz_context.request_present AND regz_context.affirmative AND regz_context.instruction_count > 0)"
        ),
        "An actual completed/submitted form, not default coupon text, supports the exception.",
        "",
    ),
    (
        36,
        "Request to reallocate previous payment",
        "comment 53(b)-3.ii.C",
        _request(
            "regz_context.request_basis <> 'reallocation' OR (regz_context.prior_default_allocation AND regz_context.affirmative AND regz_context.contact_channel AND regz_context.instruction_count > 0)"
        ),
        "Verify a prior accepted default allocation and a later submitted specific request.",
        "Caller supplies the authoritative pre-payment balance snapshot for replacement; replacement is atomic.",
    ),
    (
        37,
        "Preprinted agreement is insufficient",
        "comment 53(b)-3.iii.A",
        _request("regz_context.request_basis <> 'agreement_default'"),
        "Reject reliance on agreement deemed consent; an independent contact is a different evidence record.",
        "",
    ),
    (
        38,
        "Untouched preselection is insufficient",
        "comment 53(b)-3.iii.B",
        _request("regz_context.request_basis <> 'application_default'"),
        "Reject reliance on an untouched issuer-selected checkbox.",
        "",
    ),
    (
        39,
        "Default payment coupon is insufficient",
        "comment 53(b)-3.iii.C",
        _request("regz_context.request_basis <> 'coupon_default'"),
        "Reject reliance on default coupon text or selection.",
        "",
    ),
    (
        40,
        "Imposed acceptance is insufficient",
        "comment 53(b)-3.iii.D",
        _request("NOT regz_context.required_condition"),
        "Reject requests based on imposed acceptance.",
        "Whether a condition was imposed is externally evidenced.",
    ),
)

FULL_REGULATION_Z_53_POLICY_SPECS = tuple(RegulationZ53PolicySpec(*row) for row in _ROWS)


def build_full_regulation_z_53_policies() -> tuple[Policy, ...]:
    """Return real constraints only; all 40 entries remain in POLICY_SPECS."""
    return tuple(
        spec.build_policy()
        for spec in FULL_REGULATION_Z_53_POLICY_SPECS
        if spec.constraint is not None
    )
