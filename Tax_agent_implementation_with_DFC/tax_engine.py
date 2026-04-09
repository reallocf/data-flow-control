from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional


TAX_BRACKETS_2024_SINGLE = [
    (11600, 0.10),
    (47150, 0.12),
    (100525, 0.22),
    (191950, 0.24),
    (243725, 0.32),
    (609350, 0.35),
    (float("inf"), 0.37),
]

STANDARD_DEDUCTION_2024_SINGLE = 14600
MEAL_DEDUCTION_CAP = 0.50


@dataclass
class Expense:
    description: str
    amount: float
    category: str
    receipt_present: bool = True


@dataclass
class ScheduleCInput:
    gross_receipts: float
    expenses: list[Expense] = field(default_factory=list)


def compute_tax_2024_single(taxable_income: float) -> float:
    tax = 0.0
    prev = 0.0

    for limit, rate in TAX_BRACKETS_2024_SINGLE:
        if taxable_income <= prev:
            break
        income_in_bracket = min(taxable_income, limit) - prev
        tax += income_in_bracket * rate
        prev = limit

    return round(tax, 2)


def normalize_expense_category(category: str) -> str:
    normalized = (category or "").strip().lower()
    if normalized == "meal":
        return "meals"
    return normalized or "other"


def expense_from_receipt(
    receipt: Mapping[str, Any],
    *,
    business_use: float = 1.0,
) -> Expense:
    amount = round(float(receipt.get("amount", 0.0)) * float(business_use), 2)
    description = str(receipt.get("vendor") or f"Receipt {receipt.get('receipt_id', '')}").strip()
    category = normalize_expense_category(str(receipt.get("category", "other")))
    return Expense(
        description=description or "Receipt expense",
        amount=amount,
        category=category,
        receipt_present=True,
    )


def compute_1040(inp: ScheduleCInput, federal_withholding: float = 0.0) -> Dict[str, float]:
    total_expenses = 0.0
    for exp in inp.expenses:
        if exp.category == "meals":
            total_expenses += round(exp.amount * MEAL_DEDUCTION_CAP, 2)
        else:
            total_expenses += round(exp.amount, 2)

    net_profit = round(inp.gross_receipts - total_expenses, 2)
    se_tax = round(max(0.0, net_profit) * 0.9235 * 0.153, 2)
    se_deduction = round(se_tax * 0.50, 2)

    agi = round(net_profit - se_deduction, 2)
    taxable_income = max(0.0, round(agi - STANDARD_DEDUCTION_2024_SINGLE, 2))
    income_tax = compute_tax_2024_single(taxable_income)
    total_tax = round(income_tax + se_tax, 2)
    refund = max(0.0, round(federal_withholding - total_tax, 2))
    amount_owed = max(0.0, round(total_tax - federal_withholding, 2))

    return {
        "Line 8: Schedule C net profit": net_profit,
        "Line 10: Adjustments (SE tax deduction)": se_deduction,
        "Line 11: Adjusted gross income": agi,
        "Line 12: Standard deduction": STANDARD_DEDUCTION_2024_SINGLE,
        "Line 15: Taxable income": taxable_income,
        "Line 16: Income tax": income_tax,
        "Line 57: Self-employment tax (Schedule SE)": se_tax,
        "Line 24: Total tax": total_tax,
        "Line 25d: Federal tax withheld": federal_withholding,
        "Line 34: If line 33 is more than line 24, subtract line 24 from line 33. This is the amount you overpaid": refund,
        "Line 37: Subtract line 33 from line 24. This is the amount you owe": amount_owed,
    }


def format_for_benchmark(result: Dict[str, float]) -> str:
    return "\n".join(f"{line} | {value}" for line, value in result.items())


def schedule_c_input_from_expenses(
    gross_receipts: float,
    expenses: Iterable[Expense],
) -> ScheduleCInput:
    return ScheduleCInput(gross_receipts=float(gross_receipts), expenses=list(expenses))


def expense_summary(
    expense: Expense,
    *,
    business_use: float = 1.0,
    receipt_id: Optional[int] = None,
    original_amount: Optional[float] = None,
) -> Dict[str, Any]:
    return {
        "receipt_id": receipt_id,
        "description": expense.description,
        "category": expense.category,
        "original_amount": original_amount,
        "business_use": business_use,
        "tax_engine_amount": expense.amount,
        "receipt_present": expense.receipt_present,
    }
