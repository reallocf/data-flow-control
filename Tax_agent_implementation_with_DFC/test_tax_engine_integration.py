from tax_engine import Expense, ScheduleCInput, compute_1040, expense_from_receipt


def test_expense_from_receipt_normalizes_category_and_business_use():
    receipt = {
        "receipt_id": 1,
        "vendor": "Cafe Central",
        "amount": 100.0,
        "category": "meal",
    }

    expense = expense_from_receipt(receipt, business_use=0.5)

    assert expense == Expense(
        description="Cafe Central",
        amount=50.0,
        category="meals",
        receipt_present=True,
    )


def test_compute_1040_applies_meal_cap_after_business_use_adjustment():
    expense = Expense(
        description="Cafe Central",
        amount=50.0,
        category="meals",
        receipt_present=True,
    )

    result = compute_1040(
        ScheduleCInput(gross_receipts=1000.0, expenses=[expense]),
        federal_withholding=0.0,
    )

    assert result == {
        "Line 8: Schedule C net profit": 975.0,
        "Line 10: Adjustments (SE tax deduction)": 68.88,
        "Line 11: Adjusted gross income": 906.12,
        "Line 12: Standard deduction": 14600,
        "Line 15: Taxable income": 0.0,
        "Line 16: Income tax": 0.0,
        "Line 57: Self-employment tax (Schedule SE)": 137.76,
        "Line 24: Total tax": 137.76,
        "Line 25d: Federal tax withheld": 0.0,
        "Line 34: If line 33 is more than line 24, subtract line 24 from line 33. This is the amount you overpaid": 0.0,
        "Line 37: Subtract line 33 from line 24. This is the amount you owe": 137.76,
    }
