# Section 274 DFC rule-family inventory

The inventory separates statutory rule coverage from executable-policy correctness.

| Family | Basis | Representation | Eligible |
|---|---|---|---|
| entertainment_disallowance | 26 U.S.C. § 274(a)(1), subject to § 274(e) | PER_RECEIPT_ABSTRACTABLE | True |
| club_dues_disallowance | 26 U.S.C. § 274(a)(3) | PER_RECEIPT_DIRECT | True |
| qualified_transportation_fringe | 26 U.S.C. § 274(a)(4) | PER_RECEIPT_DIRECT | True |
| gift_annual_limit | 26 U.S.C. § 274(b) | REQUIRES_CROSS_RECEIPT_STATE | False |
| foreign_travel_allocation | 26 U.S.C. § 274(c) | PER_RECEIPT_ABSTRACTABLE | True |
| substantiation | 26 U.S.C. § 274(d), (i) | PER_RECEIPT_ABSTRACTABLE | True |
| foreign_convention_limitation | 26 U.S.C. § 274(h)(1), (3), (6) | PER_RECEIPT_ABSTRACTABLE | True |
| cruise_convention_limit | 26 U.S.C. § 274(h)(2), (5) | REQUIRES_CROSS_RECEIPT_STATE | False |
| section212_convention_disallowance | 26 U.S.C. § 274(h)(7) | PER_RECEIPT_DIRECT | True |
| employee_achievement_award | 26 U.S.C. § 274(j) | REQUIRES_CROSS_RECEIPT_STATE | False |
| business_meal_eligibility | 26 U.S.C. § 274(k) | PER_RECEIPT_ABSTRACTABLE | True |
| commuting_transportation | 26 U.S.C. § 274(l) | PER_RECEIPT_ABSTRACTABLE | True |
| luxury_water_transport | 26 U.S.C. § 274(m)(1) | PER_RECEIPT_ABSTRACTABLE | True |
| travel_as_education | 26 U.S.C. § 274(m)(2) | PER_RECEIPT_DIRECT | True |
| accompanying_person_travel | 26 U.S.C. § 274(m)(3) | PER_RECEIPT_ABSTRACTABLE | True |
| meal_percentage_limit | 26 U.S.C. § 274(n) | PER_RECEIPT_ABSTRACTABLE | True |
| employer_convenience_meals | 26 U.S.C. § 274(o) | PER_RECEIPT_ABSTRACTABLE | True |
