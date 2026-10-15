"""Reglas de seguimiento de objetivos. Ninguna salida ejecuta una decisión financiera."""

from datetime import date
from calendar import monthrange

PASSIVE_TARGETS = (500, 1000, 2400, 3000, 4500, 6000)
MORTGAGE_TARGETS = (1200, 1000, 800, 600, 400, 200)


def previous_months(today: date, count: int = 6) -> list[str]:
    year, month = today.year, today.month
    result = []
    for _ in range(count):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        result.append(f"{year:04d}-{month:02d}")
    return list(reversed(result))


def reserve(balance, budget_without_debt, monthly_payment, as_of=None):
    if any(value is None for value in (balance, budget_without_debt, monthly_payment)):
        return {"status": "faltan datos", "reason": "Falta saldo real, presupuesto vigente o cuota del cronograma", "as_of": as_of,
                "balance": balance, "target": None, "shortfall": None, "months": None, "budget": None}
    budget = budget_without_debt + monthly_payment
    if budget <= 0:
        return {"status": "faltan datos", "reason": "Presupuesto mensual no válido", "as_of": as_of,
                "balance": balance, "target": None, "shortfall": None, "months": None, "budget": budget}
    target = 6 * budget
    return {"status": "cumple" if balance >= target else "faltan condiciones", "reason": None,
            "as_of": as_of, "balance": round(balance, 2), "budget": round(budget, 2),
            "target": round(target, 2), "shortfall": round(max(target - balance, 0), 2),
            "months": round(balance / budget, 2)}


def mortgage(latest_paid, next_due):
    source = next_due or latest_paid
    if source is None:
        return {"status": "faltan datos", "reason": "No hay cronograma válido", "milestones": []}
    principal_interest = source["capital"] + source["interest"]
    insurance = source["insurance"]
    payment = principal_interest + insurance
    balance = latest_paid["balance"] if latest_paid else None
    return {"status": "prevista" if next_due else "pagada", "as_of": source["date"],
            "principal_interest": round(principal_interest, 2), "insurance": round(insurance, 2),
            "payment": round(payment, 2), "balance": balance,
            "balance_as_of": latest_paid["date"] if latest_paid else None,
            "milestones": [{"target": t, "gap": round(max(payment - t, 0), 2),
                            "met": payment <= t} for t in MORTGAGE_TARGETS],
            "debt_free": balance is not None and balance <= 0}


def passive(months: list[dict], today: date, verified: bool = False):
    expected = previous_months(today)
    by_month = {row["month"]: row for row in months}
    series = [{"month": month, "amount": by_month[month]["amount"] if month in by_month
               and by_month[month]["complete"] else None} for month in expected]
    values = [row["amount"] for row in series]
    average = round(sum(values) / 6, 2) if all(value is not None for value in values) else None
    status = "verificado" if verified and average is not None else ("provisional" if average is not None else "faltan datos")
    achieved = {}
    milestones = []
    if verified:
        for month in sorted(by_month):
            year, number = map(int, month.split("-"))
            if (year, number) >= (today.year, today.month):
                continue
            window = previous_months(date(year, number, 1), 5) + [month]
            if all(key in by_month and by_month[key]["complete"] for key in window):
                rolling = sum(by_month[key]["amount"] for key in window) / 6
                for target in PASSIVE_TARGETS:
                    if rolling >= target and target not in achieved:
                        achieved[target] = f"{month}-{monthrange(year, number)[1]:02d}"
    for target in PASSIVE_TARGETS:
        milestones.append({"target": target, "actual": average, "shortfall": max(target - average, 0) if average is not None else None,
                           "percent": round(average / target * 100, 1) if average is not None else None,
                           "current": verified and average is not None and average >= target,
                           "achieved_on": achieved.get(target)})
    return {"status": status, "reason": ("Faltan seis meses completos de datos" if average is None else
                       "Clasificación, impuestos y costos pendientes de conciliación" if not verified else None),
            "months": series, "average": average, "min": min(values) if average is not None else None,
            "max": max(values) if average is not None else None,
            "spread": round(max(values) - min(values), 2) if average is not None else None,
            "labor_needed": max(6000 - average, 0) if verified and average is not None else None,
            "milestones": milestones, "as_of": expected[-1]}


def condition(actual, threshold, *, minimum=True, as_of=None, reason=None):
    if actual is None or threshold is None:
        return {"status": "faltan datos", "actual": actual, "threshold": threshold, "difference": None,
                "as_of": as_of, "reason": reason or "Dato pendiente"}
    difference = actual - threshold if minimum else threshold - actual
    return {"status": "cumple" if difference >= -1e-9 else "faltan condiciones", "actual": actual,
            "threshold": threshold, "difference": round(difference, 2), "as_of": as_of, "reason": reason}


def decision(conditions: dict, record=None):
    statuses = [item["status"] for item in conditions.values()]
    state = ("faltan datos" if "faltan datos" in statuses else
             "faltan condiciones" if "faltan condiciones" in statuses else "habilitado")
    if record and all(record.get(key) for key in ("date", "reason", "next_review", "outcome")):
        state = "decisión tomada"
    return {"status": state, "conditions": conditions, "record": record if state == "decisión tomada" else None,
            "conditions_changed": state == "decisión tomada" and any(s != "cumple" for s in statuses)}


def property_scenario(offer):
    fields = ("equity", "closing", "works", "prepayments", "transition_deficit", "operating_reserve",
              "available_capital", "rent_monthly", "fixed_monthly", "variable_monthly", "tax_monthly",
              "reserve_monthly", "debt_monthly")
    if (not offer or any(not isinstance(offer.get(field), (int, float)) or offer[field] < 0 for field in fields)
            or not all(offer.get(field) for field in ("source", "as_of", "property", "stabilization_date"))
            or not isinstance(offer.get("price"), (int, float)) or offer["price"] <= 0):
        return {"capital": condition(None, None, reason="Falta oferta y/o costos completos"),
                "coverage": condition(None, 1.2, reason="Falta flujo estabilizado"),
                "stress": condition(None, 0, reason="Falta escenario verificable")}
    required = sum(offer[field] for field in fields[:6])
    net = offer["rent_monthly"] - sum(offer[field] for field in ("fixed_monthly", "variable_monthly", "tax_monthly", "reserve_monthly"))
    debt = offer["debt_monthly"]
    coverage = net / debt if debt > 0 else (1.2 if net > 0 else 0)
    # Vacancia sustituye dos meses de renta y de costos variables; fijos y deuda siguen 12 meses.
    stressed = 10 * (offer["rent_monthly"] * .9 - offer["variable_monthly"]) - 12 * sum(
        offer[field] for field in ("fixed_monthly", "tax_monthly", "reserve_monthly", "debt_monthly"))
    return {"capital": condition(offer["available_capital"], required, as_of=offer.get("as_of")),
            "coverage": condition(round(coverage, 3), 1.2, as_of=offer.get("as_of"),
                                  reason="Sin deuda; se exige flujo neto positivo" if debt == 0 else None),
            "stress": condition(round(stressed, 2), 0, as_of=offer.get("as_of"))}
