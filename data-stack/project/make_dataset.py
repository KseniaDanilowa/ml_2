#!/usr/bin/env python3
"""Генератор персонального датасета «Отток клиентов телеком-оператора».

У каждого студента СВОЙ датасет: seed = номер по журналу (N).
Данные отличаются распределениями, уровнем оттока, «лучшей» моделью,
поэтому списать решение невозможно — числа не совпадут.

Использование:
    python make_dataset.py --student 7            # файл customers_7.csv
"""
import argparse
import string

import numpy as np
import pandas as pd


def generate(n_student: int) -> pd.DataFrame:
    if n_student < 1:
        raise SystemExit("номер студента должен быть >= 1")
    rng = np.random.default_rng(7919 * n_student + 1000 + n_student)

    n = 7000 + (n_student % 5) * 400                      # 7000..8600 строк

    # --- подписки и услуги (вероятности слегка «дрожат» от номера) --------
    j = lambda base, amp: float(np.clip(base + amp * rng.uniform(-1, 1), 0.10, 0.80))

    def pick(options, probs):
        probs = np.array(probs, dtype=float)
        probs /= probs.sum()                              # ровно в сумме 1
        return rng.choice(options, n, p=probs)

    contract = pick(["Month-to-month", "One year", "Two year"],
                    [j(0.55, 0.06), j(0.24, 0.04), j(0.21, 0.04)])

    internet = pick(["DSL", "Fiber optic", "No"],
                    [j(0.35, 0.03), j(0.43, 0.05), j(0.22, 0.03)])

    def yes_no(p):
        return rng.choice(["Yes", "No"], n, p=[p, 1 - p])

    phone = yes_no(0.90)
    paperless = yes_no(0.60)
    partner = yes_no(0.48)
    dependents = yes_no(0.30)

    # услуги доступны только при интернете; при "No" — категория "No internet"
    svc = lambda p: np.where(
        internet == "No", "No internet",
        rng.choice(["Yes", "No"], n, p=[p, 1 - p]))
    online_security = svc(0.30)
    tech_support = svc(0.28)
    streaming_tv = svc(0.38)

    payment = pick(["Electronic check", "Mailed check", "Bank transfer (automatic)",
                    "Credit card (automatic)"],
                   [j(0.33, 0.05), j(0.23, 0.04), j(0.22, 0.04), j(0.22, 0.03)])

    # --- числовые характеристики ------------------------------------------
    tenure = np.clip(rng.weibull(1.4, n) * 26 + 1, 1, 72).astype(int)
    age = rng.integers(18, 81, n)
    monthly = np.clip(rng.normal(64, 21, n), 18, 118).round(2)
    support_calls = rng.poisson(1.1 + 0.4 * (n_student % 3), n)

    total = (tenure * monthly * rng.normal(1.0, 0.08, n)).round(2)

    # чистый шум: регион и реферальный код
    region = rng.choice(["North", "South", "East", "West"], n)
    referral = ["-".join(rng.choice(list(string.ascii_uppercase), 3)) for _ in range(n)]

    # --- целевая переменная: отток (линейная часть + нелинейности) ----------
    # сила нелинейностей nl своя у каждого студента: у кого-то данные почти
    # линейные (выиграет логрегрессия), у кого-то сильно нелинейные (деревья)
    nl = rng.uniform(0.15, 1.8)

    z = (1.05 * (contract == "Month-to-month")
         - 0.65 * (contract == "Two year")
         + 0.70 * (internet == "Fiber optic")
         + 0.45 * (payment == "Electronic check")
         + 0.20 * (support_calls >= 3)
         - 0.020 * tenure
         - 0.30 * (tech_support == "Yes")
         - 0.25 * (online_security == "Yes")
         + 0.15 * (paperless == "Yes")
         - 0.12 * (partner == "Yes")
         # нелинейные эффекты (пороги, волна по возрасту, U-форма стажа):
         + nl * 1.10 * ((monthly > 90) & (internet == "Fiber optic"))
         + nl * 0.90 * np.sin((age - 40) / 9)
         + nl * 0.80 * ((tenure < 4) | (tenure > 66))
         + nl * 0.70 * ((contract == "One year") & (support_calls >= 2))
         + nl * 0.50 * ((age < 25) & (streaming_tv == "Yes")))
    strength = 0.9 + 0.25 * rng.uniform()                # сила сигнала своя у каждого
    z = z * strength
    # калибруем сдвигом (бисекция): средняя доля оттока = target_rate (24–33%)
    target_rate = 0.24 + 0.09 * rng.uniform()
    lo, hi = -10.0, 10.0
    for _ in range(60):
        mid = (lo + hi) / 2
        rate = (1 / (1 + np.exp(-(z + mid)))).mean()
        if rate < target_rate:
            lo = mid
        else:
            hi = mid
    p_churn = 1 / (1 + np.exp(-(z + (lo + hi) / 2)))
    churn = rng.binomial(1, p_churn)

    # ЛОВУШКА-УТЕЧКА: заявление на расторжение почти всегда означает уход.
    # Кто не заметит и оставит колонку в признаках — получит «слишком хорошо»
    # и должен объяснить это в отчёте (по заданию колонку исключаем).
    cancel = rng.binomial(1, np.where(churn == 1, 0.60, 0.015))
    cancellation_request = np.where(cancel == 1, "Yes", "No")

    df = pd.DataFrame({
        "customer_id": [f"CUST-{i:05d}" for i in range(1, n + 1)],
        "age": age,
        "region": region,
        "partner": partner,
        "dependents": dependents,
        "tenure_months": tenure,
        "contract": contract,
        "internet_service": internet,
        "phone_service": phone,
        "online_security": online_security,
        "tech_support": tech_support,
        "streaming_tv": streaming_tv,
        "paperless_billing": paperless,
        "payment_method": payment,
        "monthly_charges": monthly,
        "total_charges": total,
        "support_calls": support_calls,
        "referral_code": referral,
        "cancellation_request": cancellation_request,
        "churn": np.where(churn == 1, "Yes", "No"),
    })

    # --- «грязь», с которой надо разобраться (это часть задания) -----------
    # 1) пропуски в total_charges (~3%)
    na_idx = rng.choice(n, size=int(n * 0.03), replace=False)
    df.loc[na_idx, "total_charges"] = np.nan
    # 2) выбросы в monthly_charges (невозможные значения)
    out_idx = rng.choice(n, size=12, replace=False)
    df.loc[out_idx, "monthly_charges"] = rng.uniform(250, 500, 12).round(2)
    # 3) дубликаты строк (~15)
    dup_idx = rng.choice(n, size=15, replace=False)
    df = pd.concat([df, df.iloc[dup_idx]], ignore_index=True)
    df = df.sample(frac=1.0, random_state=n_student).reset_index(drop=True)  # перемешать
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--student", type=int, required=True, help="номер по журналу, напр. 7")
    ap.add_argument("--out", default=None, help="куда сохранить csv")
    args = ap.parse_args()

    df = generate(args.student)
    out = args.out or f"customers_{args.student}.csv"
    df.to_csv(out, index=False)

    rate = (df["churn"] == "Yes").mean()
    print(f"датасет: {out}")
    print(f"строк: {len(df)} | колонок: {df.shape[1]} | доля оттока: {rate:.1%}")
    print(f"пропусков в total_charges: {df['total_charges'].isna().sum()} | "
          f"дубликатов customer_id: {df['customer_id'].duplicated().sum()}")


if __name__ == "__main__":
    main()
