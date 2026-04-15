from __future__ import annotations

import math


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def gk_price(
    spot: float,
    strike: float,
    expiry_years: float,
    domestic_rate: float,
    foreign_rate: float,
    sigma: float,
    is_call: bool,
) -> float:
    if expiry_years <= 0.0 or sigma <= 0.0 or spot <= 0.0 or strike <= 0.0:
        return max(0.0, (spot - strike) if is_call else (strike - spot))

    vol_sqrt_t = sigma * math.sqrt(expiry_years)
    d1 = (
        math.log(spot / strike)
        + (domestic_rate - foreign_rate + 0.5 * sigma * sigma) * expiry_years
    ) / vol_sqrt_t
    d2 = d1 - vol_sqrt_t
    df_d = math.exp(-domestic_rate * expiry_years)
    df_f = math.exp(-foreign_rate * expiry_years)
    if is_call:
        return spot * df_f * norm_cdf(d1) - strike * df_d * norm_cdf(d2)
    return strike * df_d * norm_cdf(-d2) - spot * df_f * norm_cdf(-d1)


def gk_gamma(
    spot: float,
    strike: float,
    expiry_years: float,
    domestic_rate: float,
    foreign_rate: float,
    sigma: float,
) -> float:
    if expiry_years <= 0.0 or sigma <= 0.0 or spot <= 0.0 or strike <= 0.0:
        return 0.0
    vol_sqrt_t = sigma * math.sqrt(expiry_years)
    d1 = (
        math.log(spot / strike)
        + (domestic_rate - foreign_rate + 0.5 * sigma * sigma) * expiry_years
    ) / vol_sqrt_t
    df_f = math.exp(-foreign_rate * expiry_years)
    return (df_f * norm_pdf(d1)) / (spot * sigma * math.sqrt(expiry_years))


def solve_gk_implied_vol(
    target_price: float,
    spot: float,
    strike: float,
    expiry_years: float,
    domestic_rate: float,
    foreign_rate: float,
    is_call: bool,
    low: float = 1e-4,
    high: float = 5.0,
    tol: float = 1e-8,
    max_iter: int = 200,
) -> float | None:
    if target_price <= 0.0 or spot <= 0.0 or strike <= 0.0 or expiry_years <= 0.0:
        return None

    intrinsic = gk_price(spot, strike, expiry_years, domestic_rate, foreign_rate, low, is_call)
    max_price = gk_price(spot, strike, expiry_years, domestic_rate, foreign_rate, high, is_call)
    if target_price < intrinsic - tol or target_price > max_price + tol:
        return None

    lo = low
    hi = high
    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        price = gk_price(spot, strike, expiry_years, domestic_rate, foreign_rate, mid, is_call)
        if abs(price - target_price) <= tol:
            return mid
        if price < target_price:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)
