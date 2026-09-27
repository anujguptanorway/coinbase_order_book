"""60-second-ahead mid-price forecast, and scoring past forecasts against
what actually happened.

The forecast itself is a linear trend fit with numpy over the recent
mid-price samples, extrapolated forward. Good enough for a POC -- no need
for a bespoke forecasting algorithm when an established library already
does this well.
"""

from __future__ import annotations

import logging
from collections import deque
from typing import NamedTuple

import numpy as np

from .mid_price_metrics import WINDOWS, MidPriceSamples

logger = logging.getLogger(__name__)

HORIZON_SECONDS = 60
TREND_WINDOW_SECONDS = 60  # fit the trend over the last 60s of samples
MIN_SAMPLES = 2  # need at least two points to fit a line
LONGEST_WINDOW = max(WINDOWS.values())


def forecast_mid_price(samples: MidPriceSamples) -> float | None:
    """Extrapolate mid-price `HORIZON_SECONDS` ahead from the trailing trend."""
    if not samples or samples[-1].mid_price is None:
        return None

    now = samples[-1].timestamp
    window: list[tuple[float, float]] = []
    for sample in reversed(samples):
        if sample.mid_price is None:
            break
        if sample.timestamp >= now - TREND_WINDOW_SECONDS:
            window.append((sample.timestamp, sample.mid_price))
    window.reverse()
    if len(window) < MIN_SAMPLES:
        return None  # not enough recent history to fit a trend yet

    offsets = np.array([timestamp - now for timestamp, _ in window])
    if offsets.max() == offsets.min():
        return None  # every sample landed at the same instant -- no trend to fit, would be singular

    mid_prices = np.array([price for _, price in window])
    try:
        slope, intercept = np.polyfit(offsets, mid_prices, 1)  # mid_price ≈ slope * offset + intercept
    except (np.linalg.LinAlgError, ValueError):
        # rare numerical edge case (e.g. a near-singular fit) -- skip this cycle rather than
        # crash the live loop over a best-effort metric
        logger.warning("Forecast fit failed on this cycle, skipping", exc_info=True)
        return None
    return float(slope * HORIZON_SECONDS + intercept)


# ------------------------------------------------------------- error scoring


class PendingForecast(NamedTuple):
    target_timestamp: float  # when this forecast's 60s horizon is reached
    predicted_mid_price: float


class ForecastError(NamedTuple):
    timestamp: float  # when the forecast was scored (i.e. the target time)
    error: float  # abs(predicted - observed)


PendingForecasts = deque[PendingForecast]
ForecastErrors = deque[ForecastError]


def update_forecasts(
    pending: PendingForecasts,
    errors: ForecastErrors,
    samples: MidPriceSamples,
    now: float,
    observed_mid_price: float,
) -> float | None:
    """Score any due forecasts against what just happened, then make a new one.

    One call does everything the pipeline needs per message: resolve, evict,
    forecast. Returns the new forecast's predicted mid-price (or `None` if
    there isn't enough history yet) so the caller can just display it.
    """
    # pending is oldest-first (target times only increase), so popping from the left is enough
    while pending and pending[0].target_timestamp <= now:
        due = pending.popleft()
        errors.append(ForecastError(now, abs(due.predicted_mid_price - observed_mid_price)))

    cutoff = now - LONGEST_WINDOW
    while errors and errors[0].timestamp < cutoff:
        errors.popleft()

    predicted = forecast_mid_price(samples)
    if predicted is not None:
        pending.append(PendingForecast(now + HORIZON_SECONDS, predicted))
    return predicted


def forecast_error_window_averages(errors: ForecastErrors) -> dict[str, float | None]:
    """One average error per window label, each over the errors within that trailing window."""
    if not errors:
        return {label: None for label in WINDOWS}

    now = errors[-1].timestamp
    return {label: _average_error_since(errors, now - seconds) for label, seconds in WINDOWS.items()}


def _average_error_since(errors: ForecastErrors, cutoff: float) -> float | None:
    values = [e.error for e in errors if e.timestamp >= cutoff]
    return sum(values) / len(values) if values else None
