from collections import deque

import pytest

from marketdata.gold.forecast_metrics import (
    forecast_error_window_averages,
    forecast_mid_price,
    update_forecasts,
)
from marketdata.gold.mid_price_metrics import add_mid_price_sample


def _samples_with(*pairs: tuple[float, float]) -> deque:
    samples = deque()
    for timestamp, mid_price in pairs:
        add_mid_price_sample(samples, timestamp, mid_price)
    return samples


def test_forecast_is_none_with_fewer_than_two_samples() -> None:
    assert forecast_mid_price(deque()) is None
    assert forecast_mid_price(_samples_with((0.0, 100.0))) is None


def test_forecast_is_none_when_all_samples_share_one_timestamp() -> None:
    samples = deque()
    add_mid_price_sample(samples, 10.0, 100.0)
    add_mid_price_sample(samples, 10.0, 105.0)
    assert forecast_mid_price(samples) is None


def test_forecast_uses_only_samples_after_a_book_validity_gap() -> None:
    samples = _samples_with(
        (0.0, 100.0),
        (30.0, 130.0),
        (60.0, None),
        (90.0, 90.0),
        (120.0, 120.0),
    )

    assert forecast_mid_price(samples) == pytest.approx(180.0)


def test_forecast_extrapolates_a_perfectly_linear_trend() -> None:
    # mid-price rises by 2/sec: at t=950 -> 100, t=975 -> 150, t=1000 (now) -> 200
    samples = _samples_with((950.0, 100.0), (975.0, 150.0), (1000.0, 200.0))

    predicted = forecast_mid_price(samples)

    assert predicted == pytest.approx(320.0)  # 60s ahead at the same 2/sec slope


def test_update_forecasts_needs_a_second_sample_before_forecasting() -> None:
    pending, errors, samples = deque(), deque(), deque()
    add_mid_price_sample(samples, 0.0, 100.0)

    result = update_forecasts(pending, errors, samples, now=0.0, observed_mid_price=100.0)

    assert result is None
    assert not pending


def test_update_forecasts_resolves_a_due_forecast_into_an_error_sample() -> None:
    pending, errors, samples = deque(), deque(), deque()

    add_mid_price_sample(samples, 0.0, 100.0)
    update_forecasts(pending, errors, samples, now=0.0, observed_mid_price=100.0)

    add_mid_price_sample(samples, 30.0, 130.0)
    predicted = update_forecasts(pending, errors, samples, now=30.0, observed_mid_price=130.0)
    assert predicted == pytest.approx(190.0)  # slope 1.0/sec, +60s horizon
    assert len(pending) == 1
    assert not errors

    # 60s later (the horizon), the forecast made at t=30 becomes due
    add_mid_price_sample(samples, 90.0, 185.0)
    update_forecasts(pending, errors, samples, now=90.0, observed_mid_price=185.0)

    assert len(errors) == 1
    assert errors[0].error == pytest.approx(5.0)  # |190 predicted - 185 observed|


def test_forecast_error_window_averages_empty_is_all_none() -> None:
    assert forecast_error_window_averages(deque()) == {"1m": None, "5m": None, "15m": None}
