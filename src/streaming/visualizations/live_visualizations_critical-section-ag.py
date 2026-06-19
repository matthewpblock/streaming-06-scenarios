"""src/streaming/visualizations/live_visualizations_critical-section-ag.py.

No-op live chart utilities for the consumer loop.
Avoids blocking GUI windows, delegating visualization entirely to Streamlit.
"""

from pathlib import Path
from typing import Any

__all__ = [
    "close_live_chart",
    "init_live_chart",
    "save_live_chart",
    "update_live_chart",
]


def init_live_chart() -> tuple[Any, Any, list[int], list[float]]:
    """Return empty placeholder structures."""
    return None, None, [], []


def update_live_chart(
    *,
    figure: Any,
    axis: Any,
    x_values: list[int],
    y_values: list[float],
    message: dict[str, Any],
) -> None:
    """No-op update."""
    pass


def save_live_chart(
    *,
    figure: Any,
    chart_path: Path,
) -> None:
    """No-op save."""
    pass


def close_live_chart() -> None:
    """No-op close."""
    pass
