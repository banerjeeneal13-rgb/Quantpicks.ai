"""HTTP helpers with retries/backoff."""
from __future__ import annotations

import time
from typing import Any, Callable

import httpx


def request_with_backoff(
    request_fn: Callable[[], httpx.Response],
    max_retries: int,
    backoff_base_s: float,
) -> httpx.Response:
    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            resp = request_fn()
            if resp.status_code in {429, 500, 502, 503, 504}:
                raise httpx.HTTPStatusError(
                    f"HTTP {resp.status_code}", request=resp.request, response=resp
                )
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt >= max_retries:
                raise
            sleep_s = backoff_base_s * (2 ** attempt)
            time.sleep(sleep_s)

    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_backoff failed unexpectedly")


def get_json_with_backoff(
    url: str,
    params: dict[str, Any],
    timeout_s: int,
    max_retries: int,
    backoff_base_s: float,
) -> Any:
    def do_request() -> httpx.Response:
        return httpx.get(url, params=params, timeout=timeout_s)

    resp = request_with_backoff(do_request, max_retries=max_retries, backoff_base_s=backoff_base_s)
    resp.raise_for_status()
    return resp.json()
