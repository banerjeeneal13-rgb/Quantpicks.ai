def probability_stub(market: str, side: str, line: float) -> float:
    base = 0.52
    adj = 0.0

    if "points" in market:
        adj = -0.003 * (line - 22.0)
    elif "assists" in market:
        adj = -0.010 * (line - 5.5)
    elif "rebounds" in market:
        adj = -0.010 * (line - 7.5)
    else:
        adj = -0.005 * (line - 10.0)

    p_over = max(0.05, min(0.95, base + adj))
    if side == "over":
        return p_over
    return 1.0 - p_over
