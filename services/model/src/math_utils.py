def implied_prob_decimal_odds(odds: float) -> float:
    if odds <= 1.0:
        return 0.0
    return 1.0 / odds

def remove_vig_two_way(p_over: float, p_under: float):
    s = p_over + p_under
    if s <= 0:
        return (0.0, 0.0)
    return (p_over / s, p_under / s)

def ev_per_dollar(p_win: float, decimal_odds: float) -> float:
    if decimal_odds <= 1.0:
        return -1.0
    return p_win * (decimal_odds - 1.0) - (1.0 - p_win)
