COMMON_FEATURES = [
    "min_ma_5", "min_ma_10",
    "fga_ma_5", "fga_ma_10",
    "is_home", "days_rest",
    "team_pace", "team_off_rating", "team_def_rating",
    "opp_pace", "opp_off_rating", "opp_def_rating",
    "pace_diff", "def_diff",
    "usage_proxy", "pace_x_min", "def_x_usage", "opp_def_x_fga", "opp_pace_x_min",
    "teammate_out_count", "starter_out_count",
    "player_status_q", "player_status_d", "player_status_o",
    "opp_pos_def_fg_pct",
    "off_matchup_pts_per_min", "off_matchup_fga_per_min", "off_matchup_fg_pct", "off_matchup_fg3_pct",
    "def_matchup_fg_pct_allowed", "def_matchup_pts_per_min_allowed",
]

MARKET_FEATURES = {
    "player_rebounds": ["reb_ma_5", "reb_ma_10"] + COMMON_FEATURES,
    "player_assists": ["ast_ma_5", "ast_ma_10"] + COMMON_FEATURES,
    "player_threes": ["fg3m_ma_5", "fg3m_ma_10"] + COMMON_FEATURES,
    "player_points_rebounds_assists": ["pra_ma_5", "pra_ma_10"] + COMMON_FEATURES,
    "player_points_assists": ["pa_ma_5", "pa_ma_10"] + COMMON_FEATURES,
    "player_points_rebounds": ["pr_ma_5", "pr_ma_10"] + COMMON_FEATURES,
    "player_rebounds_assists": ["ra_ma_5", "ra_ma_10"] + COMMON_FEATURES,
    "player_blocks_steals": ["stocks_ma_5", "stocks_ma_10"] + COMMON_FEATURES,
}

MARKET_TARGETS = {
    "player_rebounds": "REB",
    "player_assists": "AST",
    "player_threes": "FG3M",
    "player_points_rebounds_assists": "PRA",
    "player_points_assists": "PA",
    "player_points_rebounds": "PR",
    "player_rebounds_assists": "RA",
    "player_blocks_steals": "STOCKS",
}

MARKET_MODEL_NAMES = {
    "player_rebounds": "rebounds_model_v1.joblib",
    "player_assists": "assists_model_v1.joblib",
    "player_threes": "threes_model_v1.joblib",
    "player_points_rebounds_assists": "pra_model_v1.joblib",
    "player_points_assists": "pa_model_v1.joblib",
    "player_points_rebounds": "pr_model_v1.joblib",
    "player_rebounds_assists": "ra_model_v1.joblib",
    "player_blocks_steals": "stocks_model_v1.joblib",
}

MARKET_CACHE_NAMES = {
    "player_rebounds": "rebounds_feature_cache.csv",
    "player_assists": "assists_feature_cache.csv",
    "player_threes": "threes_feature_cache.csv",
    "player_points_rebounds_assists": "pra_feature_cache.csv",
    "player_points_assists": "pa_feature_cache.csv",
    "player_points_rebounds": "pr_feature_cache.csv",
    "player_rebounds_assists": "ra_feature_cache.csv",
    "player_blocks_steals": "stocks_feature_cache.csv",
}
