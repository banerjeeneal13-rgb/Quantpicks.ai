Fallback Plan: NBA Shot Zone Data

Decision Tree
1) Primary (stats.nba.com)
   - If HTTP 200 and resultSets present: use and cache.
   - If HTTP 403: mark access_denied and STOP for 60 minutes.
   - If HTTP 429: backoff and retry once, then STOP and escalate.
   - If repeated empty/malformed responses: STOP and escalate.

2) Secondary (Basketball-Reference)
   - Only for coarse historical shooting splits (not true shot zones).
   - Use for summary content when product requirements allow.

3) Tertiary (Licensed Provider)
   - If zone granularity is required and primary is blocked, use a licensed provider
     (e.g., Sportradar, Stats Perform).

Escalation Guidance
- Contact NBA data licensing or approved provider.
- Document access_denied events and time windows.
- Do NOT attempt bypass techniques (IP rotation, header spoofing, or captcha evasion).
