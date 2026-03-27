/**
 * Stake sizing utilities using Kelly criterion and EV-based methods.
 *
 * Kelly Criterion: f* = (p * b - q) / b
 *   where p = win probability, q = 1-p, b = net decimal odds (decimalOdds - 1)
 *
 * We use fractional Kelly (default: quarter Kelly) for conservative sizing,
 * then cap at the user's configured max bet.
 */

export type StakeConfig = {
  /** Current bankroll in dollars */
  bankroll: number;
  /** Unit size as a percentage of bankroll (e.g. 1 = 1%) */
  unitPct: number;
  /** Max bet as a percentage of bankroll (e.g. 3 = 3%) */
  maxPct: number;
  /** Kelly fraction to use (0.25 = quarter Kelly, 0.5 = half, 1.0 = full) */
  kellyFraction?: number;
};

export type StakeResult = {
  /** Recommended stake in dollars */
  stake: number;
  /** Stake expressed in units */
  units: number;
  /** Full Kelly stake before fraction/cap */
  kellyFull: number;
  /** The unit size in dollars */
  unitSize: number;
  /** The max bet in dollars */
  maxBet: number;
  /** Method used: "kelly" | "flat" */
  method: string;
};

const DEFAULT_KELLY_FRACTION = 0.25; // Quarter Kelly — conservative

/**
 * Calculate the Kelly criterion stake.
 *
 * @param p     Model win probability (0-1)
 * @param odds  Decimal odds (e.g. 1.91 for -110)
 * @param config Bankroll and sizing parameters
 * @returns StakeResult with recommended dollar amount and unit count
 */
export function kellyStake(
  p: number,
  odds: number,
  config: StakeConfig
): StakeResult {
  const { bankroll, unitPct, maxPct, kellyFraction = DEFAULT_KELLY_FRACTION } = config;
  const unitSize = (bankroll * unitPct) / 100;
  const maxBet = (bankroll * maxPct) / 100;

  // Validate inputs
  if (
    !Number.isFinite(p) ||
    !Number.isFinite(odds) ||
    p <= 0 ||
    p >= 1 ||
    odds <= 1 ||
    bankroll <= 0
  ) {
    return {
      stake: unitSize,
      units: 1,
      kellyFull: 0,
      unitSize,
      maxBet,
      method: "flat",
    };
  }

  const b = odds - 1; // net payout per $1
  const q = 1 - p;
  const kellyFull = (p * b - q) / b;

  // If Kelly is negative or zero, the edge is not profitable → use minimum
  if (kellyFull <= 0) {
    return {
      stake: 0,
      units: 0,
      kellyFull: 0,
      unitSize,
      maxBet,
      method: "kelly",
    };
  }

  // Apply fractional Kelly
  let stake = bankroll * kellyFull * kellyFraction;

  // Floor at 1 unit minimum when there's positive edge
  if (stake < unitSize && kellyFull > 0) {
    stake = unitSize;
  }

  // Cap at max bet
  if (stake > maxBet) {
    stake = maxBet;
  }

  // Round to nearest cent
  stake = Math.round(stake * 100) / 100;

  return {
    stake,
    units: unitSize > 0 ? Math.round((stake / unitSize) * 100) / 100 : 0,
    kellyFull: Math.round(kellyFull * 10000) / 10000,
    unitSize,
    maxBet,
    method: "kelly",
  };
}

/**
 * Format a stake amount for display.
 */
export function formatStake(amount: number): string {
  if (!Number.isFinite(amount) || amount <= 0) return "-";
  return `$${amount.toFixed(2)}`;
}

/**
 * Format a unit count for display.
 */
export function formatUnits(units: number): string {
  if (!Number.isFinite(units) || units <= 0) return "-";
  return `${units.toFixed(1)}u`;
}

/**
 * Default stake config using typical conservative defaults.
 */
export const DEFAULT_STAKE_CONFIG: StakeConfig = {
  bankroll: 1000,
  unitPct: 1,
  maxPct: 3,
  kellyFraction: 0.25,
};
