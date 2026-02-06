export type OddsFields = {
  odds?: unknown;
  price_decimal?: unknown;
  priceDecimal?: unknown;
  price?: unknown;
  odds_decimal?: unknown;
  oddsDecimal?: unknown;
  price_american?: unknown;
  priceAmerican?: unknown;
  odds_american?: unknown;
  oddsAmerican?: unknown;
};

export function decimalFromAmerican(american: number): number | null {
  if (!Number.isFinite(american) || american === 0) return null;
  if (american > 0) return 1 + american / 100;
  return 1 + 100 / Math.abs(american);
}

export function coerceDecimalOdds(row: OddsFields | null | undefined): number | null {
  if (!row) return null;

  const oddsRaw = row.odds;
  if (oddsRaw !== undefined && oddsRaw !== null) {
    const num = Number(oddsRaw);
    if (Number.isFinite(num)) {
      if (num > 1 && num < 100) return num;
      if (num <= 0 || Math.abs(num) >= 100) {
        const dec = decimalFromAmerican(num);
        if (dec !== null) return dec;
      }
    }
  }

  const decimalCandidates = [
    row.price_decimal,
    row.priceDecimal,
    row.price,
    row.odds_decimal,
    row.oddsDecimal,
  ];
  for (const candidate of decimalCandidates) {
    if (candidate === undefined || candidate === null) continue;
    const num = Number(candidate);
    if (Number.isFinite(num) && num > 1) return num;
  }

  const americanCandidates = [
    row.price_american,
    row.priceAmerican,
    row.odds_american,
    row.oddsAmerican,
  ];
  for (const candidate of americanCandidates) {
    if (candidate === undefined || candidate === null) continue;
    const num = Number(candidate);
    if (!Number.isFinite(num) || num === 0) continue;
    const dec = decimalFromAmerican(num);
    if (dec !== null) return dec;
  }

  return null;
}

export function formatDecimalOdds(row: OddsFields | null | undefined, digits = 2): string {
  const odds = coerceDecimalOdds(row);
  if (!Number.isFinite(odds)) return "-";
  return odds.toFixed(digits);
}
