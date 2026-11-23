export function formatNumber(value: number | null, digits = 3): string {
  if (value === null) return "\u2014";
  if (value !== 0 && Math.abs(value) < 10 ** -digits) {
    return Math.abs(value) < 1e-6
      ? value.toExponential(2)
      : new Intl.NumberFormat("en", { maximumSignificantDigits: 3 }).format(
          value,
        );
  }
  return new Intl.NumberFormat("en", { maximumFractionDigits: digits }).format(
    value,
  );
}
