const markRegex = /\p{Mark}/gu;

export function foldForSqlSearch(input: string | null | undefined): string {
  if (input == null) {
    return '';
  }
  return String(input).normalize('NFKD').replace(markRegex, '').toLowerCase();
}
