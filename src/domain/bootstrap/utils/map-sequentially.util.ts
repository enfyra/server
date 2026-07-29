export async function mapSequentially<T, R>(
  values: T[],
  mapper: (value: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  for (const [index, value] of values.entries()) {
    results.push(await mapper(value, index));
  }
  return results;
}
