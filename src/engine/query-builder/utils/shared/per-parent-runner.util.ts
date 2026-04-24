export async function perParentRun<T>(
  parentIds: any[],
  fn: (id: any) => Promise<T>,
  concurrency: number,
): Promise<Map<string, T>> {
  const results = new Map<string, T>();
  let cursor = 0;

  async function worker() {
    while (cursor < parentIds.length) {
      const i = cursor++;
      const id = parentIds[i];
      results.set(String(id), await fn(id));
    }
  }

  const workerCount = Math.min(concurrency, parentIds.length);
  if (workerCount === 0) return results;
  const workers = Array.from({ length: workerCount }, worker);
  await Promise.all(workers);
  return results;
}
