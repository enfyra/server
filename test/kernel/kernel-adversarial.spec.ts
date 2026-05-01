import {
  CHUNKED_FETCH_CONCURRENCY,
  WHERE_IN_CHUNK_SIZE,
  chunkedFetch,
  parseBatchFields,
  perParentRun,
} from '@enfyra/kernel';

describe('kernel batch utilities adversarial behavior', () => {
  test('chunkedFetch preserves input chunk order even when chunks finish out of order', async () => {
    const values = Array.from(
      { length: WHERE_IN_CHUNK_SIZE * 2 + 37 },
      (_, index) => index,
    );
    const seenChunks: number[][] = [];

    const result = await chunkedFetch(values, async (chunk: number[]) => {
      seenChunks.push(chunk);
      const chunkIndex = Math.floor(chunk[0] / WHERE_IN_CHUNK_SIZE);
      await new Promise((resolve) =>
        setTimeout(resolve, chunkIndex === 0 ? 20 : 1),
      );
      return chunk.map((value) => value * 2);
    });

    expect(seenChunks).toHaveLength(3);
    expect(result).toEqual(values.map((value) => value * 2));
  });

  test('chunkedFetch limits concurrent chunk fetches', async () => {
    const values = Array.from(
      { length: WHERE_IN_CHUNK_SIZE * 6 + 1 },
      (_, index) => index,
    );
    let active = 0;
    let maxActive = 0;

    await chunkedFetch(values, async (chunk: number[]) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
      return chunk;
    });

    expect(maxActive).toBeLessThanOrEqual(CHUNKED_FETCH_CONCURRENCY);
    expect(maxActive).toBeGreaterThan(1);
  });

  test('chunkedFetch propagates fetch errors instead of returning partial data', async () => {
    const values = Array.from(
      { length: WHERE_IN_CHUNK_SIZE + 1 },
      (_, index) => index,
    );

    await expect(
      chunkedFetch(values, async (chunk: number[]) => {
        if (chunk[0] >= WHERE_IN_CHUNK_SIZE) {
          throw new Error('chunk failed');
        }
        return chunk;
      }),
    ).rejects.toThrow('chunk failed');
  });

  test('perParentRun limits concurrency and maps results by parent id string', async () => {
    const parentIds = Array.from({ length: 25 }, (_, index) => index + 1);
    let active = 0;
    let maxActive = 0;

    const result = await perParentRun(
      parentIds,
      async (id: number) => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 3));
        active -= 1;
        return id * 10;
      },
      4,
    );

    expect(maxActive).toBeLessThanOrEqual(4);
    expect(result.size).toBe(parentIds.length);
    expect(result.get('1')).toBe(10);
    expect(result.get('25')).toBe(250);
  });

  test('perParentRun returns an empty map without calling the worker for empty input', async () => {
    let called = false;
    const result = await perParentRun(
      [],
      async () => {
        called = true;
        return 1;
      },
      4,
    );

    expect(called).toBe(false);
    expect(result.size).toBe(0);
  });

  test('parseBatchFields keeps root wildcards and groups nested paths by first relation', () => {
    const parsed = parseBatchFields([
      '*',
      'id',
      'author.name',
      'author.company.name',
      'comments.body',
      'comments.author.name',
    ]);

    expect(parsed.rootFields).toEqual(['*', 'id']);
    expect(parsed.subRelations.get('author')).toEqual([
      'name',
      'company.name',
    ]);
    expect(parsed.subRelations.get('comments')).toEqual([
      'body',
      'author.name',
    ]);
  });
});
