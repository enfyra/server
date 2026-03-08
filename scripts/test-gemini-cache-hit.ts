/**
 * Test script: verify extractTokenUsage picks up Gemini cache hit.
 * Uses explicit context caching (guaranteed to return cached_content_token_count).
 * Run: GOOGLE_API_KEY=xxx npx ts-node -r tsconfig-paths/register scripts/test-gemini-cache-hit.ts
 */
import { extractTokenUsage } from '../src/modules/ai-agent/utils/token-usage.helper';

const API_KEY = process.env.GOOGLE_API_KEY;
if (!API_KEY) throw new Error('Set GOOGLE_API_KEY env var');
const BASE = 'https://generativelanguage.googleapis.com/v1beta';

async function createCache(): Promise<string> {
  const longContext = 'You are a helpful assistant. Important context:\n\n' + ('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ').repeat(60);
  const res = await fetch(`${BASE}/cachedContents?key=${API_KEY}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'models/gemini-2.5-flash',
      systemInstruction: { parts: [{ text: longContext }] },
      ttl: '3600s',
    }),
  });
  if (!res.ok) throw new Error(`Create cache failed: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return data.name; // e.g. cachedContents/xxx
}

async function generateWithCache(cacheName: string): Promise<any> {
  const res = await fetch(`${BASE}/models/gemini-2.5-flash:generateContent?key=${API_KEY}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      cachedContent: cacheName,
      contents: [{ role: 'user', parts: [{ text: 'Say only: ok' }] }],
    }),
  });
  if (!res.ok) throw new Error(`Generate failed: ${res.status} ${await res.text()}`);
  return res.json();
}

async function main() {
  console.log('--- Step 1: Create cached content ---');
  const cacheName = await createCache();
  console.log('Cache created:', cacheName);

  console.log('\n--- Step 2: Generate with cache (should have cached_content_token_count) ---');
  const response = await generateWithCache(cacheName);

  // Gemini returns usageMetadata at response.usageMetadata
  const usageMetadata = response.usageMetadata;
  console.log('Raw usageMetadata:', JSON.stringify(usageMetadata, null, 2));

  // Simulate what LangChain would pass: both raw and converted formats
  const sources = [
    usageMetadata,
    { usage_metadata: usageMetadata },
    {
      usage_metadata: {
        input_tokens: usageMetadata?.promptTokenCount,
        output_tokens: usageMetadata?.candidatesTokenCount,
        input_token_details: usageMetadata?.cachedContentTokenCount
          ? { cache_read: usageMetadata.cachedContentTokenCount }
          : undefined,
      },
    },
  ];

  for (let i = 0; i < sources.length; i++) {
    const ext = extractTokenUsage(sources[i]);
    console.log(`\nextractTokenUsage(source[${i}]):`, ext);
  }

  const ext = extractTokenUsage(usageMetadata) ?? extractTokenUsage({ usage_metadata: usageMetadata });
  if (ext?.cacheHitTokens && ext.cacheHitTokens > 0) {
    console.log('\n✅ SUCCESS: Cache hit detected! cacheHitTokens=', ext.cacheHitTokens);
  } else {
    console.log('\n❌ FAIL: No cacheHitTokens extracted. Raw cachedContentTokenCount:', usageMetadata?.cachedContentTokenCount);
  }
}

main().catch(console.error);
