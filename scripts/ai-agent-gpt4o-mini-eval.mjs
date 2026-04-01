#!/usr/bin/env node
/**
 * E2E-style eval: Enfyra AI Agent + OpenAI gpt-4o-mini (config id from env).
 * Requires: server running, Redis, DB; credentials from server/.env
 *
 * Usage (from server/):
 *   node scripts/ai-agent-gpt4o-mini-eval.mjs
 *   AI_AGENT_CONFIG_ID=2 BACKEND_URL=http://localhost:1105 node scripts/ai-agent-gpt4o-mini-eval.mjs
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function loadEnv(envPath) {
  const out = {};
  if (!fs.existsSync(envPath)) return out;
  for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    const i = t.indexOf('=');
    if (i === -1) continue;
    const k = t.slice(0, i).trim();
    let v = t.slice(i + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    out[k] = v;
  }
  return out;
}

const env = loadEnv(path.join(__dirname, '..', '.env'));
const BASE = process.env.BACKEND_URL || env.BACKEND_URL || 'http://localhost:1105';
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || env.ADMIN_EMAIL || 'enfyra@admin.com';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || env.ADMIN_PASSWORD || '1234';
const CONFIG_ID = process.env.AI_AGENT_CONFIG_ID || env.AI_AGENT_CONFIG_ID || '2';

const CASES = [
  {
    id: 'meta_three_tables',
    persona: 'admin',
    message:
      'Liệt kê đúng 3 bảng metadata hệ thống (tên bảng _definition) và một dòng mô tả chức năng mỗi bảng.',
  },
  {
    id: 'user_roll_up',
    persona: 'admin',
    message:
      'Đọc bảng user_definition: có bao nhiêu user, liệt kê email. Nếu có roleId thì cố gắng join hoặc tra role_definition để nêu tên role.',
  },
  {
    id: 'enfyra_doc_hooks',
    persona: 'admin',
    message:
      'Theo tài liệu Enfyra (dùng doc tool nếu cần): pre-hook và post-hook khác nhau thế nào? Cho ví dụ ngắn khi nào dùng từng loại với REST handler.',
  },
  {
    id: 'ai_config_audit',
    persona: 'admin',
    message:
      'Truy vấn ai_config_definition: mỗi dòng gồm id, provider, model, isEnabled (không in apiKey). Tóm tắt một câu nên dùng config nào cho dev test.',
  },
  {
    id: 'schema_plan_only',
    persona: 'admin',
    message:
      'Tôi muốn bảng demo_agent_eval (name varchar, score int). CHỈ mô tả các tool Enfyra agent sẽ gọi theo thứ tự và tham số chính — không thực thi tạo bảng trên DB.',
  },
  {
    id: 'ambiguous_followup',
    persona: 'admin',
    message: 'Sửa lại cái đó cho đúng.',
  },
  {
    id: 'fake_table_recovery',
    persona: 'admin',
    message:
      'find_records table=totally_fake_table_xyz limit=5. Nếu lỗi, giải thích và đề xuất cách tìm đúng tên bảng trong hệ thống.',
  },
  {
    id: 'multi_tool_chain',
    persona: 'admin',
    message:
      'Bước 1: dùng get_table_details cho table_definition. Bước 2: find_records trên table_definition limit=5 fields id,name. Bước 3: tóm tắt kết quả bằng 2-3 câu tiếng Việt.',
  },
  {
    id: 'handler_test_awareness',
    persona: 'admin',
    message:
      'Tôi là dev: muốn test POST handler của route user (REST). Liệt kê input cần cho run_handler_test và một payload JSON mẫu tối thiểu (placeholder values).',
  },
];

async function login() {
  const r = await fetch(`${BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: ADMIN_EMAIL, password: ADMIN_PASSWORD, remember: false }),
  });
  if (!r.ok) throw new Error(`login ${r.status} ${await r.text()}`);
  const j = await r.json();
  const token = j.accessToken || j.token || j.data?.accessToken;
  if (!token) throw new Error('no accessToken in login response');
  return token;
}

function parseSseChunk(buffer, onEvent) {
  let idx;
  while ((idx = buffer.indexOf('\n\n')) !== -1) {
    const block = buffer.slice(0, idx);
    buffer = buffer.slice(idx + 2);
    for (const line of block.split('\n')) {
      if (line.startsWith('data:')) {
        const raw = line.slice(5).trim();
        if (raw === '[DONE]') continue;
        try {
          onEvent(JSON.parse(raw));
        } catch {
          /* ignore */
        }
      }
    }
  }
  return buffer;
}

async function runCase(token, { id, message }) {
  const url = new URL(`${BASE}/ai-agent/chat/stream`);
  url.searchParams.set('message', message);
  url.searchParams.set('config', String(CONFIG_ID));

  const ac = new AbortController();
  const maxMs = Number(process.env.AI_EVAL_TIMEOUT_MS || 120000);
  const to = setTimeout(() => ac.abort(), maxMs);

  const toolCalls = [];
  let text = '';
  let errMsg = null;
  let streamError = null;

  try {
    const res = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${token}`, Accept: 'text/event-stream' },
      signal: ac.signal,
    });
    if (!res.ok) {
      errMsg = `HTTP ${res.status} ${await res.text()}`;
      return summarize(id, { errMsg, toolCalls, text, streamError });
    }
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    let buf = '';
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      buf = parseSseChunk(buf, (ev) => {
        if (ev.type === 'text' && ev.data?.delta) text += ev.data.delta;
        if (ev.type === 'tool_call') {
          toolCalls.push({
            name: ev.data?.name,
            status: ev.data?.status,
            id: ev.data?.id,
          });
        }
        if (ev.type === 'error') streamError = ev.data?.error || JSON.stringify(ev.data);
      });
    }
  } catch (e) {
    errMsg = e.name === 'AbortError' ? `timeout_after_${maxMs}ms` : String(e.message || e);
  } finally {
    clearTimeout(to);
  }

  return summarize(id, { errMsg, toolCalls, text, streamError });
}

function summarize(id, { errMsg, toolCalls, text, streamError }) {
  const usedTools = [...new Set(toolCalls.map((t) => t.name).filter(Boolean))];
  const failedTools = toolCalls.filter((t) => t.status && t.status !== 'success' && t.status !== 'pending');
  const ok = !errMsg && !streamError && text.trim().length > 0;
  return {
    id,
    ok,
    errMsg,
    streamError,
    usedTools,
    toolCallSteps: toolCalls.length,
    failedToolEvents: failedTools.length,
    answerPreview: text.trim().slice(0, 420),
  };
}

async function main() {
  console.error(`BASE=${BASE} CONFIG_ID=${CONFIG_ID} cases=${CASES.length}`);
  const token = await login();
  const results = [];
  for (const c of CASES) {
    process.stderr.write(`\n>>> ${c.id} ... `);
    let r = await runCase(token, c);
    if (!r.ok && !r.errMsg && !r.streamError && Number(process.env.AI_EVAL_RETRY_EMPTY || 1)) {
      await new Promise((x) => setTimeout(x, 1500));
      r = await runCase(token, c);
      r.retried = true;
    }
    results.push({ ...c, ...r });
    process.stderr.write(r.ok ? 'OK\n' : `FAIL ${r.errMsg || r.streamError || 'empty_answer'}\n`);
    await new Promise((x) => setTimeout(x, 800));
  }
  console.log(JSON.stringify({ configId: CONFIG_ID, base: BASE, results }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
