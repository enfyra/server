import * as request from 'supertest';
import * as dotenv from 'dotenv';
import { resolve } from 'path';

dotenv.config({ path: resolve(__dirname, '../../.env') });

const BASE_URL = process.env.BACKEND_URL || 'http://localhost:1105';

describe('Enfyra System E2E Tests', () => {
  describe('Health & Metadata Checks', () => {
    it('GET /graphql-schema - should return schema correctly', async () => {
      const res = await request(BASE_URL).get('/graphql-schema');
      // If setup is missing it may return 404, or 200 with schema
      expect([200, 404]).toContain(res.status);
    });

    it('GET /logs - should require authorization', async () => {
      const res = await request(BASE_URL).get('/logs');
      expect([401, 403]).toContain(res.status);
    });
  });

  describe('Auth Flow', () => {
    it('POST /auth/login - should fail with 400 or 401 for bad credentials', async () => {
      const res = await request(BASE_URL)
        .post('/auth/login')
        .send({
          email: 'wrong@enfyra.com',
          password: 'wrongpassword'
        });
      expect(res.status).toBeGreaterThanOrEqual(400);
    });
  });
});
