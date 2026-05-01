import 'dotenv/config';
import { z } from 'zod';

const EnvSchema = z.object({
  DB_URI: z.string(),
  DB_REPLICA_URIS: z.string().optional(),
  DB_READ_FROM_MASTER: z
    .enum(['true', 'false'])
    .optional()
    .transform((v) => v === 'true'),
  DB_REPLICA_HEALTH_CHECK_INTERVAL: z.coerce
    .number()
    .int()
    .positive()
    .optional(),
  REDIS_URI: z.string(),
  REDIS_HOST: z.string().optional(),
  REDIS_PASSWORD: z.string().optional(),
  REDIS_RUNTIME_CACHE: z
    .enum(['true', 'false'])
    .optional()
    .default('false')
    .transform((v) => v === 'true'),
  REDIS_USER_CACHE_LIMIT_MB: z.coerce
    .number()
    .min(0)
    .optional()
    .default(30),
  REDIS_USER_CACHE_MAX_VALUE_BYTES: z.coerce
    .number()
    .int()
    .min(0)
    .optional()
    .default(0),
  DEFAULT_TTL: z.coerce.number().int().positive().optional().default(5),
  NODE_NAME: z.string().optional().default('enfyra'),
  PORT: z.coerce.number().int().positive().default(1105),
  SECRET_KEY: z.string().min(1),
  SALT_ROUNDS: z.coerce.number().int().min(1).optional().default(10),
  ACCESS_TOKEN_EXP: z.string().optional().default('15m'),
  REFRESH_TOKEN_NO_REMEMBER_EXP: z.string().optional().default('1d'),
  REFRESH_TOKEN_REMEMBER_EXP: z.string().optional().default('7d'),
  ADMIN_EMAIL: z.string(),
  ADMIN_PASSWORD: z.string().min(1),
  NODE_ENV: z
    .enum(['development', 'production', 'test'])
    .default('development'),
  LOG_LEVEL: z.string().optional().default('info'),
  INSTANCE_ID: z.string().optional(),
  HOSTNAME: z.string().optional(),
  MONGO_FORCE_APP_TRANSACTION: z
    .enum(['0', '1'])
    .optional()
    .transform((v) => v === '1'),
  BOOTSTRAP_VERBOSE: z
    .enum(['0', '1'])
    .optional()
    .transform((v) => v === '1'),
  ISOLATED_EXECUTOR_FILE_LOG: z
    .enum(['0', '1'])
    .optional()
    .transform((v) => v === '1'),
  JEST_WORKER_ID: z.string().optional(),
});

export type Env = z.infer<typeof EnvSchema>;

const parsed = EnvSchema.safeParse(process.env);
if (!parsed.success) {
  console.error('Invalid environment variables:', parsed.error.format());
  process.exit(1);
}
export const env: Env = parsed.data;
