import { z } from 'zod';

export const loginSchema = z
  .object({
    email: z.email(),
    password: z.string().min(1),
    remember: z.boolean().optional(),
  })
  .strict();

export const refreshTokenSchema = z
  .object({
    refreshToken: z.string().min(1),
  })
  .strict();

export const logoutSchema = refreshTokenSchema;

const apiTokenExpirySchema = z.string().refine((value) => {
  if (value === 'never') return true;
  const date = new Date(value);
  return !Number.isNaN(date.getTime());
}, 'expiresAt must be "never" or an ISO datetime');

export const createApiTokenSchema = z
  .object({
    name: z.string().trim().min(1).max(120),
    expiresAt: apiTokenExpirySchema,
  })
  .strict();

export const exchangeApiTokenSchema = z
  .object({
    apiToken: z.string().min(1),
  })
  .strict();

export type LoginBody = z.infer<typeof loginSchema>;
export type RefreshTokenBody = z.infer<typeof refreshTokenSchema>;
export type LogoutBody = z.infer<typeof logoutSchema>;
export type CreateApiTokenBody = z.infer<typeof createApiTokenSchema>;
export type ExchangeApiTokenBody = z.infer<typeof exchangeApiTokenSchema>;
