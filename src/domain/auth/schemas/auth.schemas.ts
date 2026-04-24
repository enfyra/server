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

export type LoginBody = z.infer<typeof loginSchema>;
export type RefreshTokenBody = z.infer<typeof refreshTokenSchema>;
export type LogoutBody = z.infer<typeof logoutSchema>;
