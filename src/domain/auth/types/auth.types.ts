import type { TAuthenticatedUser } from '../../../shared/types/authenticated-user.types';

export const ENFYRA_PAT_HEADER = 'x-enfyra-pat';

export type AuthTokenPayload = {
  id: string;
  loginProvider: string | null;
  tokenType: string | null;
  tokenId?: string;
  exp?: number;
};

export type AuthSource = 'pat' | 'jwt';

export type AuthenticationInput = {
  authorization?: string | null;
  patToken?: string | null;
  headers?: Record<string, unknown> | { get(name: string): string | null };
  allowAnonymous?: boolean;
};

export type AuthenticatedRequest = {
  source: AuthSource;
  payload: AuthTokenPayload;
  user: TAuthenticatedUser;
};

export type PatVerificationResult = {
  payload: AuthTokenPayload;
  expiresAt: Date | null;
};

export type RefreshTokenResult = {
  accessToken: string;
  refreshToken: string;
  expTime: number;
  loginProvider: string | null;
};
