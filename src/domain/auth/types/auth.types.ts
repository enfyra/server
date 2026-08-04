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
  allowAnonymous?: boolean;
};

export type AuthenticatedRequest = {
  source: AuthSource;
  payload: AuthTokenPayload;
  user: Record<string, any>;
};

export type PatVerificationResult = {
  payload: AuthTokenPayload;
  expiresAt: Date | null;
};
