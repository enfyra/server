export const ENFYRA_PAT_HEADER = 'x-enfyra-pat';

export type AuthHeaderCredentialType = 'pat' | 'jwt';

export type AuthHeaderScheme = 'raw' | 'bearer';

export interface AuthHeaderConfig {
  id: string | number;
  headerKey: string;
  credentialType: AuthHeaderCredentialType;
  scheme: AuthHeaderScheme;
  priority: number;
  isEnabled: boolean;
  isSystem: boolean;
  description?: string | null;
}

export const SYSTEM_AUTH_HEADER_CONFIGS: readonly AuthHeaderConfig[] = [
  {
    id: 'system-pat',
    headerKey: ENFYRA_PAT_HEADER,
    credentialType: 'pat',
    scheme: 'raw',
    priority: 0,
    isEnabled: true,
    isSystem: true,
    description: 'Built-in Enfyra personal access token header',
  },
  {
    id: 'system-authorization',
    headerKey: 'authorization',
    credentialType: 'jwt',
    scheme: 'bearer',
    priority: 1,
    isEnabled: true,
    isSystem: true,
    description: 'Built-in Bearer access token header',
  },
];

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
  user: Record<string, any>;
};

export type PatVerificationResult = {
  payload: AuthTokenPayload;
  expiresAt: Date | null;
};
