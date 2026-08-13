export interface DynamicPatCreateInput {
  userId: string | number;
  name: string;
  expiresAt?: string | null;
}

export interface DynamicPatCreateResult {
  id: string;
  name: string;
  prefix: string;
  last4: string;
  expiresAt: string;
  token: string;
}

export interface DynamicPatVerificationResult {
  userId: string;
  tokenId: string;
  expiresAt: string;
}
