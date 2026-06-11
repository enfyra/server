export type OAuthExchangeTokenPayload = {
  accessToken: string;
  refreshToken: string;
  expTime: number;
  loginProvider: string | null;
  sessionId: string | null;
};

export type OAuthExchangePendingPayload = {
  sessionId: string | null;
  expiresAt: number;
};
