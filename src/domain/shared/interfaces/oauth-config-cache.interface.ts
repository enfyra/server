export interface IOAuthConfig {
  clientId: string;
  clientSecret: string;
  redirectUri: string;
  appCallbackUrl?: string | null;
  autoSetCookies?: boolean;
  isEnabled: boolean;
}

export interface IOAuthConfigCache {
  getDirectConfigByProvider(provider: string): IOAuthConfig | null;
  getAllProviders(): string[];
}
