export interface IOAuthConfig {
  clientId: string;
  clientSecret: string;
  redirectUri: string;
  isEnabled: boolean;
}

export interface IOAuthConfigCache {
  getDirectConfigByProvider(provider: string): IOAuthConfig | null;
}
