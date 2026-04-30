export interface IOAuthConfig {
  clientId: string;
  clientSecret: string;
  redirectUri: string;
  appCallbackUrl?: string | null;
  autoSetCookies?: boolean;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  compiledCode?: string | null;
  isEnabled: boolean;
}

export interface IOAuthConfigCache {
  getDirectConfigByProvider(provider: string): Promise<IOAuthConfig | null>;
  getAllProviders(): Promise<string[]>;
}
