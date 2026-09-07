export type OAuthProvider = 'google' | 'facebook' | 'github';

export type OAuthLifecycleEvent = 'user_created' | 'login';

export type OAuthProfile = {
  providerUserId: string;
  email: string;
  emailVerified: boolean | null;
  name: string | null;
  givenName: string | null;
  familyName: string | null;
  username: string | null;
  avatarUrl: string | null;
  profileUrl: string | null;
  locale: string | null;
};

export type OAuthProviderUserInfo = {
  profile: OAuthProfile;
  claims: Record<string, unknown>;
};

export type OAuthProviderToken = {
  accessToken: string;
  type?: string;
  scope?: string;
  expiresIn?: number;
};

export type OAuthLifecycleContextData = {
  oauth: {
    event: OAuthLifecycleEvent;
    provider: OAuthProvider;
    profile: OAuthProfile;
    claims: Record<string, unknown>;
    accessToken: string;
    token: {
      type: string | null;
      scopes: string[];
      expiresAt: string | null;
    };
  };
};
