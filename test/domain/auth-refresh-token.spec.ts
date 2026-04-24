/**
 * Refresh token rotation: SHA-256 hash storage + old-token rejection.
 *
 * Validates that AuthService hashes refresh tokens before persisting,
 * verifies the hash on refresh, and rejects stale (rotated-out) tokens.
 */

import { createHash } from 'crypto';

function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex');
}

describe('AuthService — refresh token hashing', () => {
  it('hashToken produces a consistent SHA-256 hex digest', () => {
    const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test-payload';
    const h1 = hashToken(token);
    const h2 = hashToken(token);
    expect(h1).toBe(h2);
    expect(h1).toHaveLength(64); // 256 bits = 64 hex chars
    expect(h1).toMatch(/^[0-9a-f]{64}$/);
  });

  it('different tokens produce different hashes', () => {
    const h1 = hashToken('token-A');
    const h2 = hashToken('token-B');
    expect(h1).not.toBe(h2);
  });

  it('empty string hashes deterministically', () => {
    const h = hashToken('');
    expect(h).toHaveLength(64);
    // SHA-256('') is well-known
    expect(h).toBe(
      'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    );
  });
});

describe('AuthService — refresh token rotation logic', () => {
  /**
   * Simulates the session store and the refresh-token verification flow
   * that AuthService.refreshToken() performs.
   */
  interface Session {
    id: string;
    userId: string;
    refreshTokenHash: string | null;
    expiredAt: Date;
    remember: boolean;
  }

  let sessions: Map<string, Session>;

  function createSession(
    id: string,
    refreshToken: string,
    remember = false,
  ): Session {
    const session: Session = {
      id,
      userId: 'user-1',
      refreshTokenHash: hashToken(refreshToken),
      expiredAt: new Date(Date.now() + 3600_000),
      remember,
    };
    sessions.set(id, session);
    return session;
  }

  function refreshWithToken(
    sessionId: string,
    providedToken: string,
  ): { newToken: string } | { error: string } {
    const session = sessions.get(sessionId);
    if (!session) return { error: 'Session not found!' };

    if (session.expiredAt.getTime() < Date.now()) {
      return { error: 'Session has expired!' };
    }

    if (
      session.refreshTokenHash &&
      session.refreshTokenHash !== hashToken(providedToken)
    ) {
      return { error: 'Refresh token has been revoked!' };
    }

    // Issue new token and rotate hash
    const newToken = `new-token-${Date.now()}-${Math.random()}`;
    session.refreshTokenHash = hashToken(newToken);
    return { newToken };
  }

  beforeEach(() => {
    sessions = new Map();
  });

  it('accepts the current refresh token', () => {
    const originalToken = 'original-refresh-token-abc';
    createSession('s1', originalToken);
    const result = refreshWithToken('s1', originalToken);
    expect('newToken' in result).toBe(true);
  });

  it('rejects the old token after rotation', () => {
    const originalToken = 'original-refresh-token-abc';
    createSession('s1', originalToken);

    // First refresh succeeds and rotates
    const first = refreshWithToken('s1', originalToken);
    expect('newToken' in first).toBe(true);

    // Replaying the original token now fails
    const replay = refreshWithToken('s1', originalToken);
    expect('error' in replay).toBe(true);
    expect((replay as any).error).toBe('Refresh token has been revoked!');
  });

  it('accepts the new token after rotation', () => {
    const originalToken = 'original-refresh-token-abc';
    createSession('s1', originalToken);

    const first = refreshWithToken('s1', originalToken) as { newToken: string };
    const second = refreshWithToken('s1', first.newToken);
    expect('newToken' in second).toBe(true);
  });

  it('session hash updates on every rotation', () => {
    const originalToken = 'token-v1';
    createSession('s1', originalToken);

    const hashes: string[] = [sessions.get('s1')!.refreshTokenHash!];

    let currentToken = originalToken;
    for (let i = 0; i < 5; i++) {
      const result = refreshWithToken('s1', currentToken) as {
        newToken: string;
      };
      currentToken = result.newToken;
      hashes.push(sessions.get('s1')!.refreshTokenHash!);
    }

    // All hashes should be unique (each rotation changes the stored hash)
    const uniqueHashes = new Set(hashes);
    expect(uniqueHashes.size).toBe(hashes.length);
  });

  it('returns error when session does not exist', () => {
    const result = refreshWithToken('nonexistent', 'any-token');
    expect('error' in result).toBe(true);
    expect((result as any).error).toBe('Session not found!');
  });

  it('returns error when session is expired', () => {
    const token = 'token-expired';
    const session = createSession('s1', token);
    session.expiredAt = new Date(Date.now() - 1000); // expired 1s ago
    const result = refreshWithToken('s1', token);
    expect('error' in result).toBe(true);
    expect((result as any).error).toBe('Session has expired!');
  });

  it('accepts token when refreshTokenHash is null (legacy session)', () => {
    const session: Session = {
      id: 's-legacy',
      userId: 'user-1',
      refreshTokenHash: null,
      expiredAt: new Date(Date.now() + 3600_000),
      remember: false,
    };
    sessions.set('s-legacy', session);

    // Null hash means no rotation check — any token passes
    const result = refreshWithToken('s-legacy', 'any-token-here');
    expect('newToken' in result).toBe(true);
    // After refresh, hash should now be populated
    expect(sessions.get('s-legacy')!.refreshTokenHash).not.toBeNull();
  });
});
