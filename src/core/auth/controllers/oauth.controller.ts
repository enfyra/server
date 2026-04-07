import { Controller, Get, Param, Query, Res, BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Response } from 'express';
import { createHmac, timingSafeEqual } from 'crypto';
import { OAuthService } from '../services/oauth.service';
import { OAuthConfigCacheService } from '../../../infrastructure/cache/services/oauth-config-cache.service';

@Controller('auth')
export class OAuthController {
  constructor(
    private readonly oauthService: OAuthService,
    private readonly oauthConfigCache: OAuthConfigCacheService,
    private readonly configService: ConfigService,
  ) {}

  @Get(':provider')
  async oauthLogin(
    @Param('provider') provider: string,
    @Query('redirect') redirectUrl: string,
    @Res() res: Response
  ) {
    const validProviders = ['google', 'facebook', 'github'];
    if (!validProviders.includes(provider)) {
      throw new BadRequestException(`Invalid OAuth provider: ${provider}`);
    }

    if (!redirectUrl) {
      throw new BadRequestException('Redirect URL is required');
    }

    this.validateRedirectUrl(redirectUrl, provider);

    const payload = JSON.stringify({ redirect: redirectUrl, ts: Date.now() });
    const sig = this.signState(payload);
    const state = Buffer.from(JSON.stringify({ p: payload, s: sig })).toString('base64url');
    const authUrl = this.oauthService.getAuthorizationUrl(provider as any, state);
    return res.redirect(authUrl);
  }

  @Get(':provider/callback')
  async oauthCallback(
    @Param('provider') provider: string,
    @Query('code') code: string,
    @Query('state') state: string,
    @Query('error') error: string,
    @Query('error_description') errorDescription: string,
    @Res() res: Response
  ) {
    const config = this.oauthConfigCache.getDirectConfigByProvider(provider as any);
    if (!config) {
      throw new BadRequestException(`OAuth provider '${provider}' is not configured`);
    }

    const redirectUrl = this.parseRedirectFromState(state);

    if (!redirectUrl) {
      throw new BadRequestException('Invalid or expired state parameter');
    }

    if (error) {
      const safeRedirect = this.getSafeRedirectUrl(redirectUrl, provider);
      return res.redirect(`${safeRedirect}?error=${encodeURIComponent(errorDescription || error)}`);
    }

    if (!code) {
      throw new BadRequestException('Authorization code is required');
    }

    try {
      const tokens = await this.oauthService.handleCallback(provider as any, code);

      if (!config.appCallbackUrl) {
        throw new BadRequestException('App callback URL is not configured');
      }

      const callbackUrl = new URL(config.appCallbackUrl);
      callbackUrl.searchParams.set('accessToken', tokens.accessToken);
      callbackUrl.searchParams.set('refreshToken', tokens.refreshToken);
      callbackUrl.searchParams.set('expTime', String(tokens.expTime));
      callbackUrl.searchParams.set('loginProvider', tokens.loginProvider ?? '');
      callbackUrl.searchParams.set('redirect', redirectUrl);

      return res.redirect(callbackUrl.toString());
    } catch (err: any) {
      const safeRedirect = this.getSafeRedirectUrl(redirectUrl, provider);
      return res.redirect(`${safeRedirect}?error=${encodeURIComponent(err.message || 'OAuth login failed')}`);
    }
  }

  private signState(payload: string): string {
    const secret = this.configService.get<string>('SECRET_KEY') || '';
    return createHmac('sha256', secret).update(payload).digest('hex');
  }

  private parseRedirectFromState(state?: string): string | null {
    if (!state) return null;
    try {
      const outer = JSON.parse(Buffer.from(state, 'base64url').toString());
      if (!outer?.p || !outer?.s) return null;

      const expectedSig = this.signState(outer.p);
      const actualSig = outer.s;
      if (expectedSig.length !== actualSig.length) return null;
      const equal = timingSafeEqual(Buffer.from(expectedSig), Buffer.from(actualSig));
      if (!equal) return null;

      const parsed = JSON.parse(outer.p);
      const age = Date.now() - (parsed.ts || 0);
      if (age > 600000) return null;

      return parsed.redirect || null;
    } catch {
      return null;
    }
  }

  private validateRedirectUrl(url: string, provider: string): void {
    try {
      const parsed = new URL(url);
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
        throw new BadRequestException('Invalid redirect URL protocol');
      }
    } catch (e) {
      if (e instanceof BadRequestException) throw e;
      throw new BadRequestException('Invalid redirect URL');
    }
  }

  private getSafeRedirectUrl(url: string, provider: string): string {
    try {
      const parsed = new URL(url);
      if (parsed.protocol === 'http:' || parsed.protocol === 'https:') {
        return url;
      }
    } catch {}
    const config = this.oauthConfigCache.getDirectConfigByProvider(provider as any);
    return config?.appCallbackUrl || '/';
  }
}
