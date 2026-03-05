import { Controller, Get, Param, Query, Res, BadRequestException } from '@nestjs/common';
import { Response } from 'express';
import { OAuthService } from '../services/oauth.service';
import { OAuthConfigCacheService } from '../../../infrastructure/cache/services/oauth-config-cache.service';
import { Public } from '../../../shared/decorators/public-route.decorator';

@Controller('auth')
export class OAuthController {
  constructor(
    private readonly oauthService: OAuthService,
    private readonly oauthConfigCache: OAuthConfigCacheService,
  ) {}

  @Public()
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

    const state = Buffer.from(JSON.stringify({ redirect: redirectUrl })).toString('base64url');
    const authUrl = this.oauthService.getAuthorizationUrl(provider as any, state);
    return res.redirect(authUrl);
  }

  @Public()
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
      throw new BadRequestException('Redirect URL is required');
    }

    if (error) {
      return res.redirect(`${redirectUrl}?error=${encodeURIComponent(errorDescription || error)}`);
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
      callbackUrl.searchParams.set('redirect', redirectUrl);

      return res.redirect(callbackUrl.toString());
    } catch (err: any) {
      return res.redirect(`${redirectUrl}?error=${encodeURIComponent(err.message || 'OAuth login failed')}`);
    }
  }

  private parseRedirectFromState(state?: string): string | null {
    if (!state) return null;
    try {
      const decoded = JSON.parse(Buffer.from(state, 'base64url').toString());
      return decoded.redirect || null;
    } catch {
      return null;
    }
  }
}
