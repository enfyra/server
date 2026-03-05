import { Controller, Get, Param, Query, Res, BadRequestException } from '@nestjs/common';
import { Response } from 'express';
import { randomUUID } from 'crypto';
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
    @Res() res: Response
  ) {
    const validProviders = ['google', 'facebook', 'github'];
    if (!validProviders.includes(provider)) {
      throw new BadRequestException(`Invalid OAuth provider: ${provider}`);
    }

    const state = randomUUID();
    const authUrl = this.oauthService.getAuthorizationUrl(provider as any, state);
    return res.redirect(authUrl);
  }

  @Public()
  @Get(':provider/callback')
  async oauthCallback(
    @Param('provider') provider: string,
    @Query('code') code: string,
    @Query('error') error: string,
    @Query('error_description') errorDescription: string,
    @Res() res: Response
  ) {
    const config = this.oauthConfigCache.getDirectConfigByProvider(provider as any);
    if (!config) {
      throw new BadRequestException(`OAuth provider '${provider}' is not configured`);
    }

    if (!config.appCallbackUrl) {
      throw new BadRequestException('App callback URL is not configured');
    }

    const callbackUrl = new URL(config.appCallbackUrl);

    if (error) {
      callbackUrl.searchParams.set('error', errorDescription || error);
      return res.redirect(callbackUrl.toString());
    }

    if (!code) {
      throw new BadRequestException('Authorization code is required');
    }

    try {
      const tokens = await this.oauthService.handleCallback(provider as any, code);
      callbackUrl.searchParams.set('accessToken', tokens.accessToken);
      callbackUrl.searchParams.set('refreshToken', tokens.refreshToken);
      callbackUrl.searchParams.set('expTime', String(tokens.expTime));

      return res.redirect(callbackUrl.toString());
    } catch (err: any) {
      callbackUrl.searchParams.set('error', err.message || 'OAuth login failed');
      return res.redirect(callbackUrl.toString());
    }
  }
}
