import { asClass } from 'awilix';
import {
  BcryptService,
  AuthService,
  ApiTokenService,
  PatVerifierService,
  JwtVerifierService,
  AuthenticationService,
  OAuthService,
  OAuthExchangeCodeService,
  SessionCleanupService,
  UserRevocationService,
} from '../../domain/auth';
import { LoggingService } from '../../domain/exceptions';

export const authRegisters = {
  bcryptService: asClass(BcryptService).singleton(),
  authService: asClass(AuthService).singleton(),
  apiTokenService: asClass(ApiTokenService).singleton(),
  patVerifierService: asClass(PatVerifierService).singleton(),
  jwtVerifierService: asClass(JwtVerifierService).singleton(),
  authenticationService: asClass(AuthenticationService).singleton(),
  oauthService: asClass(OAuthService).singleton(),
  oauthExchangeCodeService: asClass(OAuthExchangeCodeService)
    .singleton()
    .disposer((service: OAuthExchangeCodeService) => service.onDestroy()),
  sessionCleanupService: asClass(SessionCleanupService)
    .singleton()
    .disposer((service: SessionCleanupService) => service.onDestroy()),
  userRevocationService: asClass(UserRevocationService).singleton(),
  loggingService: asClass(LoggingService).singleton(),
} as const;
