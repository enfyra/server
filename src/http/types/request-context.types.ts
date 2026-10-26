import type { AwilixContainer } from 'awilix';
import type { Request } from 'express';
import type { AuthenticatedRequest } from '../../domain/auth/types/auth.types';
import type { TPolicyRequestContext } from '../../domain/policy/policy.types';
import type { TAuthenticatedUser } from '../../shared/types/authenticated-user.types';
import type { RequestWithRouteData } from '../../shared/types/dynamic-context.types';
import type { DebugTrace } from '../../shared/utils/debug-trace.util';
import type { Cradle } from '../../wiring/cradle';

type DetectedRouteData = NonNullable<RequestWithRouteData['routeData']>;
type PolicyRouteData = NonNullable<TPolicyRequestContext['routeData']>;

type HttpRouteData = Omit<
  DetectedRouteData,
  'routeMethodConfig' | 'routePermissions' | 'route'
> & {
  id?: string | number;
  routeMethodConfig?: NonNullable<PolicyRouteData['routeMethodConfig']>;
  routePermissions?: unknown[];
  route?: { path?: string };
};

export interface RuntimeRequest extends Request {
  rawBody?: string;
  routeData?: HttpRouteData;
  user?: TAuthenticatedUser | null;
  auth?: AuthenticatedRequest | null;
  scope?: AwilixContainer<Cradle>;
  _perfStart?: number;
  _perfRouteDetect?: number;
  _perfAuth?: number;
  _debug?: DebugTrace;
}
