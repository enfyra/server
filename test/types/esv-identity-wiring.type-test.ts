import type { Resolver } from 'awilix';
import type { AuthenticatedRequest } from '../../src/domain/auth/types/auth.types';
import type { PolicyService } from '../../src/domain/policy/policy.service';
import type { RuntimeRequest } from '../../src/http/types/request-context.types';
import type { RequestWithRouteData } from '../../src/shared/types/dynamic-context.types';
import type {
  CradleRegistrations,
  RootCradleRegistrations,
} from '../../src/wiring/types/registration.types';

type Assert<T extends true> = T;
type IsAny<T> = 0 extends 1 & T ? true : false;
type Assignable<From, To> = [From] extends [To] ? true : false;

type AuthenticatedUserIsNotAny = Assert<
  IsAny<AuthenticatedRequest['user']> extends false ? true : false
>;
type CustomUserFieldsStayUnknown = Assert<
  IsAny<AuthenticatedRequest['user']['customField']> extends false ? true : false
>;
type CustomUserFieldsNeedNarrowing = Assert<
  Assignable<AuthenticatedRequest['user']['customField'], string> extends false
    ? true
    : false
>;
type UserRoleIsNotAny = Assert<
  IsAny<NonNullable<AuthenticatedRequest['user']['roles']>[number]> extends false
    ? true
    : false
>;
type HttpRequestMatchesRequestScope = Assert<
  Assignable<RuntimeRequest, RequestWithRouteData>
>;
type HttpPermissionsCannotBeNull = Assert<
  Assignable<null, NonNullable<RuntimeRequest['routeData']>['routePermissions']> extends false ? true : false
>;
type HttpUserIsNotAny = Assert<
  IsAny<RuntimeRequest['user']> extends false ? true : false
>;
type HttpMethodConfigIsNotAny = Assert<
  IsAny<NonNullable<RuntimeRequest['routeData']>['routeMethodConfig']> extends false
    ? true
    : false
>;
type RequestAccessCannotReturnPreview = Assert<
  Assignable<
    { allow: false; preview: true; details: unknown },
    ReturnType<PolicyService['checkRequestAccess']>
  > extends false ? true : false
>;
type WrongAuthResolverIsRejected = Assert<
  Assignable<Resolver<string>, CradleRegistrations<'authService'>['authService']> extends false ? true : false
>;
type MissingRootRegistrationsAreRejected = Assert<
  Assignable<{}, RootCradleRegistrations> extends false ? true : false
>;
type QueueRegistrationIsChecked = Assert<
  Assignable<Resolver<string>, RootCradleRegistrations['flowQueue']> extends false ? true : false
>;
