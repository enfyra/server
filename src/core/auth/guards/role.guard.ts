import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { PolicyService } from '../../policy/policy.service';
import { isPolicyDeny } from '../../policy/policy.types';

@Injectable()
export class RoleGuard implements CanActivate {
  constructor(private readonly policyService: PolicyService) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest();
    const decision = this.policyService.checkRequestAccess({
      method: req.method,
      routeData: req.routeData,
      user: req.user,
    });

    if (decision.allow) return true;
    if (isPolicyDeny(decision) && decision.statusCode === 401)
      throw new UnauthorizedException();
    return false;
  }
}
