import { ExtractJwt, Strategy } from 'passport-jwt';
import { PassportStrategy } from '@nestjs/passport';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { KnexService } from '../../../infrastructure/knex/knex.service';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(
    configService: ConfigService,
    private knexService: KnexService,
  ) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: configService.get('SECRET_KEY'),
    });
  }

  async validate({ id }: { id: string }) {
    const knex = this.knexService.getKnex();
    const user = await knex('user_definition')
      .where('id', id)
      .first();
    
    if (user && user.roleId) {
      user.role = await knex('role_definition')
        .where('id', user.roleId)
        .first();
    }
    
    return user;
  }
}
