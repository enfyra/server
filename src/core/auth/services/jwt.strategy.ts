import { ExtractJwt, Strategy } from 'passport-jwt';
import { PassportStrategy } from '@nestjs/passport';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(
    configService: ConfigService,
    private dataSourceService: DataSourceService,
  ) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: configService.get('SECRET_KEY'),
    });
  }

  async validate({ id }: { id: string }) {
    const userDefRepo = this.dataSourceService.getRepository('user_definition');
    const user = await userDefRepo.findOne({
      where: {
        id,
      },
      relations: ['role'],
    });
    return user;
  }
}
