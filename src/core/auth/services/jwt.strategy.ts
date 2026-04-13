import { ExtractJwt, Strategy } from 'passport-jwt';
import { PassportStrategy } from '@nestjs/passport';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(
    configService: ConfigService,
    private queryBuilder: QueryBuilderService,
  ) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: configService.get('SECRET_KEY'),
    });
  }

  async validate(payload: { id: string; loginProvider?: string | null }) {
    const { id, loginProvider } = payload;
    const isMongoDB = this.queryBuilder.isMongoDb();
    const idField = DatabaseConfigService.getPkField();
    const idValue = isMongoDB
      ? typeof id === 'string'
        ? new ObjectId(id)
        : id
      : id;
    const user = await this.queryBuilder.findOne({
      table: 'user_definition',
      where: { [idField]: idValue },
    });
    if (!user) return null;

    const roleField = isMongoDB ? 'role' : 'roleId';
    const roleId = user[roleField];
    if (roleId) {
      user.role = await this.queryBuilder.findOne({
        table: 'role_definition',
        where: { [idField]: roleId },
      });
    }

    Object.assign(user, {
      loginProvider: loginProvider ?? null,
    });

    return user;
  }
}
