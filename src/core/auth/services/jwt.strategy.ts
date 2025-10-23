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

  async validate({ id }: { id: string }) {
    const isMongoDB = this.queryBuilder.isMongoDb();
    const idField = isMongoDB ? '_id' : 'id';
    const idValue = isMongoDB ? (typeof id === 'string' ? new ObjectId(id) : id) : id;
    const user = await this.queryBuilder.findOneWhere('user_definition', { [idField]: idValue });
    if (user) {
      const roleField = isMongoDB ? 'role' : 'roleId';
      const roleId = user[roleField];
      
      if (roleId) {
        user.role = await this.queryBuilder.findOneWhere('role_definition', { [idField]: roleId });
      }
    }
    
    return user;
  }
}
