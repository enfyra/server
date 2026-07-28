import type { Db } from 'mongodb';
import type { MongoService } from './mongo.service';

export function getMongoRawDb(
  mongoService: Pick<MongoService, 'getRawDb'>,
): Db {
  return mongoService.getRawDb();
}
