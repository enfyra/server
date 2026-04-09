import { Db } from 'mongodb';

export class FilterTestMongoService {
  constructor(private readonly db: Db) {}

  getDb() {
    return this.db;
  }

  collection(name: string) {
    return this.db.collection(name);
  }
}
