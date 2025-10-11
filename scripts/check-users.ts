import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { KnexService } from '../src/infrastructure/knex/knex.service';

async function test() {
  const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
  const knex = app.get(KnexService).getKnex();
  
  console.log('👥 All users:');
  const users = await knex('user_definition').select('*');
  console.table(users);
  
  await app.close();
  process.exit(0);
}

test().catch(e => {
  console.error('Error:', e.message);
  process.exit(1);
});

