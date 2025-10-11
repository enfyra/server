import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from '../src/app.module';
import { initializeDatabaseKnex } from './init-db-knex';
import { KnexService } from '../src/infrastructure/knex/knex.service';

async function testBootstrap() {
  const logger = new Logger('TestBootstrap');
  
  try {
    logger.log('🚀 Step 1: Initialize database with Knex...');
    await initializeDatabaseKnex();
    logger.log('✅ Database initialized\n');

    logger.log('🚀 Step 2: Create NestJS app...');
    const app = await NestFactory.create(AppModule, {
      logger: ['log', 'error', 'warn', 'debug'],
    });
    logger.log('✅ App created\n');

    logger.log('🚀 Step 3: Initialize app (triggers Bootstrap)...');
    await app.init();
    logger.log('✅ App initialized\n');

    logger.log('🚀 Step 4: Check metadata in DB...');
    const knexService = app.get(KnexService);
    const knex = knexService.getKnex();

    const tables = await knex('table_definition').count('* as count');
    logger.log(`📊 table_definition: ${tables[0].count} records`);

    const columns = await knex('column_definition').count('* as count');
    logger.log(`📊 column_definition: ${columns[0].count} records`);

    const relations = await knex('relation_definition').count('* as count');
    logger.log(`📊 relation_definition: ${relations[0].count} records`);

    const users = await knex('user_definition').select('*');
    logger.log(`📊 user_definition: ${users.length} records`);
    users.forEach(u => logger.log(`   - ${u.email}`));

    const settings = await knex('setting_definition').select('*');
    logger.log(`📊 setting_definition: ${settings.length} records`);
    if (settings[0]) {
      logger.log(`   isInit: ${settings[0].isInit}`);
      logger.log(`   projectName: ${settings[0].projectName}`);
    }

    logger.log('\n🎉 Bootstrap test completed successfully!');
    
    await app.close();
    process.exit(0);
  } catch (error) {
    logger.error('❌ Bootstrap test failed:', error);
    process.exit(1);
  }
}

testBootstrap();



