import { NestFactory } from '@nestjs/core';
import { AppModule } from './src/app.module';
import { MetadataCacheService } from './src/infrastructure/cache/services/metadata-cache.service';

const bootstrap = async () => {
    const app = await NestFactory.createApplicationContext(AppModule);
    const metadata = app.get(MetadataCacheService);
    await metadata.reload();
    const routeDef = await metadata.getTableMetadata('route_definition');
    const rel = routeDef.relations.find(r => r.propertyName === 'publishedMethods');
    console.log('mappedBy is:', rel.mappedBy);
    console.log('junctionTableName is:', rel.junctionTableName);
    await app.close();
};

bootstrap();
