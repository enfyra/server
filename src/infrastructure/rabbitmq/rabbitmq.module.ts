import { Module } from '@nestjs/common';
import { RabbitMQRegistry } from './services/rabbitmq.service';

@Module({
  providers: [RabbitMQRegistry],
  exports: [RabbitMQRegistry],
})
export class RabbitmqModule {}
