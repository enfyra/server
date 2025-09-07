import { Injectable, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  ClientProxy,
  ClientProxyFactory,
  Transport,
} from '@nestjs/microservices';

@Injectable()
export class RabbitMQRegistry implements OnModuleInit {
  private rabbitmqClient: ClientProxy;

  constructor(private configService: ConfigService) {}
  async onModuleInit() {
    this.rabbitmqClient = ClientProxyFactory.create({
      transport: Transport.RMQ,
      options: {
        urls: [
          `amqp://${this.configService.get('RABBITMQ_USERNAME')}:${this.configService.get('RABBITMQ_PASSWORD')}@localhost:5672`,
        ],
        queue: 'table_changes_queue',
        queueOptions: {
          durable: false,
        },
      },
    });
    await this.rabbitmqClient.connect();
  }

  getClient() {
    return this.rabbitmqClient;
  }
}
