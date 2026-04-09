import { Injectable, Logger } from '@nestjs/common';
import { randomBytes } from 'crypto';
@Injectable()
export class InstanceService {
  private readonly logger = new Logger(InstanceService.name);
  private readonly instanceId: string;
  constructor() {
    this.instanceId = randomBytes(16).toString('hex');
    this.logger.log(`📌 Instance ID: ${this.instanceId}`);
  }
  getInstanceId(): string {
    return this.instanceId;
  }
}
