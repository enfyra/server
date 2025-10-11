import { Injectable, Logger } from '@nestjs/common';
import { randomBytes } from 'crypto';

/**
 * Service to manage application instance identity
 * Used for distributed locking and instance tracking in multi-instance deployments
 */
@Injectable()
export class InstanceService {
  private readonly logger = new Logger(InstanceService.name);
  private readonly instanceId: string;

  constructor() {
    // Generate unique instance ID on service initialization
    this.instanceId = randomBytes(16).toString('hex');
    this.logger.log(`📌 Instance ID: ${this.instanceId}`);
  }

  /**
   * Get the unique instance ID
   */
  getInstanceId(): string {
    return this.instanceId;
  }
}

