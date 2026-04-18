import { randomBytes } from 'crypto';

export class InstanceService {
  private readonly instanceId: string;

  constructor() {
    this.instanceId = randomBytes(16).toString('hex');
    console.log(`Instance ID: ${this.instanceId}`);
  }

  getInstanceId(): string {
    return this.instanceId;
  }
}
