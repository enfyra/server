import { Logger } from '@nestjs/common';
import { Processor, OnWorkerEvent, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { PackageManagementService } from '../services/package-management.service';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';

export interface PackageInstallJobData {
  name: string;
  version: string;
}

@Processor(SYSTEM_QUEUES.PACKAGE_INSTALL, { concurrency: 1 })
export class PackageInstallQueueService extends WorkerHost {
  private readonly logger = new Logger(PackageInstallQueueService.name);

  constructor(
    private readonly packageManagementService: PackageManagementService,
  ) {
    super();
  }

  async process(job: Job<PackageInstallJobData>): Promise<any> {
    const { name, version } = job.data;
    this.logger.log(`Installing package ${name}@${version}...`);

    const result = await this.packageManagementService.installPackage({
      name,
      type: 'Server',
      version,
    });

    this.logger.log(`Successfully installed ${name}@${result.version}`);
    return { success: true, name, version: result.version };
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job, error: Error) {
    this.logger.error(`Package install job ${job.id} failed for ${job.data?.name}: ${error.message}`);
  }

  @OnWorkerEvent('error')
  onError(error: Error) {
    this.logger.error(`Package install queue error: ${error.message}`);
  }
}
