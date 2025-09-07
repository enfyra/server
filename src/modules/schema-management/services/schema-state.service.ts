import { Injectable } from '@nestjs/common';

@Injectable()
export class SchemaStateService {
  private currentVersion: number;

  getVersion() {
    return this.currentVersion;
  }

  setVersion(newVer: number) {
    this.currentVersion = newVer;
  }
}
