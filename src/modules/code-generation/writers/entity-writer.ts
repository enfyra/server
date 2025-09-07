import { SourceFile } from 'ts-morph';
import { Logger } from '@nestjs/common';

const logger = new Logger('EntityWriter');

export async function writeEntityFile(
  sourceFile: SourceFile,
  entityPath: string,
) {
  await sourceFile.save();
  logger.log(`✅ Entity written: ${entityPath}`);
}
