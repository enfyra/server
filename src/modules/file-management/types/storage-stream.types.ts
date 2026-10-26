import type { Readable } from 'stream';

export type StorageReadableStream = Readable & {
  contentLength?: number;
};
