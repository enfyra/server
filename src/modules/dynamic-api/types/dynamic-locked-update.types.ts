import type { LockedUpdateOptions, UpdatePayload } from '@enfyra/kernel';

export type DynamicLockedUpdateOptions = LockedUpdateOptions;

export type DynamicUpdateOptions = Pick<LockedUpdateOptions, 'id'> & UpdatePayload;
