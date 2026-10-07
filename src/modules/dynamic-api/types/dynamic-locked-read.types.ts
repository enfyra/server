import type { DynamicReadOptions } from './dynamic-read.types';
import type { DynamicMutationId } from './dynamic-mutation-lifecycle.types';

export interface DynamicLockedReadOptions
  extends Pick<DynamicReadOptions, 'fields' | 'deep'> {
  id: DynamicMutationId;
}
