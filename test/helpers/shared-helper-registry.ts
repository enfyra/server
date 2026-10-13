import type { RuntimeRegistryService } from '../../src/engines/cache/services/runtime-registry.service';

export function emptySharedHelperRegistry(): RuntimeRegistryService {
  return { getSharedHelpers: () => [] } as unknown as RuntimeRegistryService;
}
