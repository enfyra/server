import { describe, expect, it } from 'vitest';
import { registerAdminRoutes } from '../../src/http/routes/admin.routes';

describe('fixed authentication transport administration', () => {
  it('does not register custom header configuration endpoints', () => {
    const paths = new Set<string>();
    const register = (path: string) => { paths.add(path); };
    registerAdminRoutes({ get: register, post: register, patch: register, delete: register } as never, { cradle: {} } as never);
    expect(paths.has('/admin/auth-header/reorder')).toBe(false);
    expect(paths.has('/admin/menu/reorder')).toBe(true);
  });
});
