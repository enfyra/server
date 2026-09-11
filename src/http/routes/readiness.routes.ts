import type { Express } from 'express';

export function registerReadinessRoute(app: Express, isReady: () => boolean): void {
  app.get('/health/ready', (_req, res) => {
    const ready = isReady();
    res.setHeader('Cache-Control', 'no-store');
    if (!ready) res.setHeader('Retry-After', '1');
    res.status(ready ? 200 : 503).json({
      ready,
      code: ready ? 'RUNTIME_READY' : 'RUNTIME_NOT_READY',
    });
  });
}
