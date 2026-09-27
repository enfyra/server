export type BootstrapLogMode = 'quiet' | 'verbose';

export type StartupProgressMode = 'Installing' | 'Upgrading' | 'Starting';

export interface StartupStep {
  label: string;
  run: () => unknown | Promise<unknown>;
}
