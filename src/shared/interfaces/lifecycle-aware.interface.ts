export interface LifecycleAware {
  init?(): Promise<void> | void;
  onDestroy?(): void;
}
