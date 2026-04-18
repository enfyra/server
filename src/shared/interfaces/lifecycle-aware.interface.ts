export interface LifecycleAware {
  onInit?(): Promise<void> | void;
  onDestroy?(): void;
  onBootstrap?(): Promise<void> | void;
}
