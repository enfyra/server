export interface LifecycleAware {
  onInit?(): Promise<void> | void;
  onBootstrap?(): Promise<void> | void;
  onDestroy?(): Promise<void> | void;
}

export abstract class BaseService implements LifecycleAware {
  onInit?(): Promise<void> | void;
  onBootstrap?(): Promise<void> | void;
  onDestroy?(): Promise<void> | void;
}

