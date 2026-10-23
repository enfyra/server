export type DynamicTransactionResourceEffect = () => void | Promise<void>;

export type DynamicTransactionScopeRunner = <T>(work: () => Promise<T>) => Promise<T>;
