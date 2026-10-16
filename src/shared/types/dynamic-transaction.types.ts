export type DynamicTransactionScopeRunner = <T>(work: () => Promise<T>) => Promise<T>;
