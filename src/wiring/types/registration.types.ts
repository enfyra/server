import type { Resolver } from 'awilix';
import type { Cradle } from '../cradle';

export type CradleRegistrations<Keys extends keyof Cradle = keyof Cradle> = {
  [Key in Keys]: Resolver<Cradle[Key]>;
};

export type RequestCradleKey = Extract<keyof Cradle, `$${string}`>;

export type RootCradleRegistrations = CradleRegistrations<
  Exclude<keyof Cradle, RequestCradleKey>
>;
