import { TDynamicContext } from '../../../shared/types';

export interface IRepoRegistry {
  createReposProxy(
    ctx: TDynamicContext,
    mainTableName?: string,
  ): Record<string, any>;
}
