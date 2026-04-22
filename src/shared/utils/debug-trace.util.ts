export interface DebugStep {
  stage: string;
  dur: number;
  meta?: Record<string, unknown>;
}

export class DebugTrace {
  readonly steps: DebugStep[] = [];
  readonly createdAt = performance.now();
  private _plan: any = null;
  private _sql: any = null;
  private _explain: any = null;
  private _queryPath: string | null = null;

  dur(stage: string, startTs: number, meta?: Record<string, unknown>): number {
    const endTs = performance.now();
    const dur = parseFloat((endTs - startTs).toFixed(3));
    this.steps.push({ stage, dur, ...(meta ? { meta } : {}) });
    return dur;
  }

  setPlan(plan: any): void {
    this._plan = plan;
  }

  setSql(sql: any): void {
    this._sql = sql;
  }

  setExplain(explain: any): void {
    this._explain = explain;
  }

  setQueryPath(path: string): void {
    this._queryPath = path;
  }

  toJSON(): any {
    const mwTotal = this.steps
      .filter((s) => s.stage.startsWith('mw_'))
      .reduce((sum, s) => sum + s.dur, 0);
    const qbTotal = this.steps
      .filter(
        (s) =>
          s.stage.startsWith('qb_') ||
          s.stage === 'db_execute' ||
          s.stage === 'sql_executor',
      )
      .reduce((sum, s) => sum + s.dur, 0);

    const result: any = {
      totalMs: parseFloat((performance.now() - this.createdAt).toFixed(3)),
      middlewareMs: parseFloat(mwTotal.toFixed(3)),
      queryMs: parseFloat(qbTotal.toFixed(3)),
      steps: this.steps,
    };

    if (this._plan) {
      result.plan = this._plan;
    }
    if (this._queryPath) {
      result.queryPath = this._queryPath;
    }
    if (this._sql) {
      result.sql = this._sql;
    }
    if (this._explain) {
      result.explain = this._explain;
    }

    return result;
  }
}
