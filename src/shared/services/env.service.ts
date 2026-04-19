import { env, Env } from '../../env';

export class EnvService {
  private readonly data: Env;

  constructor() {
    this.data = env;
  }

  get<K extends keyof Env>(key: K): Env[K] {
    return this.data[key];
  }

  get isDev(): boolean {
    return this.data.NODE_ENV === 'development';
  }

  get isProd(): boolean {
    return this.data.NODE_ENV === 'production';
  }

  get isTest(): boolean {
    return this.data.NODE_ENV === 'test';
  }

  getAll(): Readonly<Env> {
    return this.data;
  }
}
