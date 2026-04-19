// Jest TypeScript namespace compatibility shim for Vitest.
// Maps the subset of `jest.*` types used in this codebase onto vitest's `Mock` / `MockedFunction` / `Mocked`.
import type {
  Mock,
  MockedFunction,
  MockedObject,
  Mocked as ViMocked,
} from 'vitest';

declare global {
  namespace jest {
    type Mock<T extends (...args: any[]) => any = (...args: any[]) => any> = Mock<T>;
    type MockedFunction<T extends (...args: any[]) => any> = MockedFunction<T>;
    type Mocked<T> = ViMocked<T>;
    type MockedObject<T> = MockedObject<T>;
  }
}

export {};
