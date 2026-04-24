// Jest TypeScript namespace compatibility shim for Vitest.
// Maps the subset of `jest.*` types used in this codebase onto vitest's `Mock` / `MockedFunction` / `Mocked`.
import type {
  Mock as ViMock,
  MockedFunction as ViMockedFunction,
  MockedObject as ViMockedObject,
  Mocked as ViMocked,
} from 'vitest';

declare global {
  namespace jest {
    type Mock<T extends (...args: any[]) => any = (...args: any[]) => any> =
      ViMock<T>;
    type MockedFunction<T extends (...args: any[]) => any> =
      ViMockedFunction<T>;
    type Mocked<T> = ViMocked<T>;
    type MockedObject<T> = ViMockedObject<T>;
  }
}

export {};
