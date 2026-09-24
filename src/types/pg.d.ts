declare module 'pg' {
  export const types: {
    builtins: { DATE: number };
    setTypeParser: (oid: number, parseFn: (value: string) => unknown) => void;
  };
}
