// Deep-fetch options used when reading `enfyra_table` metadata for runtime
// schema mutation. The generic query builder caps one-to-many / many-to-many
// deep fetches at DEEP_TO_MANY_DEFAULT_LIMIT (10) unless an explicit limit is
// provided, which silently truncates tables with more than 10 columns and
// breaks revision hashing / target attestation. Passing `limit: 0` disables
// the cap (the SQL adapter only applies a LIMIT clause when limit > 0), so
// the full persisted schema is read.
export const RUNTIME_SCHEMA_METADATA_READ_DEEP: Record<string, any> = {
  columns: {
    limit: 0,
    deep: {
      rules: { limit: 0 },
      fieldPermissions: {
        limit: 0,
        deep: {
          allowedUsers: { limit: 0 },
        },
      },
    },
  },
  relations: {
    limit: 0,
    deep: {
      fieldPermissions: {
        limit: 0,
        deep: {
          allowedUsers: { limit: 0 },
        },
      },
    },
  },
};
