# Enfyra Server v2.1.0

## Features
- Added TypeScript-first dynamic script storage for `route_handler_definition`, `pre_hook_definition`, `post_hook_definition`, `bootstrap_script_definition`, `websocket_definition`, `websocket_event_definition`, and flow script steps using `scriptLanguage`, `sourceCode`, and `compiledCode`.
- Added runtime script repair for `RouteCacheService`, `WebsocketCacheService`, `FlowCacheService`, and `BootstrapScriptService` so missing or invalid `compiledCode` is regenerated from `sourceCode` and persisted back to the database.
- Added snapshot and data provisioning support for TypeScript script fields in `data/snapshot.json`, `data/default-data.json`, and `data/data-migration.json`.

## Bug Fixes
- Fixed production Docker startup by moving `typescript` into runtime `dependencies` so `dist/domain/shared/script-code.util.js` can compile TypeScript-backed scripts after dev dependencies are omitted.
- Fixed legacy script metadata cleanup in `MetadataMigrationService` so replaced script columns are removed from metadata and physical database schemas during snapshot migration.
- Improved executor fallback in `IsolatedExecutorService` so script parse failures can retry through TypeScript compilation before failing the request.
