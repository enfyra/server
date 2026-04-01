/**
 * Lazy-loaded doc slices aligned with Enfyra MCP instructions (mcp-server).
 * Keeps the main system prompt small; agents pull detail on demand via get_enfyra_doc.
 */

export const ENFYRA_DOC_SECTION_IDS = [
  'overview',
  'rest_routes',
  'route_vs_table',
  'auth_published',
  'graphql',
  'crud_relations',
  'handlers_hooks',
  'extension_vue',
  'flows',
  'websocket',
  'column_definition',
] as const;

export type EnfyraDocSectionId = (typeof ENFYRA_DOC_SECTION_IDS)[number];

const SECTIONS: Record<EnfyraDocSectionId, string> = {
  overview: `Enfyra REST uses GET/POST on /{table}, PATCH/DELETE on /{table}/{id}. There is NO GET /{table}/{id}; fetch one row with GET /{table}?filter={"id":{"_eq":"<id>"}}&limit=1 (or Mongo _id). API base is ENFYRA_API_URL (Nuxt often .../api; direct Nest has no /api prefix unless proxied).`,

  rest_routes: `Custom URL or handler: use route_definition with mainTable pointing at an existing table (mainTable id). Prefer create_route-style workflow: pick mainTableId from table_definition, set path, availableMethods, publishedMethods, then handlers/hooks. Path may differ from table name—confirm with route list. After schema changes, operations are sequential (migration lock).`,

  route_vs_table: `create_table: new persisted entity + columns only. create_route (new path/handler): use existing mainTableId—do NOT create a table just to expose an endpoint. Default route /{table_name} is auto-created when a table is created; extra paths are additional routes on same or other tables.`,

  auth_published: `publishedMethods: methods callable without Bearer JWT. Otherwise Authorization Bearer + routePermissions. MCP uses admin credentials; end users need tokens unless route method is published.`,

  graphql: `POST {base}/graphql, GET {base}/graphql-schema. Table in schema if enabled route has GQL_QUERY and GQL_MUTATION and mainTable. Mutations: create_{tableName}, update_{tableName}, delete_{tableName}. publishedMethods can list GQL_QUERY and GQL_MUTATION separately.`,

  crud_relations: `Use relation propertyName in payloads, e.g. {"role":{"id":1}} not foreignKeyColumn names. Many-to-many: array of {id}. Check get_table_details relations before writes. find_records: always set minimal fields, use filter; count with limit=1 and meta totalCount/filterCount.`,

  handlers_hooks: `Handler/hook scripts: macros @BODY @QUERY @PARAMS @USER @REPOS @HELPERS @THROW4xx/5xx @SOCKET @PKGS @LOGS @SHARE or $ctx. run_handler_test before saving risky handler code. Reload routes after route/handler/hook changes.`,

  extension_vue: `extension_definition: Vue SFC only <template> + <script setup>, NO imports; globals ref, useApi, useToast, UButton, etc. type=page needs menu. After create/update tell user to refresh. install packages via package_definition workflow—not raw guess.`,

  flows: `flow_definition, flow_step_definition, flow_execution_definition. Steps use @PAYLOAD @LAST @FLOW #table_name. HTTP steps: public URLs only (SSRF hardening). Trigger flows via admin API or $ctx.$dispatch in handlers.`,

  websocket: `Socket.IO: websocket_definition (namespace path), websocket_event_definition. @SOCKET in scripts. Client uses JWT in auth if requireAuth.`,

  column_definition: `There is NO REST route for column_definition. Columns are managed via PATCH table_definition with columns array (cascade). Use create_column-style workflows on table_definition, not query_table(column_definition).`,
};

export function listEnfyraDocSectionIds(): string[] {
  return [...ENFYRA_DOC_SECTION_IDS];
}

export function getEnfyraDocSection(id: string): { found: boolean; section?: string; content?: string } {
  if (!id || typeof id !== 'string') {
    return { found: false };
  }
  const key = id.trim() as EnfyraDocSectionId;
  if (!ENFYRA_DOC_SECTION_IDS.includes(key as EnfyraDocSectionId)) {
    return { found: false };
  }
  return { found: true, section: key, content: SECTIONS[key] };
}

export function getMultipleEnfyraDocSections(ids: string[]): { sections: Record<string, string>; unknown: string[] } {
  const sections: Record<string, string> = {};
  const unknown: string[] = [];
  for (const raw of ids) {
    const r = getEnfyraDocSection(String(raw));
    if (r.found && r.section && r.content) {
      sections[r.section] = r.content;
    } else {
      unknown.push(String(raw));
    }
  }
  return { sections, unknown };
}
