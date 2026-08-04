import { BadRequestException } from '../../../domain/exceptions';
import { QueryBuilderService } from '@enfyra/kernel';
import {
  GRAPHQL_OPERATION_NAMES,
  type GraphqlOperationName,
} from '../../../shared/types/graphql.types';
const GUARD_RULE_TYPES = [
  'rate_limit_by_ip',
  'rate_limit_by_user',
  'rate_limit_by_route',
  'rate_limit_by_operation',
  'ip_whitelist',
  'ip_blacklist',
] as const;

/**
 * Hard-coded validation for `enfyra_guard` and `enfyra_guard_rule` invariants.
 *
 * This is the single enforcement point for guard data written outside the UI.
 * Guards are stored via generic CRUD (no dedicated service), so without this
 * check conflicting rows (e.g. type=graphql with a route attached) would be
 * silently dropped by the cache builder. Core-side validation keeps the
 * invariants out of DB scripts (visible/editable via API) and in code.
 *
 * Invariants enforced:
 * - type=route (default)  → route required OR isGlobal=true; gqlOperation/table must be null.
 * - type=graphql          → route must be null; methods empty; isGlobal false;
 *                           targeting via (table, gqlOperation) matrix with null = all.
 * - gqlOperation is only valid when type=graphql.
 * - guard_rule.rate_limit_by_route   → guard.type must be route.
 * - guard_rule.rate_limit_by_operation → guard.type must be graphql.
 */
export class GuardValidationService {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  /**
   * Validate a guard row on create (existing=null) or update (existing merged).
   * For PATCH, only the fields present in `body` are asserted; existing values
   * are used for the fields not being patched.
   */
  assertGuardBody(body: any, existing?: any): void {
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new BadRequestException(
        'guard data is required and must be an object',
      );
    }

    const nestedFields = ['rules', 'children'].filter((field) =>
      Object.prototype.hasOwnProperty.call(body, field),
    );
    if (nestedFields.length > 0) {
      throw new BadRequestException(
        `Nested guard writes through ${nestedFields.join(', ')} are not supported. Create child guards through enfyra_guard and rules through enfyra_guard_rule.`,
      );
    }

    const merged = { ...existing, ...body };
    const type = merged.type ?? 'route';

    if (type !== 'route' && type !== 'graphql') {
      throw new BadRequestException(
        `Invalid guard type "${type}". Expected "route" or "graphql".`,
      );
    }

    const hasRoute =
      merged.route != null &&
      merged.route !== '' &&
      !(
        typeof merged.route === 'object' &&
        Object.keys(merged.route).length === 0
      );
    const hasGlobal = merged.isGlobal === true;
    // Child guards (parent != null) are pure logic combinators — targeting is
    // owned by the root, so they need no route/isGlobal themselves.
    const hasParent =
      merged.parent != null &&
      !(
        typeof merged.parent === 'object' &&
        Object.keys(merged.parent).length === 0
      );
    const hasGqlOperation = merged.gqlOperation != null;
    const hasTable =
      merged.table != null &&
      !(
        typeof merged.table === 'object' &&
        Object.keys(merged.table).length === 0
      );

    if (
      !hasParent &&
      merged.isEnabled === true &&
      merged.position !== 'pre_auth' &&
      merged.position !== 'post_auth'
    ) {
      throw new BadRequestException(
        'Enabled root guard requires position pre_auth or post_auth.',
      );
    }

    if (type === 'route') {
      if (!hasParent && !hasRoute && !hasGlobal) {
        throw new BadRequestException(
          'Guard type=route requires a route or isGlobal=true (root guards only; child guards inherit targeting from their parent).',
        );
      }
      if (hasGqlOperation) {
        throw new BadRequestException(
          'Guard type=route cannot set gqlOperation. gqlOperation is only valid for type=graphql.',
        );
      }
      if (hasTable) {
        throw new BadRequestException(
          'Guard type=route cannot set table. table targeting is only valid for type=graphql.',
        );
      }
    } else {
      // type=graphql
      if (hasRoute) {
        throw new BadRequestException(
          'Guard type=graphql cannot have a route. GraphQL guards target (table, gqlOperation) instead.',
        );
      }
      if (merged.isGlobal === true) {
        throw new BadRequestException(
          'Guard type=graphql cannot set isGlobal. Use the (table, gqlOperation) matrix with null = all.',
        );
      }
      const methods = Array.isArray(merged.methods)
        ? merged.methods
        : merged.methods == null
          ? []
          : [merged.methods];
      if (methods.length > 0) {
        throw new BadRequestException(
          'Guard type=graphql cannot set methods. GraphQL guards have no HTTP methods.',
        );
      }
      if (
        hasGqlOperation &&
        !GRAPHQL_OPERATION_NAMES.includes(
          merged.gqlOperation as GraphqlOperationName,
        )
      ) {
        throw new BadRequestException(
          `Invalid gqlOperation "${merged.gqlOperation}". Expected one of ${GRAPHQL_OPERATION_NAMES.join(', ')}.`,
        );
      }
    }
  }

  async assertGuardCreate(body: any): Promise<void> {
    this.assertGuardBody(body);
    if (!this.hasReference(body.parent)) return;

    const parentId = this.requirePersistedReferenceId(
      body.parent,
      'Guard parent',
    );
    const guardMap = await this.loadGuardMap();
    const root = this.resolveRootFromMap(parentId, guardMap);
    this.assertEnabledRootPosition(root);
  }

  async assertGuardUpdate(id: string | number, body: any): Promise<void> {
    const existing = await this.loadGuard(id);
    this.assertGuardBody(body, existing);
    if (
      !Object.prototype.hasOwnProperty.call(body, 'type') &&
      !Object.prototype.hasOwnProperty.call(body, 'parent')
    ) {
      return;
    }
    await this.assertGuardHierarchyUpdate(id, { ...existing, ...body });
  }

  /**
   * Validate a guard_rule row on create/update. Rules live on their own route
   * (/enfyra_guard_rule), so the owning guard's type must be resolved first.
   */
  async assertGuardRuleBody(body: any, existing?: any): Promise<void> {
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new BadRequestException(
        'guard rule data is required and must be an object',
      );
    }

    const merged = { ...existing, ...body };
    const ruleType = merged.type;
    if (!GUARD_RULE_TYPES.includes(ruleType)) {
      throw new BadRequestException(
        `Invalid rule type "${ruleType}". Expected one of ${GUARD_RULE_TYPES.join(', ')}.`,
      );
    }

    const guardType = await this.resolveEffectiveGuardType(merged.guard);
    this.assertRuleTargetCompatibility(ruleType, guardType);
  }

  async assertGuardRuleUpdate(id: string | number, body: any): Promise<void> {
    const existing = await this.loadGuardRule(id);
    await this.assertGuardRuleBody(body, existing);
  }

  private async resolveEffectiveGuardType(
    guardRef: any,
  ): Promise<'route' | 'graphql'> {
    if (guardRef == null) {
      throw new BadRequestException('Guard rule requires an owning guard.');
    }

    const initialId = this.requirePersistedReferenceId(
      guardRef,
      'Guard rule owner',
    );
    let current = await this.loadGuard(initialId);

    const visited = new Set<string>();
    while (true) {
      const currentId = this.getReferenceId(current);
      if (currentId != null) {
        const key = String(currentId);
        if (visited.has(key)) {
          throw new BadRequestException(
            'Guard parent hierarchy contains a cycle.',
          );
        }
        visited.add(key);
      }

      if (!this.hasReference(current?.parent)) {
        this.assertEnabledRootPosition(current);
        return this.normalizeGuardType(current?.type);
      }
      const parentId = this.requirePersistedReferenceId(
        current.parent,
        'Guard parent',
      );
      current = await this.loadGuard(parentId);
    }
  }

  private async assertGuardHierarchyUpdate(
    id: string | number,
    proposed: any,
  ): Promise<void> {
    const guardMap = await this.loadGuardMap();
    const idKey = String(id);
    guardMap.set(idKey, { ...proposed, id });

    const affectedIds = this.collectDescendantIds(idKey, guardMap);
    for (const affectedId of affectedIds) {
      const root = this.resolveRootFromMap(affectedId, guardMap);
      this.assertEnabledRootPosition(root);
    }

    const affectedGuardIds = [...affectedIds]
      .map((affectedId) => this.getReferenceId(guardMap.get(affectedId)))
      .filter((guardId): guardId is string | number => guardId != null);
    const rulesResult = await this.queryBuilderService.find({
      table: 'enfyra_guard_rule',
      fields: ['id', 'type', 'guard'],
      filter: { guard: { _in: affectedGuardIds } },
    });
    for (const rule of rulesResult.data ?? []) {
      const guardId = this.getReferenceId(rule.guard);
      if (guardId == null || !affectedIds.has(String(guardId))) continue;
      const root = this.resolveRootFromMap(String(guardId), guardMap);
      this.assertRuleTargetCompatibility(
        rule.type,
        this.normalizeGuardType(root.type),
      );
    }
  }

  private async loadGuardMap(): Promise<Map<string, any>> {
    const result = await this.queryBuilderService.find({
      table: 'enfyra_guard',
      fields: [
        'id',
        'type',
        'parent',
        'route',
        'table',
        'methods',
        'isGlobal',
        'isEnabled',
        'position',
        'gqlOperation',
      ],
    });
    const map = new Map<string, any>();
    for (const guard of result.data ?? []) {
      const guardId = this.getReferenceId(guard);
      if (guardId == null) continue;
      map.set(String(guardId), guard);
    }
    return map;
  }

  private collectDescendantIds(
    rootId: string,
    guardMap: Map<string, any>,
  ): Set<string> {
    const childrenByParent = new Map<string, string[]>();
    for (const [candidateId, candidate] of guardMap) {
      const parentId = this.getReferenceId(candidate.parent);
      if (parentId == null) continue;
      const parentKey = String(parentId);
      const children = childrenByParent.get(parentKey) ?? [];
      children.push(candidateId);
      childrenByParent.set(parentKey, children);
    }

    const affected = new Set<string>();
    const pending = [rootId];
    while (pending.length > 0) {
      const currentId = pending.pop()!;
      if (affected.has(currentId)) continue;
      affected.add(currentId);
      pending.push(...(childrenByParent.get(currentId) ?? []));
    }
    return affected;
  }

  private resolveRootFromMap(
    startId: string | number,
    guardMap: Map<string, any>,
  ): any {
    const visited = new Set<string>();
    let currentId = String(startId);
    while (true) {
      if (visited.has(currentId)) {
        throw new BadRequestException(
          'Guard parent hierarchy contains a cycle.',
        );
      }
      visited.add(currentId);

      const current = guardMap.get(currentId);
      if (!current) {
        throw new BadRequestException(`Guard id ${currentId} does not exist.`);
      }
      if (!this.hasReference(current.parent)) return current;
      const parentId = this.requirePersistedReferenceId(
        current.parent,
        'Guard parent',
      );
      currentId = String(parentId);
    }
  }

  private assertRuleTargetCompatibility(
    ruleType: unknown,
    guardType: 'route' | 'graphql',
  ): void {
    if (ruleType === 'rate_limit_by_route' && guardType !== 'route') {
      throw new BadRequestException(
        'Rule rate_limit_by_route is only valid on guards with type=route.',
      );
    }
    if (ruleType === 'rate_limit_by_operation' && guardType !== 'graphql') {
      throw new BadRequestException(
        'Rule rate_limit_by_operation is only valid on guards with type=graphql.',
      );
    }
  }

  private assertEnabledRootPosition(root: any): void {
    if (
      root?.isEnabled === true &&
      root?.position !== 'pre_auth' &&
      root?.position !== 'post_auth'
    ) {
      throw new BadRequestException(
        'Enabled root guard requires position pre_auth or post_auth.',
      );
    }
  }

  private async loadGuard(id: string | number): Promise<any> {
    const result = await this.queryBuilderService.findOne({
      table: 'enfyra_guard',
      fields: [
        'id',
        'type',
        'parent',
        'route',
        'table',
        'methods',
        'isGlobal',
        'isEnabled',
        'position',
        'gqlOperation',
      ],
      where: { [this.queryBuilderService.getPkField()]: id },
    });
    if (!result) {
      throw new BadRequestException(`Guard id ${id} does not exist.`);
    }
    return result;
  }

  private async loadGuardRule(id: string | number): Promise<any> {
    const result = await this.queryBuilderService.findOne({
      table: 'enfyra_guard_rule',
      fields: ['id', 'type', 'guard'],
      where: { [this.queryBuilderService.getPkField()]: id },
    });
    if (!result) {
      throw new BadRequestException(`Guard rule id ${id} does not exist.`);
    }
    return result;
  }

  private normalizeGuardType(value: unknown): 'route' | 'graphql' {
    if (value == null || value === 'route') return 'route';
    if (value === 'graphql') return 'graphql';
    throw new BadRequestException(`Invalid guard type "${String(value)}".`);
  }

  private getReferenceId(value: any): string | number | null {
    if (value == null) return null;
    if (typeof value === 'string' || typeof value === 'number') return value;
    if (typeof value !== 'object' || Array.isArray(value)) return null;
    return value.id ?? value._id ?? null;
  }

  private hasReference(value: any): boolean {
    return (
      value != null &&
      value !== '' &&
      !(typeof value === 'object' && Object.keys(value).length === 0)
    );
  }

  private requirePersistedReferenceId(
    value: any,
    label: string,
  ): string | number {
    const id = this.getReferenceId(value);
    if (id == null) {
      throw new BadRequestException(`${label} requires a persisted guard id.`);
    }
    return id;
  }
}
