
enum NodeType {
  STATIC = 'static',
  PARAM = 'param',
  WILDCARD = 'wildcard',
}

interface RouteNode {
  type: NodeType;
  path: string;
  paramName?: string;
  children: Map<string, RouteNode>;
  paramChild?: RouteNode;
  wildcardChild?: RouteNode;
  handler?: any;
}

interface RouteMatchResult {
  route: any;
  params: Record<string, string>;
}

interface RouteEngineStats {
  totalRoutes: number;
  methods: string[];
}

export class EnfyraRouteEngine {
  private roots: Map<string, RouteNode>;
  private debug: boolean;
  private routeCount: number;

  constructor(debug = false) {
    this.roots = new Map();
    this.debug = debug;
    this.routeCount = 0;
  }

  insert(method: string, path: string, route: any): void {
    const normalizedPath = this.normalizePath(path);
    const segments = this.splitPath(normalizedPath);

    if (this.debug) {
      console.log(`[EnfyraRouteEngine] INSERT ${method} ${path} -> normalized: ${normalizedPath} -> segments: [${segments.join(', ')}]`);
    }

    if (!this.roots.has(method)) {
      this.roots.set(method, this.createNode(NodeType.STATIC, ''));
    }

    let node = this.roots.get(method)!;
    this.routeCount++;

    for (let i = 0; i < segments.length; i++) {
      const segment = segments[i];
      const isLast = i === segments.length - 1;

      if (segment === '*' || segment.startsWith('*')) {
        if (!node.wildcardChild) {
          node.wildcardChild = this.createNode(NodeType.WILDCARD, segment);
        }
        node = node.wildcardChild;
        break;
      } else if (segment.startsWith(':')) {
        const paramName = segment.slice(1);
        if (!node.paramChild) {
          node.paramChild = this.createNode(NodeType.PARAM, segment, paramName);
        }
        node = node.paramChild;
      } else {
        if (!node.children.has(segment)) {
          node.children.set(segment, this.createNode(NodeType.STATIC, segment));
        }
        node = node.children.get(segment)!;
      }

      if (isLast) {
        node.handler = route;
      }
    }
  }

  find(method: string, path: string): RouteMatchResult | null {
    const root = this.roots.get(method);
    if (!root) {
      if (this.debug) {
        console.log(`[EnfyraRouteEngine] FIND ${method} ${path} -> NO ROOT`);
      }
      return null;
    }

    const normalizedPath = this.normalizePath(path);
    const segments = this.splitPath(normalizedPath);
    const params: Record<string, string> = {};

    if (this.debug) {
      console.log(`[EnfyraRouteEngine] FIND ${method} ${path} -> normalized: ${normalizedPath} -> segments: [${segments.join(', ')}]`);
    }

    const result = this.search(root, segments, 0, params);
    if (!result) {
      if (this.debug) {
        console.log(`[EnfyraRouteEngine] FIND ${method} ${path} -> NOT FOUND`);
      }
      return null;
    }

    if (this.debug) {
      console.log(`[EnfyraRouteEngine] FIND ${method} ${path} -> MATCHED`, params);
    }

    return {
      route: result.handler,
      params,
    };
  }

  private search(
    node: RouteNode,
    segments: string[],
    index: number,
    params: Record<string, string>,
  ): RouteNode | null {
    if (index === segments.length) {
      return node.handler ? node : null;
    }

    const segment = segments[index];

    const staticChild = node.children.get(segment);
    if (staticChild) {
      const result = this.search(staticChild, segments, index + 1, params);
      if (result) return result;
    }

    if (node.paramChild) {
      params[node.paramChild.paramName!] = decodeURIComponent(segment);
      const result = this.search(node.paramChild, segments, index + 1, params);
      if (result) return result;
      delete params[node.paramChild.paramName!];
    }

    if (node.wildcardChild) {
      params.splat = segments.slice(index).join('/');
      return node.wildcardChild.handler ? node.wildcardChild : null;
    }

    return null;
  }

  getStats(): RouteEngineStats {
    return {
      totalRoutes: this.routeCount,
      methods: Array.from(this.roots.keys()),
    };
  }

  private createNode(type: NodeType, path: string, paramName?: string): RouteNode {
    return {
      type,
      path,
      paramName,
      children: new Map(),
      paramChild: null,
      wildcardChild: null,
      handler: null,
    };
  }

  private normalizePath(path: string): string {
    if (!path) return '/';
    let normalized = path.startsWith('/') ? path : `/${path}`;
    if (normalized.length > 1 && normalized.endsWith('/')) {
      normalized = normalized.slice(0, -1);
    }
    return normalized;
  }

  private splitPath(path: string): string[] {
    if (path === '/') return [];
    return path.split('/').filter(segment => segment.length > 0);
  }
}
