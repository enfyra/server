/**
 * OpenAPI Schema Object type
 */
interface SchemaObject {
  type?: string;
  format?: string;
  properties?: Record<string, SchemaObject>;
  items?: SchemaObject;
  required?: string[];
  $ref?: string;
  [key: string]: any;
}

/**
 * Generate OpenAPI paths from route definitions
 * @param routes - Array of route definitions
 * @param restMethods - Array of REST method names (excluding GraphQL methods)
 */
export function generatePathsFromRoutes(routes: any[], restMethods: string[]): Record<string, any> {
  const paths: Record<string, any> = {};
  const restMethodsSet = new Set(restMethods);

  for (const route of routes) {
    if (!route?.path || !route?.isEnabled) continue;

    const path = route.path;
    const tableName = route.mainTable?.name;
    const publishedMethods = route.publishedMethods || [];


    // Determine available methods based on route type
    const availableMethods = new Set<string>();
    
    if (route.isExpressRoute) {
      // Routes cứng từ Express - generate methods theo controller definition
      route.handlers.forEach((handler: any) => {
        const method = handler.method?.method;
        if (method && restMethodsSet.has(method)) {
          availableMethods.add(method);
        }
      });
    } else {
      // Routes custom từ DB - chỉ generate methods có handler
      if (route.handlers && Array.isArray(route.handlers) && route.handlers.length > 0) {
        // Routes custom từ DB - chỉ generate methods có handler
        route.handlers.forEach((handler: any) => {
          const method = handler.method?.method;
          if (method && restMethodsSet.has(method)) {
            availableMethods.add(method);
          }
        });
      } else {
        // Routes DB không có handlers - check if dynamic route (có mainTable)
        if (tableName) {
          // Routes dynamic (có mainTable) - generate all methods từ method_definition
          restMethods.forEach(method => availableMethods.add(method));
        } else {
          // Routes custom không có handlers và không có mainTable - chỉ generate GET
          availableMethods.add('GET');
        }
      }
    }


    // Kiểm tra methods nào được publish (không cần auth)
    const isPublished = (method: string) => {
      return publishedMethods.some((pm: any) => pm.method === method);
    };

    // Khởi tạo object path
    if (!paths[path]) {
      paths[path] = {};
    }

    // GET - List/Find (chỉ nếu có)
    if (availableMethods.has('GET')) {
      paths[path].get = {
      tags: [getTagName(route)],
      summary: `Get ${tableName || 'records'}`,
      description: route.description || `Retrieve ${tableName || 'records'} with filtering and pagination`,
      parameters: [
        {
          name: 'fields',
          in: 'query',
          description: 'Fields to select. Supports: scalar fields, wildcard (*), nested relations (relation.field), and wildcard relations (relation.*)',
          required: false,
          schema: { type: 'string' },
          examples: {
            simple: {
              value: 'id,name,email',
              summary: 'Select specific fields'
            },
            wildcard: {
              value: '*',
              summary: 'Select all scalar fields + auto-join 1-level relations (ID only)'
            },
            nested: {
              value: 'id,name,author.name,author.email',
              summary: 'Select with nested relation fields'
            },
            wildcardRelation: {
              value: '*,author.*,comments.*',
              summary: 'Wildcard with relation wildcards (auto-joins nested relations)'
            }
          }
        },
        {
          name: 'filter',
          in: 'query',
          description: 'Filter conditions using operators: _eq, _neq, _gt, _gte, _lt, _lte, _in, _not_in, _contains, _starts_with, _ends_with, _is_null',
          required: false,
          schema: { type: 'object' },
          example: { status: { _eq: 'active' }, age: { _gte: 18 } }
        },
        {
          name: 'sort',
          in: 'query',
          description: 'Sort fields (prefix with - for descending)',
          required: false,
          schema: { type: 'string' },
          example: '-createdAt,name'
        },
        {
          name: 'page',
          in: 'query',
          description: 'Page number (starts at 1)',
          required: false,
          schema: { type: 'integer', default: 1 }
        },
        {
          name: 'limit',
          in: 'query',
          description: 'Records per page',
          required: false,
          schema: { type: 'integer', default: 10 }
        },
        {
          name: 'meta',
          in: 'query',
          description: 'Metadata to include: totalCount, filterCount, or * (all)',
          required: false,
          schema: { type: 'string', enum: ['totalCount', 'filterCount', '*'] }
        }
      ],
      responses: {
        200: {
          description: 'Successful response',
          content: {
            'application/json': {
              schema: {
                $ref: '#/components/schemas/PaginatedResponse'
              }
            }
          }
        },
        400: { $ref: '#/components/responses/BadRequest' },
        401: { $ref: '#/components/responses/Unauthorized' },
        403: { $ref: '#/components/responses/Forbidden' },
      },
      security: isPublished('GET') ? [] : [{ bearerAuth: [] }]
      };
    }

    // POST - Create (chỉ nếu có)
    if (availableMethods.has('POST')) {
      paths[path].post = {
      tags: [getTagName(route)],
      summary: `Create ${tableName || 'record'}`,
      description: route.description || `Create a new ${tableName || 'record'}`,
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: { type: 'object' }
          }
        }
      },
      responses: {
        201: {
          description: 'Created successfully',
          content: {
            'application/json': {
              schema: { type: 'object' }
            }
          }
        },
        400: { $ref: '#/components/responses/BadRequest' },
        401: { $ref: '#/components/responses/Unauthorized' },
        403: { $ref: '#/components/responses/Forbidden' },
      },
      security: isPublished('POST') ? [] : [{ bearerAuth: [] }]
      };
    }

    // PATCH và DELETE cần parameter :id
    const pathWithId = `${path}/{id}`;
    const hasIdMethods = availableMethods.has('PATCH') || availableMethods.has('DELETE');
    
    if (hasIdMethods) {
      if (!paths[pathWithId]) {
        paths[pathWithId] = {};
      }
    }

    // PATCH - Update (chỉ nếu có)
    if (availableMethods.has('PATCH')) {
      paths[pathWithId].patch = {
      tags: [getTagName(route)],
      summary: `Update ${tableName || 'record'}`,
      description: route.description || `Update an existing ${tableName || 'record'}`,
      parameters: [
        {
          name: 'id',
          in: 'path',
          description: 'Record ID',
          required: true,
          schema: { type: 'string' }
        }
      ],
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: { type: 'object' }
          }
        }
      },
      responses: {
        200: {
          description: 'Updated successfully',
          content: {
            'application/json': {
              schema: { type: 'object' }
            }
          }
        },
        400: { $ref: '#/components/responses/BadRequest' },
        401: { $ref: '#/components/responses/Unauthorized' },
        403: { $ref: '#/components/responses/Forbidden' },
        404: { $ref: '#/components/responses/NotFound' },
      },
      security: isPublished('PATCH') ? [] : [{ bearerAuth: [] }]
      };
    }

    // DELETE - Delete (chỉ nếu có)
    if (availableMethods.has('DELETE')) {
      paths[pathWithId].delete = {
      tags: [getTagName(route)],
      summary: `Delete ${tableName || 'record'}`,
      description: route.description || `Delete a ${tableName || 'record'}`,
      parameters: [
        {
          name: 'id',
          in: 'path',
          description: 'Record ID',
          required: true,
          schema: { type: 'string' }
        }
      ],
      responses: {
        200: {
          description: 'Deleted successfully',
          content: {
            'application/json': {
              schema: {
                type: 'object',
                properties: {
                  success: { type: 'boolean' },
                  message: { type: 'string' }
                }
              }
            }
          }
        },
        400: { $ref: '#/components/responses/BadRequest' },
        401: { $ref: '#/components/responses/Unauthorized' },
        403: { $ref: '#/components/responses/Forbidden' },
        404: { $ref: '#/components/responses/NotFound' },
      },
      security: isPublished('DELETE') ? [] : [{ bearerAuth: [] }]
      };
    }
  }

  return paths;
}

function getTagName(route: any): string {
  const path = route.path;
  const tableName = route.mainTable?.name;
  
  // Routes Express - dùng tên controller/service
  if (route.isExpressRoute) {
    if (path.startsWith('/auth/')) return 'Authentication';
    if (path.startsWith('/file_definition')) return 'File Management';
    if (path.startsWith('/package_definition')) return 'Package Management';
    if (path.startsWith('/me')) return 'User Profile';
    if (path.startsWith('/api-docs')) return 'API Documentation';
    if (path.startsWith('/assets/')) return 'Assets';
    return 'System';
  }
  
  // Routes DB - dùng tableName hoặc path
  if (tableName) {
    return tableName.replace('_definition', '').replace(/_/g, ' ');
  }
  
  // Custom routes - tất cả đều vào tag Custom
  return 'Custom';
}

/**
 * Tạo common response schemas
 */
export function generateCommonResponses(): Record<string, any> {
  return {
    BadRequest: {
      description: 'Bad Request',
      content: {
        'application/json': {
          schema: { $ref: '#/components/schemas/Error' }
        }
      }
    },
    Unauthorized: {
      description: 'Unauthorized - Invalid or missing authentication',
      content: {
        'application/json': {
          schema: { $ref: '#/components/schemas/Error' }
        }
      }
    },
    Forbidden: {
      description: 'Forbidden - Insufficient permissions',
      content: {
        'application/json': {
          schema: { $ref: '#/components/schemas/Error' }
        }
      }
    },
    NotFound: {
      description: 'Resource not found',
      content: {
        'application/json': {
          schema: { $ref: '#/components/schemas/Error' }
        }
      }
    }
  };
}

