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
 */
export function generatePathsFromRoutes(routes: any[]): Record<string, any> {
  const paths: Record<string, any> = {};

  for (const route of routes) {
    if (!route?.path || !route?.isEnabled) continue;

    const path = route.path;
    const tableName = route.mainTable?.name;
    const publishedMethods = route.publishedMethods || [];

    // Check which methods are published (no auth required)
    const isPublished = (method: string) => {
      return publishedMethods.some((pm: any) => pm.method === method);
    };

    paths[path] = {};

    // GET - List/Find
    paths[path].get = {
      tags: [tableName || 'Custom'],
      summary: `Get ${tableName || 'records'}`,
      description: route.description || `Retrieve ${tableName || 'records'} with filtering and pagination`,
      parameters: [
        {
          name: 'filter',
          in: 'query',
          description: 'Filter conditions (JSON object)',
          required: false,
          schema: { type: 'object' },
          example: { status: { _eq: 'active' } }
        },
        {
          name: 'fields',
          in: 'query',
          description: 'Comma-separated field names',
          required: false,
          schema: { type: 'string' },
          example: 'id,name,email'
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
          description: 'Maximum records to return',
          required: false,
          schema: { type: 'integer', default: 10 }
        },
        {
          name: 'meta',
          in: 'query',
          description: 'Metadata to include (totalCount, filterCount, or *)',
          required: false,
          schema: { type: 'string' }
        }
      ],
      responses: {
        200: {
          description: 'Successful response',
          content: {
            'application/json': {
              schema: tableName ? {
                $ref: `#/components/schemas/PaginatedResponse`
              } : { type: 'object' }
            }
          }
        },
        400: { $ref: '#/components/responses/BadRequest' },
        401: { $ref: '#/components/responses/Unauthorized' },
        403: { $ref: '#/components/responses/Forbidden' },
      },
      security: isPublished('GET') ? [] : [{ bearerAuth: [] }]
    };

    // POST - Create
    paths[path].post = {
      tags: [tableName || 'Custom'],
      summary: `Create ${tableName || 'record'}`,
      description: route.description || `Create a new ${tableName || 'record'}`,
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: tableName ? {
              $ref: `#/components/schemas/${tableName}Input`
            } : { type: 'object' }
          }
        }
      },
      responses: {
        201: {
          description: 'Created successfully',
          content: {
            'application/json': {
              schema: tableName ? {
                $ref: `#/components/schemas/${tableName}`
              } : { type: 'object' }
            }
          }
        },
        400: { $ref: '#/components/responses/BadRequest' },
        401: { $ref: '#/components/responses/Unauthorized' },
        403: { $ref: '#/components/responses/Forbidden' },
      },
      security: isPublished('POST') ? [] : [{ bearerAuth: [] }]
    };

    // PATCH - Update (with :id parameter)
    const pathWithId = `${path}/{id}`;
    if (!paths[pathWithId]) {
      paths[pathWithId] = {};
    }

    paths[pathWithId].patch = {
      tags: [tableName || 'Custom'],
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
            schema: tableName ? {
              $ref: `#/components/schemas/${tableName}Input`
            } : { type: 'object' }
          }
        }
      },
      responses: {
        200: {
          description: 'Updated successfully',
          content: {
            'application/json': {
              schema: tableName ? {
                $ref: `#/components/schemas/${tableName}`
              } : { type: 'object' }
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

    // DELETE - Delete
    paths[pathWithId].delete = {
      tags: [tableName || 'Custom'],
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

  return paths;
}

/**
 * Generate common response schemas
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

