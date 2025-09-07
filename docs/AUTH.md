# Authentication Documentation

## Overview

Enfyra Backend uses JWT (JSON Web Tokens) for authentication and implements role-based access control (RBAC) for authorization. The system supports both REST API and GraphQL authentication.

## Authentication Flow

### 1. Login Process

**REST API:**

```http
POST /auth/login
Content-Type: application/json

{
  "email": "enfyra@admin.com",
  "password": "1234"
}
```

**Response:**

```json
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expTime": 1754378861000,
  "statusCode": 201,
  "message": "Success"
}
```

### 2. Using JWT Token

**REST API:**
```http
GET /posts
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**GraphQL API:**
```http
POST /graphql
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
Content-Type: application/json

{
  "query": "query { users { data { id name email } } }"
}
```

**GraphQL Client Headers:**
```json
{
  "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Apollo Client Setup:**
```javascript
const client = new ApolloClient({
  uri: 'http://localhost:1105/graphql',
  headers: {
    authorization: `Bearer ${token}`,
  }
});
```

## JWT Strategy

### Token Structure

```typescript
// JWT Payload
{
  "sub": "user_id",
  "email": "user@example.com",
  "role": "admin",
  "permissions": ["read", "write", "delete"],
  "iat": 1640995200,
  "exp": 1641081600
}
```

### Configuration

```typescript
// src/auth/jwt.strategy.ts
@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor() {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: process.env.SECRET_KEY,
    });
  }

  async validate(payload: any) {
    return {
      id: payload.sub,
      email: payload.email,
      role: payload.role,
      permissions: payload.permissions,
    };
  }
}
```

## Authorization

### Role-Based Access Control

Enfyra implements a flexible role-based access control system where:

- **Users** are assigned to specific **roles**
- **Roles** determine what **permissions** users have
- **Routes** can be configured to require specific roles or permissions
- **Root admins** bypass all permission checks

The system supports common roles like admin, user, and moderator, with customizable permissions for different operations like read, write, delete, and user management.

### Route Protection

Enfyra uses a dynamic route protection system managed through route definitions rather than controller decorators:

```typescript
// Routes are protected via route configuration
// All dynamic routes go through the DynamicController
@Controller()
export class DynamicController {
  @All('*splat')
  dynamicGetController(@Req() req: Request & { routeData: any; user: any }) {
    return this.dynamicService.runHandler(req);
  }
}

// Authentication is handled in middleware and route configuration
// Routes can be:
// - Public (no authentication required)
// - Protected (JWT required)
// - Role-based (specific roles required)
```

### Authentication Flow

Authentication in Enfyra is handled through:

1. **JWT Token Validation**: Tokens are verified using the JWT service
2. **Dynamic Route Resolution**: Route permissions are checked dynamically
3. **User Context Injection**: Authenticated user is available in request handlers

```typescript
// Authentication is handled in the dynamic resolver and middleware
private async canPass(currentRoute: any, accessToken: string) {
  if (!currentRoute?.isEnabled) {
    throwGqlError('404', 'NotFound');
  }

  const isPublished = currentRoute.publishedMethods.some(
    (item: any) => item.method === 'GQL_QUERY'
  );

  if (isPublished) {
    return { isAnonymous: true };
  }

  let decoded;
  try {
    decoded = this.jwtService.verify(accessToken);
  } catch {
    throwGqlError('401', 'Unauthorized');
  }

  const userRepo = this.dataSourceService.getRepository('user_definition');
  const user = await userRepo.findOne({
    where: { id: decoded.id },
    relations: ['role'],
  });

  if (!user) {
    throwGqlError('401', 'Invalid user');
  }

  const canPass =
    user.isRootAdmin ||
    currentRoute.routePermissions?.some(
      (permission: any) =>
        permission.role?.id === user.role?.id &&
        permission.methods?.includes('GQL_QUERY')
    );

  if (!canPass) {
    throwGqlError('403', 'Not allowed');
  }

  return user;
}
```

## User Management

### User Data Structure

```typescript
interface User {
  id: string;           // UUID primary key
  email: string;        // Unique email address
  password: string;     // Hashed password (bcrypt)
  isRootAdmin: boolean; // Root admin flag
  isSystem: boolean;    // System user flag
  role?: Role;          // Associated role (optional)
  createdAt: Date;      // Creation timestamp
  updatedAt: Date;      // Last update timestamp
}

interface Role {
  id: string;
  name: string;         // Role name (admin, user, etc.)
  permissions: string[];// Array of permission strings
  isSystem: boolean;    // System role flag
  createdAt: Date;
  updatedAt: Date;
}
```

### Authentication Service

The authentication service handles login validation and JWT token generation:

- **Password validation**: Uses bcrypt to compare hashed passwords
- **JWT generation**: Creates access tokens with user payload
- **Token expiration**: Configurable via environment variables
- **User context**: Provides user information to route handlers

## Password Security

### Bcrypt Service

```typescript
// src/auth/bcrypt.service.ts
@Injectable()
export class BcryptService {
  async hash(password: string): Promise<string> {
    return bcrypt.hash(password, 12);
  }

  async compare(password: string, hash: string): Promise<boolean> {
    return bcrypt.compare(password, hash);
  }
}
```

### Password Validation

The system uses simple validation through DTOs:

```typescript
// Login DTO validation
export class LoginAuthDto {
  @IsString()
  @IsNotEmpty()
  email: string;

  @IsString()
  @IsNotEmpty()
  password: string;

  @IsBoolean()
  @IsOptional()
  remember: boolean = false;
}
```

## Token Management

### Refresh Tokens

Enfyra uses a session-based refresh token system:

```typescript
// Refresh token process:
// 1. Verify the refresh token contains a valid sessionId
// 2. Look up the session in the database
// 3. Generate new access token for the user
// 4. Return new tokens

async refreshToken(body: RefreshTokenAuthDto) {
  let decoded: any;
  try {
    decoded = this.jwtService.verify(body.refreshToken);
  } catch (e) {
    throw new BadRequestException('Invalid or expired refresh token!');
  }
  
  const session = await sessionDefRepo.findOne({
    where: { id: decoded.sessionId },
    relations: ['user'],
  });
  
  if (!session) {
    throw new BadRequestException('Session not found!');
  }

  const accessToken = this.jwtService.sign(
    { id: session.user.id },
    { expiresIn: this.configService.get<string>('ACCESS_TOKEN_EXP') }
  );
  
  return { accessToken, refreshToken, expTime };
}
```

### Session Management

Instead of token blacklisting, Enfyra uses session-based logout:

```typescript
// Logout removes the session from database
async logout(body: LogoutAuthDto, req: Request & { user: any }) {
  let decoded: any;
  try {
    decoded = this.jwtService.verify(body.refreshToken);
  } catch (e) {
    throw new BadRequestException('Invalid or expired refresh token!');
  }
  
  const { sessionId } = decoded;
  const session = await sessionDefRepo.findOne({
    where: { id: sessionId },
    relations: ['user'],
  });
  
  if (!session || session.user.id !== req.user.id)
    throw new BadRequestException(`Logout failed!`);
    
  await sessionDefRepo.delete({ id: session.id });
  return 'Logout successfully!';
}
```

This approach is more secure as it completely removes the session rather than relying on blacklists.

## Security Best Practices

### 1. Token Security

- Use strong JWT secrets
- Set appropriate token expiration times
- Implement token refresh mechanism
- Blacklist invalidated tokens

### 2. Password Security

- Use bcrypt with high salt rounds
- Enforce strong password policies
- Implement rate limiting on login attempts
- Use HTTPS in production

### 3. Session Management

- Implement session timeout
- Track active sessions
- Allow users to revoke sessions
- Monitor for suspicious activity

### 4. Rate Limiting

Rate limiting should be handled at the infrastructure level by administrators:

- **Cloudflare**: Use Cloudflare's built-in rate limiting rules for login endpoints
- **Nginx/Apache**: Configure rate limiting in reverse proxy settings  
- **Load Balancers**: Most cloud load balancers provide rate limiting features
- **Firewall**: Configure firewall rules to limit repeated requests from same IP

This approach is more efficient than application-level rate limiting and provides better protection.

## Error Handling

### Authentication Errors

```typescript
// Custom exceptions
export class AuthenticationException extends CustomException {
  readonly errorCode = 'UNAUTHORIZED';
  readonly statusCode = 401;
}

export class AuthorizationException extends CustomException {
  readonly errorCode = 'FORBIDDEN';
  readonly statusCode = 403;
}
```

### Error Responses

```json
{
  "success": false,
  "message": "Authentication required",
  "statusCode": 401,
  "error": {
    "code": "UNAUTHORIZED",
    "message": "Invalid or expired token",
    "timestamp": "2025-08-05T03:54:42.610Z",
    "path": "/api/protected",
    "method": "GET"
  }
}
```

## Testing Authentication

### Unit Tests

```typescript
describe('AuthService', () => {
  let service: AuthService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [AuthService],
    }).compile();

    service = module.get<AuthService>(AuthService);
  });

  it('should validate user credentials', async () => {
    const result = await service.validateUser('test@example.com', 'password');
    expect(result).toBeDefined();
  });
});
```

### Integration Tests

```typescript
describe('Authentication', () => {
  it('should login with valid credentials', async () => {
    const response = await request(app.getHttpServer())
      .post('/auth/login')
      .send({
        email: 'enfyra@admin.com',
        password: '1234',
      })
      .expect(200);

    expect(response.body.accessToken).toBeDefined();
  });

  it('should reject invalid credentials', async () => {
    await request(app.getHttpServer())
      .post('/auth/login')
      .send({
        email: 'enfyra@admin.com',
        password: 'wrongpassword',
      })
      .expect(401);
  });
});
```

## Environment Variables

```bash
# JWT Configuration
SECRET_KEY=your-super-secret-jwt-key
ACCESS_TOKEN_EXP=15m
REFRESH_TOKEN_NO_REMEMBER_EXP=1d
REFRESH_TOKEN_REMEMBER_EXP=7d

# Security
BCRYPT_ROUNDS=12
LOGIN_MAX_ATTEMPTS=5
LOGIN_LOCKOUT_DURATION=300
```

