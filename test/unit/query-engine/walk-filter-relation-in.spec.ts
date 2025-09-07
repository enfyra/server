import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  OneToMany,
  ManyToMany,
  JoinTable,
  DataSource,
} from 'typeorm';
import { describe, beforeAll, afterAll, it, expect } from '@jest/globals';
import { walkFilter } from '../../../src/infrastructure/query-engine/utils/walk-filter';

// Test Entities
@Entity('user')
class User {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  name: string;

  @OneToMany(() => Post, (post) => post.author)
  posts: Post[];

  @ManyToMany(() => Role, (role) => role.users)
  @JoinTable({
    name: 'user_roles',
    joinColumn: { name: 'user_id', referencedColumnName: 'id' },
    inverseJoinColumn: { name: 'role_id', referencedColumnName: 'id' }
  })
  roles: Role[];
}

@Entity('post')
class Post {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  title: string;

  @ManyToOne(() => User, (user) => user.posts)
  author: User;

  @ManyToMany(() => Category, (category) => category.posts)
  @JoinTable({
    name: 'post_categories',
    joinColumn: { name: 'post_id', referencedColumnName: 'id' },
    inverseJoinColumn: { name: 'category_id', referencedColumnName: 'id' }
  })
  categories: Category[];
}

@Entity('category')
class Category {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  name: string;

  @ManyToMany(() => Post, (post) => post.categories)
  posts: Post[];
}

@Entity('role')
class Role {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  name: string;

  @ManyToMany(() => User, (user) => user.roles)
  users: User[];
}

describe('walkFilter - Relation _in/_not_in Operators', () => {
  let dataSource: DataSource;
  let users: User[];
  let posts: Post[];
  let categories: Category[];
  let roles: Role[];

  beforeAll(async () => {
    dataSource = new DataSource({
      type: 'sqlite',
      database: ':memory:',
      dropSchema: true,
      synchronize: true,
      entities: [User, Post, Category, Role],
    });
    await dataSource.initialize();

    // Setup test data
    const userRepo = dataSource.getRepository(User);
    const postRepo = dataSource.getRepository(Post);
    const categoryRepo = dataSource.getRepository(Category);
    const roleRepo = dataSource.getRepository(Role);

    // Create categories
    const categoryData = [
      { name: 'Technology' },
      { name: 'Sports' },
      { name: 'Music' },
      { name: 'Travel' }
    ];
    categories = await categoryRepo.save(categoryData);

    // Create roles
    const roleData = [
      { name: 'Admin' },
      { name: 'Editor' },
      { name: 'Viewer' }
    ];
    roles = await roleRepo.save(roleData);

    // Create users
    const userData = [
      { name: 'John' },
      { name: 'Jane' },
      { name: 'Bob' },
      { name: 'Alice' }
    ];
    users = await userRepo.save(userData);

    // Assign roles to users
    users[0].roles = [roles[0], roles[1]]; // John: Admin, Editor
    users[1].roles = [roles[1]]; // Jane: Editor
    users[2].roles = [roles[2]]; // Bob: Viewer
    users[3].roles = [roles[0]]; // Alice: Admin
    await userRepo.save(users);

    // Create posts
    const postData = [
      { title: 'Tech Post 1', author: users[0] },
      { title: 'Sports Post 1', author: users[1] },
      { title: 'Music Post 1', author: users[2] },
      { title: 'Travel Post 1', author: users[3] },
      { title: 'Tech Post 2', author: users[0] }
    ];
    posts = await postRepo.save(postData);

    // Assign categories to posts
    posts[0].categories = [categories[0]]; // Tech Post 1: Technology
    posts[1].categories = [categories[1]]; // Sports Post 1: Sports
    posts[2].categories = [categories[2]]; // Music Post 1: Music
    posts[3].categories = [categories[3]]; // Travel Post 1: Travel
    posts[4].categories = [categories[0], categories[2]]; // Tech Post 2: Technology, Music
    await postRepo.save(posts);
  });

  afterAll(async () => {
    if (dataSource?.isInitialized) {
      await dataSource.destroy();
    }
  });

  describe('Many-to-Many Relations', () => {
    it('should handle _in operator for post categories', () => {
      const postMeta = dataSource.getMetadata(Post);
      const filter = {
        categories: { _in: [categories[0].id, categories[1].id] } // Technology, Sports
      };

      const result = walkFilter({
        filter,
        currentMeta: postMeta,
        currentAlias: 'post',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      // Should generate subquery for many-to-many relation
      expect(part.sql).toContain('post.id IN');
      expect(part.sql).toContain('SELECT post_id FROM post_categories');
      expect(part.sql).toContain('category_id IN');
      expect(part.params).toHaveProperty('p1', categories[0].id);
      expect(part.params).toHaveProperty('p2', categories[1].id);
    });

    it('should handle _not_in operator for post categories', () => {
      const postMeta = dataSource.getMetadata(Post);
      const filter = {
        categories: { _not_in: [categories[2].id] } // Not Music
      };

      const result = walkFilter({
        filter,
        currentMeta: postMeta,
        currentAlias: 'post',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('post.id NOT IN');
      expect(part.sql).toContain('SELECT post_id FROM post_categories');
      expect(part.params).toHaveProperty('p1', categories[2].id);
    });

    it('should handle _in operator for user roles', () => {
      const userMeta = dataSource.getMetadata(User);
      const filter = {
        roles: { _in: [roles[0].id] } // Admin role only
      };

      const result = walkFilter({
        filter,
        currentMeta: userMeta,
        currentAlias: 'user',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('user.id IN');
      expect(part.sql).toContain('SELECT user_id FROM user_roles');
      expect(part.sql).toContain('role_id IN');
      expect(part.params).toHaveProperty('p1', roles[0].id);
    });
  });

  describe('One-to-Many Relations', () => {
    it('should handle _in operator for user posts', () => {
      const userMeta = dataSource.getMetadata(User);
      const filter = {
        posts: { _in: [posts[0].id, posts[4].id] } // John's posts
      };

      const result = walkFilter({
        filter,
        currentMeta: userMeta,
        currentAlias: 'user',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      // For one-to-many, should use direct relation
      expect(part.sql).toContain('user.id IN');
      expect(part.sql).toContain('SELECT id FROM post');
      expect(part.params).toHaveProperty('p1', posts[0].id);
      expect(part.params).toHaveProperty('p2', posts[4].id);
    });
  });

  describe('String Parsing and Type Casting', () => {
    it('should parse string array for _in operator', () => {
      const postMeta = dataSource.getMetadata(Post);
      const filter = {
        categories: { _in: `[${categories[0].id},${categories[1].id}]` } // String: "[1,2]"
      };

      const result = walkFilter({
        filter,
        currentMeta: postMeta,
        currentAlias: 'post',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('post.id IN');
      expect(part.params).toHaveProperty('p1', categories[0].id);
      expect(part.params).toHaveProperty('p2', categories[1].id);
      // Check that values are numbers, not strings
      expect(typeof part.params.p1).toBe('number');
      expect(typeof part.params.p2).toBe('number');
    });

    it('should parse string array for _not_in operator', () => {
      const userMeta = dataSource.getMetadata(User);
      const filter = {
        roles: { _not_in: `[${roles[0].id}]` } // String: "[1]"
      };

      const result = walkFilter({
        filter,
        currentMeta: userMeta,
        currentAlias: 'user',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('user.id NOT IN');
      expect(part.params).toHaveProperty('p1', roles[0].id);
      expect(typeof part.params.p1).toBe('number');
    });

    it('should handle string numbers and cast them to integers', () => {
      const postMeta = dataSource.getMetadata(Post);
      const filter = {
        categories: { _in: '["1", "2"]' } // String numbers in array
      };

      const result = walkFilter({
        filter,
        currentMeta: postMeta,
        currentAlias: 'post',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.params).toHaveProperty('p1', 1); // Should be number 1, not string "1"
      expect(part.params).toHaveProperty('p2', 2); // Should be number 2, not string "2"
      expect(typeof part.params.p1).toBe('number');
      expect(typeof part.params.p2).toBe('number');
    });

    it('should handle invalid JSON string gracefully', () => {
      const postMeta = dataSource.getMetadata(Post);
      const filter = {
        categories: { _in: '[1,2' } // Invalid JSON - missing closing bracket
      };

      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      
      const result = walkFilter({
        filter,
        currentMeta: postMeta,
        currentAlias: 'post',
      });

      expect(result.parts).toHaveLength(0);
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('[Relation] ❌ Failed to parse _in value')
      );
      
      consoleSpy.mockRestore();
    });

    it('should handle invalid number strings', () => {
      const postMeta = dataSource.getMetadata(Post);
      const filter = {
        categories: { _in: '["invalid", "numbers"]' }
      };

      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      
      const result = walkFilter({
        filter,
        currentMeta: postMeta,
        currentAlias: 'post',
      });

      expect(result.parts).toHaveLength(0); // Should skip due to all invalid values
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('[Relation] ❌ No valid values after type casting')
      );
      
      consoleSpy.mockRestore();
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty array for _in (always false)', () => {
      const postMeta = dataSource.getMetadata(Post);
      const filter = {
        categories: { _in: [] }
      };

      const result = walkFilter({
        filter,
        currentMeta: postMeta,
        currentAlias: 'post',
      });

      expect(result.parts).toHaveLength(1);
      expect(result.parts[0].sql).toBe('1 = 0'); // Always false
    });

    it('should handle empty array for _not_in (always true)', () => {
      const postMeta = dataSource.getMetadata(Post);
      const filter = {
        categories: { _not_in: [] }
      };

      const result = walkFilter({
        filter,
        currentMeta: postMeta,
        currentAlias: 'post',
      });

      expect(result.parts).toHaveLength(1);
      expect(result.parts[0].sql).toBe('1 = 1'); // Always true
    });

    it('should handle non-array value with error', () => {
      const postMeta = dataSource.getMetadata(Post);
      const filter = {
        categories: { _in: "not-an-array" }
      };

      // Should not throw but log error and skip
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      
      const result = walkFilter({
        filter,
        currentMeta: postMeta,
        currentAlias: 'post',
      });

      expect(result.parts).toHaveLength(0);
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('[Relation] ❌ Failed to parse _in value')
      );
      
      consoleSpy.mockRestore();
    });
  });

  describe('Combined Filters', () => {
    it('should handle multiple relation filters with AND', () => {
      const userMeta = dataSource.getMetadata(User);
      const filter = {
        roles: { _in: [roles[0].id, roles[1].id] }, // Admin or Editor
        posts: { _in: [posts[0].id] } // Has specific post
      };

      const result = walkFilter({
        filter,
        currentMeta: userMeta,
        currentAlias: 'user',
      });

      expect(result.parts).toHaveLength(2);
      expect(result.parts[0].operator).toBe('AND');
      expect(result.parts[1].operator).toBe('AND');
    });

    it('should handle _or with relation filters', () => {
      const userMeta = dataSource.getMetadata(User);
      const filter = {
        _or: [
          { roles: { _in: [roles[0].id] } }, // Admin
          { roles: { _in: [roles[2].id] } }  // Viewer
        ]
      };

      const result = walkFilter({
        filter,
        currentMeta: userMeta,
        currentAlias: 'user',
      });

      expect(result.parts).toHaveLength(2);
      expect(result.parts[0].operator).toBe('OR');
      expect(result.parts[1].operator).toBe('OR');
    });
  });
});