import * as fs from 'fs';
import * as path from 'path';

export const knownGlobalImports: Record<string, string> = {
  Column: 'typeorm',
  Entity: 'typeorm',
  OneToMany: 'typeorm',
  PrimaryGeneratedColumn: 'typeorm',
  ManyToMany: 'typeorm',
  ManyToOne: 'typeorm',
  OneToOne: 'typeorm',
  JoinTable: 'typeorm',
  JoinColumn: 'typeorm',
  Index: 'typeorm',
  Unique: 'typeorm',
  CreateDateColumn: 'typeorm',
  UpdateDateColumn: 'typeorm',
};

export async function loadDynamicEntities(entityDir: string) {
  const entities = [];
  if (!fs.existsSync(entityDir)) fs.mkdirSync(entityDir, { recursive: true });
  const files = fs.readdirSync(entityDir);
  for (const file of files) {
    if (file.endsWith('.js')) {
      const module = await import(path.join(entityDir, file));
      for (const exported in module) {
        entities.push(module[exported]);
      }
    }
  }
  return entities;
}
