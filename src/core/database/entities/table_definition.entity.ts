import { Entity, Unique, PrimaryGeneratedColumn, Column, OneToMany, CreateDateColumn, UpdateDateColumn } from 'typeorm';
import { Column_definition } from './column_definition.entity';
import { Relation_definition } from './relation_definition.entity';

@Entity('table_definition')
@Unique(['name'])
@Unique(['alias'])
export class Table_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "varchar", nullable: true })
    alias: string;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "simple-json", nullable: true })
    indexes: any;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "varchar", nullable: false })
    name: string;
    @Column({ type: "simple-json", nullable: true })
    uniques: any;
    @OneToMany('Column_definition', (rel: any) => rel.table, { cascade: true })
    columns: any;
    @OneToMany('Relation_definition', (rel: any) => rel.sourceTable, { cascade: true })
    relations: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
