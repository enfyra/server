import { Entity, Unique, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';
import { Table_definition } from './table_definition.entity';

@Entity('relation_definition')
@Unique(['propertyName', 'sourceTable'])
@Index(['sourceTable'])
@Index(['targetTable'])
export class Relation_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "varchar", nullable: true })
    inversePropertyName: string;
    @Column({ type: "boolean", nullable: false, default: true })
    isNullable: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "varchar", nullable: false })
    propertyName: string;
    @Column({ type: "enum", nullable: false, enum: ['one-to-one', 'many-to-one', 'one-to-many', 'many-to-many'] })
    type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
    @ManyToOne('Table_definition', (rel: any) => rel.relations, { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    sourceTable: any;
    @ManyToOne('Table_definition', { nullable: false, onDelete: 'RESTRICT', onUpdate: 'CASCADE' })
    @JoinColumn()
    targetTable: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
