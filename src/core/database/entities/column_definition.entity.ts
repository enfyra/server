import { Entity, Unique, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('column_definition')
@Unique(['name', 'table'])
@Index(['table'])
export class Column_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "simple-json", nullable: true })
    defaultValue: any;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "boolean", nullable: false, default: false })
    isGenerated: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isHidden: boolean;
    @Column({ type: "boolean", nullable: true, default: true })
    isNullable: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isPrimary: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "boolean", nullable: false, default: true })
    isUpdatable: boolean;
    @Column({ type: "varchar", nullable: false })
    name: string;
    @Column({ type: "simple-json", nullable: true })
    options: any;
    @Column({ type: "text", nullable: true })
    placeholder: string;
    @Column({ type: "varchar", nullable: false })
    type: string;
    @ManyToOne('Table_definition', (rel: any) => rel.columns, { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    table: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
