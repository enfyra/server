import { Entity, Index, PrimaryGeneratedColumn, Column, ManyToMany, JoinTable, ManyToOne, JoinColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('hook_definition')
@Index(['route'])
export class Hook_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "text", nullable: true })
    afterHook: string;
    @Column({ type: "int", nullable: true })
    afterHookTimeout: number;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "boolean", nullable: false, default: false })
    isEnabled: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "varchar", nullable: false })
    name: string;
    @Column({ type: "text", nullable: true })
    preHook: string;
    @Column({ type: "int", nullable: true })
    preHookTimeout: number;
    @Column({ type: "int", nullable: true, default: 0 })
    priority: number;
    @ManyToMany('Method_definition', (rel: any) => rel.hooks, { nullable: true, cascade: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    @JoinTable()
    methods: any;
    @ManyToOne('Route_definition', (rel: any) => rel.hooks, { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    route: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
