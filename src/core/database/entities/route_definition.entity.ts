import { Entity, Unique, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, ManyToMany, JoinTable, OneToMany, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('route_definition')
@Unique(['path'])
@Index(['mainTable'])
export class Route_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "varchar", nullable: false, default: "lucide:route" })
    icon: string;
    @Column({ type: "boolean", nullable: true, default: false })
    isEnabled: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "varchar", nullable: false })
    path: string;
    @ManyToOne('Table_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    mainTable: any;
    @ManyToMany('Table_definition', { nullable: true, cascade: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    @JoinTable()
    targetTables: any;
    @OneToMany('Route_permission_definition', (rel: any) => rel.route, { cascade: true })
    routePermissions: any;
    @OneToMany('Route_handler_definition', (rel: any) => rel.route, { cascade: true })
    handlers: any;
    @OneToMany('Hook_definition', (rel: any) => rel.route, { cascade: true })
    hooks: any;
    @ManyToMany('Method_definition', (rel: any) => rel.routes, { nullable: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    publishedMethods: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
