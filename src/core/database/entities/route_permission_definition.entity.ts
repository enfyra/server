import { Entity, Index, PrimaryGeneratedColumn, Column, ManyToMany, JoinTable, ManyToOne, JoinColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('route_permission_definition')
@Index(['role'])
@Index(['route'])
export class Route_permission_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "boolean", nullable: false, default: true })
    isEnabled: boolean;
    @ManyToMany('User_definition', (rel: any) => rel.allowedRoutePermissions, { nullable: true, cascade: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    @JoinTable()
    allowedUsers: any;
    @ManyToOne('Role_definition', (rel: any) => rel.routePermissions, { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    role: any;
    @ManyToOne('Route_definition', (rel: any) => rel.routePermissions, { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    route: any;
    @ManyToMany('Method_definition', (rel: any) => rel.route_permissions, { nullable: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    methods: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
