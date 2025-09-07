import { Entity, PrimaryGeneratedColumn, Column, ManyToMany, JoinTable, OneToMany, CreateDateColumn, UpdateDateColumn } from 'typeorm';
import { Route_permission_definition } from './route_permission_definition.entity';
import { Route_definition } from './route_definition.entity';
import { Route_handler_definition } from './route_handler_definition.entity';
import { Hook_definition } from './hook_definition.entity';

@Entity('method_definition')
export class Method_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "varchar", nullable: false })
    method: string;
    @ManyToMany('Route_permission_definition', (rel: any) => rel.methods, { nullable: true, cascade: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    @JoinTable()
    route_permissions: any;
    @ManyToMany('Route_definition', (rel: any) => rel.publishedMethods, { nullable: true, cascade: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    @JoinTable()
    routes: any;
    @OneToMany('Route_handler_definition', (rel: any) => rel.method, { cascade: true })
    handlers: any;
    @ManyToMany('Hook_definition', (rel: any) => rel.methods, { nullable: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    hooks: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
