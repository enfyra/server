import { Entity, Unique, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, ManyToMany, CreateDateColumn, UpdateDateColumn } from 'typeorm';
import { HiddenField } from '../../../shared/decorators/hidden-field.decorator';
import { Role_definition } from './role_definition.entity';
import { Route_permission_definition } from './route_permission_definition.entity';

@Entity('user_definition')
@Unique(['email'])
@Index(['role'])
export class User_definition {
    @PrimaryGeneratedColumn('uuid')
    id: string;
    @Column({ type: "varchar", nullable: false })
    email: string;
    @Column({ type: "boolean", nullable: false, default: false, update: false })
    isRootAdmin: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "varchar", nullable: false })
    @HiddenField()
    password: string;
    @ManyToOne('Role_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    role: any;
    @ManyToMany('Route_permission_definition', (rel: any) => rel.allowedUsers, { nullable: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    allowedRoutePermissions: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
