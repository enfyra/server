import { Entity, Unique, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';
import { User_definition } from './user_definition.entity';

@Entity('bootstrap_script_definition')
@Unique(['name'])
@Index(['createdBy'])
@Index(['updatedBy'])
export class Bootstrap_script_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "boolean", nullable: false, default: true })
    isEnabled: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "text", nullable: false })
    logic: string;
    @Column({ type: "varchar", nullable: false })
    name: string;
    @Column({ type: "int", nullable: false, default: 0 })
    priority: number;
    @Column({ type: "int", nullable: true })
    timeout: number;
    @ManyToOne('User_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    createdBy: any;
    @ManyToOne('User_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    updatedBy: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
