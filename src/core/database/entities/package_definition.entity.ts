import { Entity, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';
import { User_definition } from './user_definition.entity';

@Entity('package_definition')
@Index(['installedBy'])
export class Package_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "varchar", nullable: true })
    flags: string;
    @Column({ type: "boolean", nullable: false, default: true })
    isEnabled: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "varchar", nullable: false })
    name: string;
    @Column({ type: "enum", nullable: false, enum: ['App', 'Backend'] })
    type: 'App' | 'Backend';
    @Column({ type: "varchar", nullable: false, default: "latest" })
    version: string;
    @ManyToOne('User_definition', { nullable: false, onDelete: 'RESTRICT', onUpdate: 'CASCADE' })
    @JoinColumn()
    installedBy: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
