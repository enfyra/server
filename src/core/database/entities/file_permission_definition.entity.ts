import { Entity, Unique, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('file_permission_definition')
@Unique(['allowedUsers', 'file', 'role'])
@Index(['allowedUsers'])
@Index(['file'])
@Index(['role'])
export class File_permission_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "simple-json", nullable: true })
    actions: any[];
    @Column({ type: "simple-json", nullable: true })
    allowedDomains: any;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "boolean", nullable: false, default: true })
    isEnabled: boolean;
    @ManyToOne('User_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    allowedUsers: any;
    @ManyToOne('File_definition', (rel: any) => rel.permissions, { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    file: any;
    @ManyToOne('Role_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    role: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
