import { Entity, Unique, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, OneToMany, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('folder_definition')
@Unique(['parent', 'slug'])
@Index(['order'])
@Index(['parent'])
@Index(['user'])
export class Folder_definition {
    @PrimaryGeneratedColumn('uuid')
    id: string;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "varchar", nullable: true, default: "lucide:folder" })
    icon: string;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "varchar", nullable: false })
    name: string;
    @Column({ type: "int", nullable: false, default: 0 })
    order: number;
    @Column({ type: "varchar", nullable: false })
    slug: string;
    @ManyToOne('Folder_definition', (rel: any) => rel.children, { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    parent: any;
    @ManyToOne('User_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    user: any;
    @OneToMany('Folder_definition', (rel: any) => rel.parent, { cascade: true })
    children: any;
    @OneToMany('File_definition', (rel: any) => rel.folder, { cascade: true })
    files: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
