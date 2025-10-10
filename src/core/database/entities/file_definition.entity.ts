import { Entity, Unique, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, OneToMany, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('file_definition')
@Unique(['location'])
@Index(['filesize'])
@Index(['status'])
@Index(['isPublished'])
@Index(['folder'])
@Index(['uploaded_by'])
export class File_definition {
    @PrimaryGeneratedColumn('uuid')
    id: string;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "varchar", nullable: false })
    filename: string;
    @Column({ type: "bigint", nullable: false })
    filesize: number;
    @Column({ type: "boolean", nullable: false, default: true })
    isPublished: boolean;
    @Column({ type: "varchar", nullable: false })
    location: string;
    @Column({ type: "varchar", nullable: false })
    mimetype: string;
    @Column({ type: "enum", nullable: false, default: "active", enum: ['active', 'archived', 'quarantine'] })
    status: 'active' | 'archived' | 'quarantine';
    @Column({ type: "varchar", nullable: true, default: "local" })
    storage: string;
    @ManyToOne('Folder_definition', (rel: any) => rel.files, { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    folder: any;
    @ManyToOne('User_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    uploaded_by: any;
    @OneToMany('File_permission_definition', (rel: any) => rel.file, { cascade: true })
    permissions: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
