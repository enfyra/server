import { Entity, Unique, Index, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, OneToOne, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('extension_definition')
@Unique(['menu'])
@Index(['createdBy'])
@Index(['updatedBy'])
export class Extension_definition {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "text", nullable: false })
    code: string;
    @Column({ type: "text", nullable: false })
    compiledCode: string;
    @Column({ type: "text", nullable: true })
    description: string;
    @Column({ type: "varchar", nullable: false })
    extensionId: string;
    @Column({ type: "boolean", nullable: false, default: true })
    isEnabled: boolean;
    @Column({ type: "boolean", nullable: false, default: false })
    isSystem: boolean;
    @Column({ type: "varchar", nullable: false })
    name: string;
    @Column({ type: "enum", nullable: false, default: "page", enum: ['page', 'widget'] })
    type: 'page' | 'widget';
    @Column({ type: "varchar", nullable: false, default: "1.0.0" })
    version: string;
    @ManyToOne('User_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    createdBy: any;
    @OneToOne('Menu_definition', (rel: any) => rel.extension, { nullable: true, cascade: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    menu: any;
    @ManyToOne('User_definition', { nullable: true, onDelete: 'SET NULL', onUpdate: 'CASCADE' })
    @JoinColumn()
    updatedBy: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
