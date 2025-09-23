import { Entity, PrimaryGeneratedColumn, Column, ManyToMany, CreateDateColumn, UpdateDateColumn } from 'typeorm';
import { Category } from './category.entity';

@Entity('post')
export class Post {
    @PrimaryGeneratedColumn('increment')
    id: number;
    @Column({ type: "varchar", nullable: true })
    title: string;
    @ManyToMany('Category', (rel: any) => rel.posts, { nullable: true, onDelete: 'CASCADE', onUpdate: 'CASCADE' })
    categories: any;
    @CreateDateColumn()
    createdAt: Date;
    @UpdateDateColumn()
    updatedAt: Date;
}
