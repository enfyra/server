import { Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';

@Injectable()
export class SqlFunctionService implements OnApplicationBootstrap {
  constructor(private dataSourceService: DataSourceService) {}

  async onApplicationBootstrap() {
    const dataSource = this.dataSourceService.getDataSource();
    const dbType = dataSource.options.type;

    if (dbType === 'mysql') {
      const exists = await this.functionExists('unaccent');
      if (!exists) {
        await this.createUnaccentFunction();
        console.log('✅ Created MySQL function: unaccent()');
      } else {
        console.log('ℹ️ MySQL function unaccent() already exists');
      }
    } else if (dbType === 'postgres') {
      await dataSource.query(`CREATE EXTENSION IF NOT EXISTS unaccent;`);
      console.log('✅ Postgres: unaccent extension ready');
    } else {
      console.warn(`⚠️ Unsupported DB_TYPE for unaccent: ${dbType}`);
    }
  }

  private async functionExists(name: string): Promise<boolean> {
    const dataSource = this.dataSourceService.getDataSource();

    const result = await dataSource.query(
      `SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_TYPE='FUNCTION' AND ROUTINE_SCHEMA=DATABASE() AND ROUTINE_NAME = ?`,
      [name],
    );
    return result.length > 0;
  }

  private async createUnaccentFunction() {
    const dataSource = this.dataSourceService.getDataSource();

    await dataSource.query(`
      DROP FUNCTION IF EXISTS unaccent;
    `);

    await dataSource.query(`
      CREATE FUNCTION unaccent(input TEXT) RETURNS TEXT
      DETERMINISTIC
      BEGIN
        SET input = REPLACE(input, 'á', 'a');
        SET input = REPLACE(input, 'à', 'a');
        SET input = REPLACE(input, 'ả', 'a');
        SET input = REPLACE(input, 'ã', 'a');
        SET input = REPLACE(input, 'ạ', 'a');
        SET input = REPLACE(input, 'ă', 'a');
        SET input = REPLACE(input, 'ắ', 'a');
        SET input = REPLACE(input, 'ằ', 'a');
        SET input = REPLACE(input, 'ẳ', 'a');
        SET input = REPLACE(input, 'ẵ', 'a');
        SET input = REPLACE(input, 'ặ', 'a');
        SET input = REPLACE(input, 'â', 'a');
        SET input = REPLACE(input, 'ấ', 'a');
        SET input = REPLACE(input, 'ầ', 'a');
        SET input = REPLACE(input, 'ẩ', 'a');
        SET input = REPLACE(input, 'ẫ', 'a');
        SET input = REPLACE(input, 'ậ', 'a');

        SET input = REPLACE(input, 'đ', 'd');

        SET input = REPLACE(input, 'é', 'e');
        SET input = REPLACE(input, 'è', 'e');
        SET input = REPLACE(input, 'ẻ', 'e');
        SET input = REPLACE(input, 'ẽ', 'e');
        SET input = REPLACE(input, 'ẹ', 'e');
        SET input = REPLACE(input, 'ê', 'e');
        SET input = REPLACE(input, 'ế', 'e');
        SET input = REPLACE(input, 'ề', 'e');
        SET input = REPLACE(input, 'ể', 'e');
        SET input = REPLACE(input, 'ễ', 'e');
        SET input = REPLACE(input, 'ệ', 'e');

        SET input = REPLACE(input, 'í', 'i');
        SET input = REPLACE(input, 'ì', 'i');
        SET input = REPLACE(input, 'ỉ', 'i');
        SET input = REPLACE(input, 'ĩ', 'i');
        SET input = REPLACE(input, 'ị', 'i');

        SET input = REPLACE(input, 'ó', 'o');
        SET input = REPLACE(input, 'ò', 'o');
        SET input = REPLACE(input, 'ỏ', 'o');
        SET input = REPLACE(input, 'õ', 'o');
        SET input = REPLACE(input, 'ọ', 'o');
        SET input = REPLACE(input, 'ô', 'o');
        SET input = REPLACE(input, 'ố', 'o');
        SET input = REPLACE(input, 'ồ', 'o');
        SET input = REPLACE(input, 'ổ', 'o');
        SET input = REPLACE(input, 'ỗ', 'o');
        SET input = REPLACE(input, 'ộ', 'o');
        SET input = REPLACE(input, 'ơ', 'o');
        SET input = REPLACE(input, 'ớ', 'o');
        SET input = REPLACE(input, 'ờ', 'o');
        SET input = REPLACE(input, 'ở', 'o');
        SET input = REPLACE(input, 'ỡ', 'o');
        SET input = REPLACE(input, 'ợ', 'o');

        SET input = REPLACE(input, 'ú', 'u');
        SET input = REPLACE(input, 'ù', 'u');
        SET input = REPLACE(input, 'ủ', 'u');
        SET input = REPLACE(input, 'ũ', 'u');
        SET input = REPLACE(input, 'ụ', 'u');
        SET input = REPLACE(input, 'ư', 'u');
        SET input = REPLACE(input, 'ứ', 'u');
        SET input = REPLACE(input, 'ừ', 'u');
        SET input = REPLACE(input, 'ử', 'u');
        SET input = REPLACE(input, 'ữ', 'u');
        SET input = REPLACE(input, 'ự', 'u');

        SET input = REPLACE(input, 'ý', 'y');
        SET input = REPLACE(input, 'ỳ', 'y');
        SET input = REPLACE(input, 'ỷ', 'y');
        SET input = REPLACE(input, 'ỹ', 'y');
        SET input = REPLACE(input, 'ỵ', 'y');

        RETURN input;
      END
    `);
  }
}
