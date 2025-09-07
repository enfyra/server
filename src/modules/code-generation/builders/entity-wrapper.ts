import { SourceFile } from 'ts-morph';
import { ValidationException } from '../../../core/exceptions/custom-exceptions';

export function wrapEntityClass({
  sourceFile,
  className,
  tableName,
  uniques = [],
  indexes = [],
  usedImports,
  validEntityFields = [],
  actualEntityFields,
  relations = [], // Thêm parameter relations
}: {
  sourceFile: SourceFile;
  className: string;
  tableName: string;
  uniques?: Array<{ value: string[] }>;
  indexes?: Array<{ value: string[] }>;
  usedImports: Set<string>;
  validEntityFields?: string[];
  actualEntityFields?: Set<string>; // Optional: actual fields that will be in the entity
  relations?: Array<any>; // Thêm type cho relations
}) {
  const decorators: { name: string; arguments: string[] }[] = [];

  // @Entity('table_name')
  decorators.push({ name: 'Entity', arguments: [`'${tableName}'`] });
  usedImports.add('Entity');

  // Create set of all valid entity fields (including system fields)
  const allValidFields = new Set([
    ...validEntityFields,
    'id',
    'createdAt',
    'updatedAt', // Always include system fields
  ]);

  // Create sets to track what we've already added
  const addedUniqueKeys = new Set<string>();
  const addedIndexKeys = new Set<string>();

  // First pass: collect all unique constraint keys to prevent index duplicates
  const allUniqueKeys = new Set<string>();
  for (const unique of uniques || []) {
    if (
      !unique ||
      !unique.value ||
      !Array.isArray(unique.value) ||
      unique.value.length === 0
    )
      continue;
    const validFields = unique.value
      .filter((f) => f && typeof f === 'string' && f.trim().length > 0)
      .map((f) => f.trim());
    if (validFields.length === 0) continue;
    const fields = validFields.slice().sort();
    const allFieldsExist = fields.every((field) => allValidFields.has(field));
    if (!allFieldsExist) {
      const missingFields = fields.filter(
        (field) => !allValidFields.has(field),
      );
      throw new ValidationException(
        `@Unique constraint [${fields.join(', ')}] contains non-existent fields: [${missingFields.join(', ')}]`,
        {
          constraint: 'unique',
          fields,
          missingFields,
          availableFields: Array.from(allValidFields),
        },
      );
    }

    // Optional: Additional validation against actual entity fields
    if (actualEntityFields) {
      const allFieldsActuallyExist = fields.every((field) =>
        actualEntityFields.has(field),
      );
      if (!allFieldsActuallyExist) {
        const missingActualFields = fields.filter(
          (field) => !actualEntityFields.has(field),
        );
        throw new ValidationException(
          `@Unique constraint [${fields.join(', ')}] contains fields not found in actual entity: [${missingActualFields.join(', ')}]`,
          {
            constraint: 'unique',
            fields,
            missingFields: missingActualFields,
            actualEntityFields: Array.from(actualEntityFields),
          },
        );
      }
    }

    allUniqueKeys.add(fields.join('|'));
  }

  for (const unique of uniques || []) {
    // Handle null/undefined unique.value
    if (
      !unique ||
      !unique.value ||
      !Array.isArray(unique.value) ||
      unique.value.length === 0
    ) {
      console.warn(
        `Skipping invalid @Unique constraint - value is not a valid array:`,
        unique,
      );
      continue;
    }

    // Filter out invalid fields and trim whitespace BEFORE sorting
    const validFields = unique.value
      .filter((f) => f && typeof f === 'string' && f.trim().length > 0)
      .map((f) => f.trim());
    if (validFields.length === 0) {
      console.warn(
        `Skipping @Unique constraint - no valid fields found:`,
        unique.value,
      );
      continue;
    }

    const fields = validFields.slice().sort(); // Work with trimmed valid fields only

    // Validate that ALL fields exist in the entity
    const allFieldsExist = fields.every((field) => allValidFields.has(field));
    if (!allFieldsExist) {
      const missingFields = fields.filter(
        (field) => !allValidFields.has(field),
      );
      throw new ValidationException(
        `@Unique constraint [${fields.join(', ')}] contains non-existent fields: [${missingFields.join(', ')}]`,
        {
          constraint: 'unique',
          fields,
          missingFields,
          availableFields: Array.from(allValidFields),
        },
      );
    }

    // Optional: Additional validation against actual entity fields
    if (actualEntityFields) {
      const allFieldsActuallyExist = fields.every((field) =>
        actualEntityFields.has(field),
      );
      if (!allFieldsActuallyExist) {
        const missingActualFields = fields.filter(
          (field) => !actualEntityFields.has(field),
        );
        throw new ValidationException(
          `@Unique constraint [${fields.join(', ')}] contains fields not found in actual entity: [${missingActualFields.join(', ')}]`,
          {
            constraint: 'unique',
            fields,
            missingFields: missingActualFields,
            actualEntityFields: Array.from(actualEntityFields),
          },
        );
      }
    }

    const key = fields.join('|');

    // Only check for duplicates - no field-level conflicts
    if (!addedUniqueKeys.has(key)) {
      decorators.push({
        name: 'Unique',
        arguments: [`[${fields.map((f) => `'${f}'`).join(', ')}]`],
      });
      usedImports.add('Unique');
      addedUniqueKeys.add(key);
      // Unique constraints also act as indexes
      addedIndexKeys.add(key);
    }
  }

  for (const index of indexes || []) {
    // Handle null/undefined index.value
    if (
      !index ||
      !index.value ||
      !Array.isArray(index.value) ||
      index.value.length === 0
    ) {
      console.warn(
        `Skipping invalid @Index constraint - value is not a valid array:`,
        index,
      );
      continue;
    }

    // Filter out invalid fields and trim whitespace BEFORE sorting
    const validFields = index.value
      .filter((f) => f && typeof f === 'string' && f.trim().length > 0)
      .map((f) => f.trim());
    if (validFields.length === 0) {
      console.warn(
        `Skipping @Index constraint - no valid fields found:`,
        index.value,
      );
      continue;
    }

    const fields = validFields.slice().sort(); // Work with trimmed valid fields only

    // Validate that ALL fields exist in the entity
    const allFieldsExist = fields.every((field) => allValidFields.has(field));
    if (!allFieldsExist) {
      const missingFields = fields.filter(
        (field) => !allValidFields.has(field),
      );
      throw new ValidationException(
        `@Index constraint [${fields.join(', ')}] contains non-existent fields: [${missingFields.join(', ')}]`,
        {
          constraint: 'index',
          fields,
          missingFields,
          availableFields: Array.from(allValidFields),
        },
      );
    }

    // Optional: Additional validation against actual entity fields
    if (actualEntityFields) {
      const allFieldsActuallyExist = fields.every((field) =>
        actualEntityFields.has(field),
      );
      if (!allFieldsActuallyExist) {
        const missingActualFields = fields.filter(
          (field) => !actualEntityFields.has(field),
        );
        throw new ValidationException(
          `@Index constraint [${fields.join(', ')}] contains fields not found in actual entity: [${missingActualFields.join(', ')}]`,
          {
            constraint: 'index',
            fields,
            missingFields: missingActualFields,
            actualEntityFields: Array.from(actualEntityFields),
          },
        );
      }
    }

    const key = fields.join('|');

    // Skip if duplicate index or if unique constraint exists for same fields
    const isBlockedByUnique = allUniqueKeys.has(key);

    if (!addedIndexKeys.has(key) && !isBlockedByUnique) {
      decorators.push({
        name: 'Index',
        arguments: [`[${fields.map((f) => `'${f}'`).join(', ')}]`],
      });
      usedImports.add('Index');
      addedIndexKeys.add(key);
    }
  }

  // Tự động thêm class-level @Index cho foreign key fields (many-to-one relations)
  for (const relation of relations || []) {
    if (relation.type === 'many-to-one' && relation.propertyName) {
      const fieldName = relation.propertyName;

      // Kiểm tra field có tồn tại không
      if (allValidFields.has(fieldName)) {
        const key = fieldName;

        // Check xem field có bị block bởi unique constraint đơn lẻ không
        // (Unique cụm vẫn cho phép thêm index riêng lẻ)
        const isBlockedBySingleUnique = allUniqueKeys.has(key);

        // Chỉ thêm nếu:
        // 1. Chưa có index cho field này
        // 2. Field không bị block bởi unique constraint đơn lẻ
        // 3. Field không bị block bởi unique constraint đơn lẻ
        if (!addedIndexKeys.has(key) && !isBlockedBySingleUnique) {
          decorators.push({
            name: 'Index',
            arguments: [`['${fieldName}']`],
          });
          usedImports.add('Index');
          addedIndexKeys.add(key);
        }
      }
    }
  }

  return sourceFile.addClass({
    name: className,
    isExported: true,
    decorators,
  });
}
