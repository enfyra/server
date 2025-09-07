import { BadRequestException } from '@nestjs/common';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';

export async function validateDto<T extends object>(
  dtoClass: new () => T,
  obj: object,
): Promise<T> {
  const instance = plainToInstance(dtoClass, obj);
  const errors = await validate(instance);
  if (errors.length > 0) {
    const constraints = errors
      .flatMap((e) => Object.values(e.constraints || {}))
      .join('; ');
    throw new BadRequestException('Validation failed: ' + constraints);
  }
  return instance;
}
