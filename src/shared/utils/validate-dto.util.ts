import { BadRequestException } from '../../core/exceptions/custom-exceptions';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';

export async function validateDto<T extends object>(
  dtoClass: new () => T,
  obj: object,
): Promise<T> {
  const instance = plainToInstance(dtoClass, obj);
  const errors = await validate(instance);
  if (errors.length > 0) {
    const extractMessages = (errs: typeof errors): string[] =>
      errs.flatMap((e) => [
        ...Object.values(e.constraints || {}),
        ...extractMessages(e.children || []),
      ]);
    const constraints = extractMessages(errors).join('; ');
    throw new BadRequestException('Validation failed: ' + constraints);
  }
  return instance;
}
