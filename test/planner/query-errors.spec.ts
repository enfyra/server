import { BadRequestException } from '@enfyra/kernel';

describe('Kernel query errors', () => {
  it('constructs a stable message for unserializable details', () => {
    const circular: Record<string, unknown> = { value: 1n };
    circular.self = circular;

    const error = new BadRequestException(circular);

    expect(error.message).toBe('Bad Request');
    expect(error.statusCode).toBe(400);
  });

  it('defensively copies array messages', () => {
    const messages = ['first'];
    const error = new BadRequestException(messages);

    messages.push('second');

    expect(error.message).toBe('first');
    expect(error.messages).toEqual(['first']);
  });
});
