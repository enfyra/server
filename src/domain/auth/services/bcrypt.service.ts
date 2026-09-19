import * as bcrypt from 'bcryptjs';

const BCRYPT_PASSWORD_LIMIT = 72;

export class BcryptService {
  async hash(password: string, saltRounds = 10): Promise<string> {
    return bcrypt.hash(this.normalize(password), saltRounds);
  }

  async compare(password: string, hash: string): Promise<boolean> {
    return bcrypt.compare(this.normalize(password), hash);
  }

  private normalize(password: string): string {
    if (password.length <= BCRYPT_PASSWORD_LIMIT) return password;
    return password.slice(0, BCRYPT_PASSWORD_LIMIT);
  }
}
