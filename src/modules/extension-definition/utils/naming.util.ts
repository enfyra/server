import { randomUUID } from 'crypto';

const EXTENSION_UUID_PATTERN =
  /^extension_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function autoAssignExtensionName(body: any): any {
  const currentExtensionId = body.extensionId || '';

  if (!currentExtensionId || !EXTENSION_UUID_PATTERN.test(currentExtensionId)) {
    const uuid = randomUUID();
    body.extensionId = `extension_${uuid}`;
  }

  return body;
}

export function isValidExtensionId(extensionId: string): boolean {
  return EXTENSION_UUID_PATTERN.test(extensionId);
}
