export function generateTitleFromMessage(message: string): string {
  const maxLength = 100;
  const trimmed = message.trim();
  if (trimmed.length <= maxLength) {
    return trimmed;
  }
  return trimmed.substring(0, maxLength - 3) + '...';
}

