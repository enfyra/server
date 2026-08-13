export type AssetPermissionRow = Record<string, any>;

export type AssetFileRecord = Record<string, any> & {
  permissions?: AssetPermissionRow[];
};

export type LocalFileSignature = {
  extension: string;
  mimetype: string;
} | null;

export type HeicConvert = (options: {
  buffer: Buffer;
  format: 'JPEG' | 'PNG';
  quality?: number;
}) => Promise<ArrayBuffer | Uint8Array>;
