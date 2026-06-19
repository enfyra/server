import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import fs from 'fs/promises';
import { createReadStream } from 'fs';
import os from 'os';
import path from 'path';
import { FileUploadException } from '../../domain/exceptions';

export function resolveUploadedTempFilePath(file: any): string {
  if (!file?.path) {
    throw new FileUploadException('No uploaded temp file path provided');
  }

  const tmpDir = path.resolve(os.tmpdir());
  const resolvedPath = path.resolve(file.path);
  const basename = path.basename(resolvedPath);

  if (
    !resolvedPath.startsWith(`${tmpDir}${path.sep}`) ||
    !basename.startsWith('enfyra-upload-')
  ) {
    throw new FileUploadException('Invalid uploaded temp file path');
  }

  return resolvedPath;
}

async function cleanupUploadedTempFile(file: any) {
  if (!file?.path) return;
  try {
    await fs.unlink(resolveUploadedTempFilePath(file));
  } catch {}
}

export function registerFileRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  // codeql[js/missing-rate-limiting] Built-in routes use metadata guards for admin-configured rate limit policies.
  app.post('/enfyra_file', async (req: any, res: Response) => {
    const fileManagementService =
      req.scope?.cradle?.fileManagementService ??
      container.cradle.fileManagementService;
    const file = req.file;

    if (!file) {
      throw new FileUploadException('No file provided');
    }

    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.enfyra_file;

    if (!fileRepo) {
      const { ValidationException } = await import('../../domain/exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const body = req.body;
    try {
      const result = await fileManagementService.uploadFileAndCreateRecord(
        {
          filename: file.originalname,
          mimetype: file.mimetype,
          stream: createReadStream(resolveUploadedTempFilePath(file)),
          size: file.size,
        },
        {
          folder: body.folder,
          storageConfig: body.storageConfig,
          title: file.originalname,
          description: null,
          userId: req.user?.id,
        },
        fileRepo,
      );
      return res.json(result);
    } finally {
      await cleanupUploadedTempFile(file);
    }
  });

  app.get('/enfyra_file', async (req: any, res: Response) => {
    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.enfyra_file;

    if (!fileRepo) {
      const { ValidationException } = await import('../../domain/exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const result = await fileRepo.find();
    return res.json(result);
  });

  // codeql[js/missing-rate-limiting] Built-in routes use metadata guards for admin-configured rate limit policies.
  app.patch('/enfyra_file/:id', async (req: any, res: Response) => {
    const fileManagementService =
      req.scope?.cradle?.fileManagementService ??
      container.cradle.fileManagementService;
    const file = req.file;
    const id = req.params.id;
    const body = req.body;

    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.enfyra_file;

    if (!fileRepo) {
      const { ValidationException } = await import('../../domain/exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const currentFiles = await fileRepo.find({ filter: { id: { _eq: id } } });
    const currentFile = currentFiles.data?.[0];

    if (!currentFile) {
      const { FileNotFoundException } = await import('../../domain/exceptions');
      throw new FileNotFoundException(`File with ID ${id} not found`);
    }

    if (file) {
      try {
        const result = await fileManagementService.replaceFileAndUpdateRecord(
          fileRepo,
          id,
          currentFile,
          {
            filename: file.originalname,
            mimetype: file.mimetype,
            stream: createReadStream(resolveUploadedTempFilePath(file)),
            size: file.size,
          },
          {
            folder: body.folder,
            storageConfig: body.storageConfig,
            title: file.originalname,
            description: body.description,
            status: body.status,
            isPublished: body.isPublished,
          },
        );
        return res.json(result);
      } finally {
        await cleanupUploadedTempFile(file);
      }
    }

    const result = await fileManagementService.updateFileMetadataRecord(
      fileRepo,
      id,
      currentFile,
      {
        folder: body.folder,
        storageConfig: body.storageConfig,
        description: body.description,
        status: body.status,
        isPublished: body.isPublished,
      },
    );
    return res.json(result);
  });

  app.delete('/enfyra_file/:id', async (req: any, res: Response) => {
    const fileManagementService =
      req.scope?.cradle?.fileManagementService ??
      container.cradle.fileManagementService;
    const id = req.params.id;

    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.enfyra_file;

    if (!fileRepo) {
      const { ValidationException } = await import('../../domain/exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const files = await fileRepo.find({ filter: { id: { _eq: id } } });
    const file = files.data?.[0];

    if (!file) {
      const { FileNotFoundException } = await import('../../domain/exceptions');
      throw new FileNotFoundException(`File with ID ${id} not found`);
    }

    const result = await fileManagementService.deleteFileAndRecord(
      fileRepo,
      id,
      file,
    );
    return res.json(result);
  });
}
