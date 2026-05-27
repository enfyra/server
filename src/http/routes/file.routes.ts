import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

export function registerFileRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.post('/file_definition', async (req: any, res: Response) => {
    const fileManagementService =
      req.scope?.cradle?.fileManagementService ??
      container.cradle.fileManagementService;
    const file = req.file;

    if (!file) {
      const { FileUploadException } = await import('../../domain/exceptions');
      throw new FileUploadException('No file provided');
    }

    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      const { ValidationException } = await import('../../domain/exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const body = req.body;
    const result = await fileManagementService.uploadFileAndCreateRecord(
      {
        filename: file.originalname,
        mimetype: file.mimetype,
        buffer: file.buffer,
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
  });

  app.get('/file_definition', async (req: any, res: Response) => {
    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      const { ValidationException } = await import('../../domain/exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const result = await fileRepo.find();
    return res.json(result);
  });

  app.patch('/file_definition/:id', async (req: any, res: Response) => {
    const fileManagementService =
      req.scope?.cradle?.fileManagementService ??
      container.cradle.fileManagementService;
    const file = req.file;
    const id = req.params.id;
    const body = req.body;

    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.file_definition;

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
      const result = await fileManagementService.replaceFileAndUpdateRecord(
        fileRepo,
        id,
        currentFile,
        {
          filename: file.originalname,
          mimetype: file.mimetype,
          buffer: file.buffer,
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

  app.delete('/file_definition/:id', async (req: any, res: Response) => {
    const fileManagementService =
      req.scope?.cradle?.fileManagementService ??
      container.cradle.fileManagementService;
    const id = req.params.id;

    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.file_definition;

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
