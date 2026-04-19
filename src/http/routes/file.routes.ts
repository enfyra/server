import type { Express, Request, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

export function registerFileRoutes(app: Express, container: AwilixContainer<Cradle>) {
  app.post('/file_definition', async (req: any, res: Response) => {
    const fileManagementService = req.scope?.cradle?.fileManagementService ?? container.cradle.fileManagementService;
    const file = req.file;

    if (!file) {
      const { FileUploadException } = await import('../../core/exceptions/custom-exceptions');
      throw new FileUploadException('No file provided');
    }

    const fileRepo = req.routeData?.context?.$repos?.main || req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      const { ValidationException } = await import('../../core/exceptions/custom-exceptions');
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
    res.json(result);
  });

  app.get('/file_definition', async (req: any, res: Response) => {
    const fileRepo = req.routeData?.context?.$repos?.main || req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      const { ValidationException } = await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const result = await fileRepo.find();
    res.json(result);
  });

  app.patch('/file_definition/:id', async (req: any, res: Response) => {
    const fileManagementService = req.scope?.cradle?.fileManagementService ?? container.cradle.fileManagementService;
    const file = req.file;
    const id = req.params.id;
    const body = req.body;

    const fileRepo = req.routeData?.context?.$repos?.main || req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      const { ValidationException } = await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const currentFiles = await fileRepo.find({ where: { id: { _eq: id } } });
    const currentFile = currentFiles.data?.[0];

    if (!currentFile) {
      const { FileNotFoundException } = await import('../../core/exceptions/custom-exceptions');
      throw new FileNotFoundException(`File with ID ${id} not found`);
    }

    if (file) {
      let storageConfigId = currentFile.storageConfig?.id || null;
      if (body.storageConfig !== undefined && body.storageConfig !== null) {
        storageConfigId = typeof body.storageConfig === 'object' ? (body.storageConfig as any).id : body.storageConfig;
      }

      let storageConfig = null;
      if (storageConfigId) {
        storageConfig = await fileManagementService.getStorageConfigById(storageConfigId);
      }

      if (
        storageConfig &&
        (storageConfig.type === 'Google Cloud Storage' ||
          storageConfig.type === 'Cloudflare R2' ||
          storageConfig.type === 'Amazon S3')
      ) {
        await fileManagementService.replaceFileOnStorage(currentFile.location, file.buffer, file.mimetype, storageConfigId);

        const nextDescription = body.description !== undefined ? body.description : currentFile.description;
        const nextFolder = body.folder ? (typeof body.folder === 'object' ? body.folder : { id: body.folder }) : currentFile.folder;
        const nextStatus = body.status !== undefined ? body.status : currentFile.status;
        const nextIsPublished = body.isPublished !== undefined ? body.isPublished : currentFile.isPublished;

        const updateData = {
          filename: file.originalname,
          mimetype: file.mimetype,
          filesize: file.size,
          storageConfig: fileManagementService.createIdReference(storageConfigId),
          description: nextDescription,
          folder: nextFolder,
          uploadedBy: currentFile.uploadedBy,
          status: nextStatus,
          isPublished: nextIsPublished,
        };

        const result = await fileRepo.update({ id, data: updateData });
        return res.json(result);
      }

      const nextDescription = body.description !== undefined ? body.description : currentFile.description;
      const nextFolder = body.folder ? (typeof body.folder === 'object' ? body.folder : { id: body.folder }) : currentFile.folder;
      const nextStatus = body.status !== undefined ? body.status : currentFile.status;
      const nextIsPublished = body.isPublished !== undefined ? body.isPublished : currentFile.isPublished;

      const processedFile = await fileManagementService.processFileUpload(
        {
          filename: file.originalname,
          mimetype: file.mimetype,
          buffer: file.buffer,
          size: file.size,
          folder: nextFolder,
          title: file.originalname,
          description: nextDescription,
        },
        storageConfigId,
      );

      const backupPath = await fileManagementService.backupFile(currentFile.location);

      try {
        await fileManagementService.replacePhysicalFile(currentFile.location, processedFile.location);

        const updateData = {
          filename: processedFile.filename,
          mimetype: processedFile.mimetype,
          type: processedFile.type,
          filesize: processedFile.filesize,
          location: currentFile.location,
          description: nextDescription,
          folder: nextFolder,
          uploadedBy: currentFile.uploadedBy,
          status: nextStatus,
          isPublished: nextIsPublished,
          storageConfig: processedFile.storage_config_id ? fileManagementService.createIdReference(processedFile.storage_config_id) : null,
        };

        const result = await fileRepo.update({ id, data: updateData });

        await fileManagementService.rollbackFileCreation(processedFile.location, processedFile.storage_config_id);
        await fileManagementService.deleteBackupFile(backupPath);

        return res.json(result);
      } catch (error) {
        await fileManagementService.restoreFromBackup(currentFile.location, backupPath);
        throw error;
      }
    }

    const updateData: any = {};

    if (body.folder) {
      updateData.folder = typeof body.folder === 'object' ? body.folder : { id: body.folder };
    }

    if (body.storageConfig !== undefined && body.storageConfig !== null) {
      const storageConfigId = typeof body.storageConfig === 'object' ? body.storageConfig.id : body.storageConfig;
      if (storageConfigId) {
        updateData.storageConfig = fileManagementService.createIdReference(storageConfigId);
      }
    }

    if (body.isPublished !== undefined) {
      updateData.isPublished = body.isPublished;
    }

    if (body.description !== undefined) {
      updateData.description = body.description;
    }

    if (body.status !== undefined) {
      updateData.status = body.status;
    }

    if (Object.keys(updateData).length === 0) {
      return res.json(currentFile);
    }

    const result = await fileRepo.update({ id, data: updateData });
    res.json(result);
  });

  app.delete('/file_definition/:id', async (req: any, res: Response) => {
    const fileManagementService = req.scope?.cradle?.fileManagementService ?? container.cradle.fileManagementService;
    const id = req.params.id;

    const fileRepo = req.routeData?.context?.$repos?.main || req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      const { ValidationException } = await import('../../core/exceptions/custom-exceptions');
      throw new ValidationException('Repository not found in context');
    }

    const files = await fileRepo.find({ where: { id: { _eq: id } } });
    const file = files.data?.[0];

    if (!file) {
      const { FileNotFoundException } = await import('../../core/exceptions/custom-exceptions');
      throw new FileNotFoundException(`File with ID ${id} not found`);
    }

    const { location, storageConfig } = file;

    await fileManagementService.deletePhysicalFile(location, storageConfig?.id || null);

    const result = await fileRepo.delete({ id });
    res.json(result);
  });
}
