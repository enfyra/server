import {
  Controller,
  Post,
  Get,
  Patch,
  Delete,
  Param,
  Req,
} from '@nestjs/common';
import { FileManagementService } from '../services/file-management.service';
import { RequestWithRouteData } from '../../../shared/interfaces/dynamic-context.interface';
import {
  ValidationException,
  FileUploadException,
  FileNotFoundException,
} from '../../../core/exceptions/custom-exceptions';

@Controller('file_definition')
export class FileController {
  constructor(private fileManagementService: FileManagementService) {}

  @Post()
  async uploadFile(@Req() req: RequestWithRouteData) {
    const file = req.file;
    if (!file) {
      throw new FileUploadException('No file provided');
    }

    const body = req.routeData?.context?.$body || {};

    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      throw new ValidationException('Repository not found in context');
    }

    return await this.fileManagementService.uploadFileAndCreateRecord(
      {
        filename: file.originalname,
        mimetype: file.mimetype,
        buffer: file.buffer,
        size: file.size,
      },
      {
        folder: body.folder,
        storageConfig: body.storageConfig,
        title: body.title || file.originalname,
        description: body.description || null,
        userId: req.user?.id,
      },
      fileRepo,
    );
  }

  @Get()
  async getFiles(@Req() req: RequestWithRouteData) {
    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      throw new ValidationException('Repository not found in context');
    }

    const result = await fileRepo.find();
    return result;
  }

  @Patch(':id')
  async updateFile(@Param('id') id: string, @Req() req: RequestWithRouteData) {
    const body = req.routeData?.context?.$body || {};
    const file = req.file;

    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      throw new ValidationException('Repository not found in context');
    }

    const currentFiles = await fileRepo.find({ where: { id: { _eq: id } } });
    const currentFile = currentFiles.data?.[0];

    if (!currentFile) {
      throw new FileNotFoundException(`File with ID ${id} not found`);
    }

    if (file) {
      try {
        let storageConfigId = currentFile.storageConfig?.id || null;
        if (body.storageConfig) {
          storageConfigId = typeof body.storageConfig === 'object'
            ? body.storageConfig.id
            : body.storageConfig;
        }

        let storageConfig = null;
        if (storageConfigId) {
          storageConfig = await this.fileManagementService.getStorageConfigById(storageConfigId);
        }

        if (storageConfig && (storageConfig.type === 'Google Cloud Storage' || storageConfig.type === 'Cloudflare R2')) {
          await this.fileManagementService.replaceFileOnStorage(
            currentFile.location,
            file.buffer,
            file.mimetype,
            storageConfigId,
          );

          const updateData = {
            filename: file.originalname,
            mimetype: file.mimetype,
            filesize: file.size,
            storageConfig: this.fileManagementService.createIdReference(storageConfigId),
            description: body.description || currentFile.description,
            folder: currentFile.folder,
            uploaded_by: currentFile.uploaded_by,
            status: currentFile.status,
          };

          return await fileRepo.update(id, updateData);
        }

        const processedFile =
          await this.fileManagementService.processFileUpload(
            {
              filename: file.originalname,
              mimetype: file.mimetype,
              buffer: file.buffer,
              size: file.size,
              folder: currentFile.folder,
              title: body.title || file.originalname,
              description: body.description || currentFile.description,
            },
            storageConfigId,
          );

        const backupPath = await this.fileManagementService.backupFile(
          currentFile.location,
        );

        try {
          await this.fileManagementService.replacePhysicalFile(
            currentFile.location,
            processedFile.location,
          );

          const updateData = {
            filename: processedFile.filename,
            mimetype: processedFile.mimetype,
            type: processedFile.type,
            filesize: processedFile.filesize,
            location: currentFile.location,
            description: processedFile.description,
            folder: currentFile.folder,
            uploaded_by: currentFile.uploaded_by,
            status: currentFile.status,
            storageConfig: processedFile.storage_config_id
              ? this.fileManagementService.createIdReference(processedFile.storage_config_id)
              : null,
          };

          const result = await fileRepo.update(id, updateData);

          await this.fileManagementService.rollbackFileCreation(
            processedFile.location,
            processedFile.storage_config_id,
          );
          await this.fileManagementService.deleteBackupFile(backupPath);

          return result;
        } catch (error) {
          await this.fileManagementService.restoreFromBackup(
            currentFile.location,
            backupPath,
          );
          throw error;
        }
      } catch (error) {
        throw error;
      }
    }

    if (body.folder && body.folder !== currentFile.folder) {
      const newFolder =
        typeof body.folder === 'object' ? body.folder : { id: body.folder };
      body.folder = newFolder;
    }

    try {
      const result = await fileRepo.update(id, body);
      return result;
    } catch (error) {
      throw error;
    }
  }

  @Delete(':id')
  async deleteFile(@Param('id') id: string, @Req() req: RequestWithRouteData) {
    const fileRepo =
      req.routeData?.context?.$repos?.main ||
      req.routeData?.context?.$repos?.file_definition;

    if (!fileRepo) {
      throw new ValidationException('Repository not found in context');
    }

    const files = await fileRepo.find({ where: { id: { _eq: id } } });
    const file = files.data?.[0];

    if (!file) {
      throw new FileNotFoundException(`File with ID ${id} not found`);
    }

    const { location, storageConfig } = file;

    await this.fileManagementService.deletePhysicalFile(
      location,
      storageConfig?.id || null,
    );

    const result = await fileRepo.delete(id);
    return result;
  }
}
