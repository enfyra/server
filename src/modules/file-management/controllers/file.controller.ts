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

    let folderData = null;
    if (body.folder) {
      folderData =
        typeof body.folder === 'object' ? body.folder : { id: body.folder };
    }

    const processedFile = await this.fileManagementService.processFileUpload({
      filename: file.originalname,
      mimetype: file.mimetype,
      buffer: file.buffer,
      size: file.size,
      folder: folderData,
      title: body.title || file.originalname,
      description: body.description || null,
    });

    try {
      const fileRepo =
        req.routeData?.context?.$repos?.main ||
        req.routeData?.context?.$repos?.file_definition;

      if (!fileRepo) {
        throw new ValidationException('Repository not found in context');
      }

      const savedFile = await fileRepo.create({
        ...processedFile,
        folder: folderData,
        uploaded_by: req.user?.id ? { id: req.user.id } : null,
      });

      return savedFile;
    } catch (error) {
      await this.fileManagementService.rollbackFileCreation(
        processedFile.location,
      );
      throw error;
    }
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
    const file = req.file; // File mới nếu có

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

    // Nếu có file mới → REPLACE FILE
    if (file) {
      try {
        // 1. Process file mới (generate metadata mới)
        const processedFile =
          await this.fileManagementService.processFileUpload({
            filename: file.originalname,
            mimetype: file.mimetype,
            buffer: file.buffer,
            size: file.size,
            folder: currentFile.folder, // Giữ nguyên folder
            title: body.title || file.originalname,
            description: body.description || currentFile.description,
          });

        // 2. Backup file cũ (để rollback nếu cần)
        const backupPath = await this.fileManagementService.backupFile(
          currentFile.location,
        );

        try {
          // 3. Replace physical file
          await this.fileManagementService.replacePhysicalFile(
            currentFile.location,
            processedFile.location,
          );

          // 4. Update metadata vào database (id cũ)
          const updateData = {
            filename: processedFile.filename,
            mimetype: processedFile.mimetype,
            type: processedFile.type,
            filesize: processedFile.filesize,
            location: currentFile.location, // Giữ nguyên location cũ
            description: processedFile.description,
            // Giữ nguyên các field khác
            folder: currentFile.folder,
            uploaded_by: currentFile.uploaded_by,
            status: currentFile.status,
          };

          const result = await fileRepo.update(id, updateData);

          // 5. Cleanup temporary file mới và backup file cũ (vì đã thành công)
          await this.fileManagementService.rollbackFileCreation(
            processedFile.location,
          );
          await this.fileManagementService.deleteBackupFile(backupPath);

          return result;
        } catch (error) {
          // Nếu replace thất bại → restore từ backup
          await this.fileManagementService.restoreFromBackup(
            currentFile.location,
            backupPath,
          );
          throw error;
        }
      } catch (error) {
        // Rollback nếu có lỗi
        throw error;
      }
    }

    // Nếu không có file mới → UPDATE METADATA ONLY (logic cũ)
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

    const filePath = file.location;

    const result = await fileRepo.delete(id);

    try {
      await this.fileManagementService.deletePhysicalFile(filePath);
    } catch (error) {
      console.error(`Failed to delete physical file ${filePath}:`, error);
    }

    return result;
  }
}
