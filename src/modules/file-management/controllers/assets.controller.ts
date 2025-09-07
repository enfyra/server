import { Controller, Get, Req, Res, Logger } from '@nestjs/common';
import { Response } from 'express';
import { FileAssetsService } from '../services/file-assets.service';
import { Public } from '../../../shared/decorators/public-route.decorator';
import { RequestWithRouteData } from '../../../shared/interfaces/dynamic-context.interface';

@Controller('assets')
export class AssetsController {
  private readonly logger = new Logger(AssetsController.name);

  constructor(private readonly fileAssetsService: FileAssetsService) {}

  @Public()
  @Get(':id')
  async getAsset(
    @Req() req: RequestWithRouteData,
    @Res() res: Response,
  ): Promise<void> {
    try {
      return await this.fileAssetsService.streamFile(req, res);
    } catch (error) {
      this.logger.error('Failed to get asset:', error);
      throw error;
    }
  }
}
