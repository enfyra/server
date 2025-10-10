import { Controller, Get, Res } from '@nestjs/common';
import { Public } from '../../../shared/decorators/public-route.decorator';
import { SwaggerService } from '../services/swagger.service';
import { Response } from 'express';

@Controller('api-docs')
export class SwaggerController {
  constructor(private readonly swaggerService: SwaggerService) {}

  @Public()
  @Get()
  getSwaggerUI(@Res() res: Response) {
    const spec = this.swaggerService.getCurrentSpec();
    
    // Serve Swagger UI HTML
    const html = this.generateSwaggerHTML(spec);
    res.setHeader('Content-Type', 'text/html');
    res.send(html);
  }

  @Get('json')
  getSwaggerSpec() {
    return this.swaggerService.getCurrentSpec();
  }

  private generateSwaggerHTML(spec: any): string {
    return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Enfyra API Documentation</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
  <script>
    window.onload = function() {
      const spec = ${JSON.stringify(spec)};
      
      window.ui = SwaggerUIBundle({
        spec: spec,
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout",
        persistAuthorization: true
      });
    };
  </script>
</body>
</html>
    `;
  }
}

