import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';
import { AllExceptionsFilter } from './common/http-exception.filter';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalFilters(new AllExceptionsFilter());
  app.enableShutdownHooks();

  if (process.env.WORKER_ONLY === 'true') {
    await app.init();
    Logger.log('Started in worker-only mode (no HTTP listener)', 'Bootstrap');
    return;
  }

  const port = process.env.PORT ?? 3000;
  await app.listen(port);
  Logger.log(`Application listening on port ${port}`, 'Bootstrap');
}
void bootstrap();
