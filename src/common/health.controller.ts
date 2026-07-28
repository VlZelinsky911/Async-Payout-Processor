import {
  Controller,
  Get,
  Inject,
  ServiceUnavailableException,
} from '@nestjs/common';
import { EntityManager } from '@mikro-orm/core';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module';

@Controller('health')
export class HealthController {
  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly em: EntityManager,
  ) {}

  @Get()
  async check() {
    const [redis, postgres] = await Promise.all([
      this.ping(() => this.redis.ping()),
      this.ping(() => this.em.getConnection().execute('SELECT 1')),
    ]);

    const status = redis && postgres ? 'ok' : 'degraded';
    if (status !== 'ok') {
      throw new ServiceUnavailableException({ status, redis, postgres });
    }
    return { status, redis, postgres };
  }

  private async ping(fn: () => Promise<unknown>): Promise<boolean> {
    try {
      await fn();
      return true;
    } catch {
      return false;
    }
  }
}
