import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CreateRequestContext, MikroORM } from '@mikro-orm/core';
import { Redis } from 'ioredis';
import { randomUUID } from 'crypto';
import { REDIS_CLIENT } from '../redis/redis.module';
import { PAYOUT_DLQ, PAYOUT_GROUP, PAYOUT_STREAM } from './payouts.constants';
import { PayoutsService } from './payouts.service';
import { payoutEventSchema } from './payouts.schema';

type StreamMessage = [id: string, fields: string[]];
type StreamReadReply = [stream: string, messages: StreamMessage[]][];

@Injectable()
export class PayoutsConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PayoutsConsumer.name);
  private readonly consumerId = `consumer-${randomUUID()}`;

  private readonly blockMs: number;
  private readonly batchSize: number;
  private readonly maxRetries: number;
  private readonly claimIdleMs: number;

  private blockingRedis: Redis;
  private running = false;
  private errorBackoffMs = 0;

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly service: PayoutsService,
    private readonly orm: MikroORM,
    config: ConfigService,
  ) {
    this.blockMs = Number(config.get('PAYOUT_BLOCK_MS') ?? 5000);
    this.batchSize = Number(config.get('PAYOUT_BATCH_SIZE') ?? 10);
    this.maxRetries = Number(config.get('PAYOUT_MAX_RETRIES') ?? 3);
    this.claimIdleMs = Number(config.get('PAYOUT_CLAIM_IDLE_MS') ?? 30000);
  }

  async onModuleInit() {
    await this.ensureGroup();
    this.blockingRedis = this.redis.duplicate();
    this.running = true;
    void this.loop();
  }

  async onModuleDestroy() {
    this.running = false;
    await this.blockingRedis?.quit();
  }

  private async ensureGroup() {
    try {
      await this.redis.xgroup(
        'CREATE',
        PAYOUT_STREAM,
        PAYOUT_GROUP,
        '$',
        'MKSTREAM',
      );
    } catch (err) {
      if (!String(err).includes('BUSYGROUP')) throw err;
    }
  }

  private async loop() {
    let cursor = '0';
    while (this.running) {
      try {
        await this.claimStale();

        const reply = (await this.blockingRedis.xreadgroup(
          'GROUP',
          PAYOUT_GROUP,
          this.consumerId,
          'COUNT',
          String(this.batchSize),
          'BLOCK',
          String(this.blockMs),
          'STREAMS',
          PAYOUT_STREAM,
          cursor,
        )) as StreamReadReply | null;

        const messages = reply?.[0]?.[1] ?? [];
        if (messages.length === 0) {
          cursor = '>';
          continue;
        }
        for (const [id, fields] of messages) {
          await this.handle(id, fields);
        }
        this.errorBackoffMs = 0;
      } catch (err) {
        if (!this.running) break;
        if (String(err).includes('NOGROUP')) {
          await this.ensureGroup();
          cursor = '0';
          continue;
        }
        await this.backoff(err);
      }
    }
  }

  private async claimStale() {
    const [, claimed] = (await this.redis.xautoclaim(
      PAYOUT_STREAM,
      PAYOUT_GROUP,
      this.consumerId,
      this.claimIdleMs,
      '0',
      'COUNT',
      String(this.batchSize),
    )) as [string, StreamMessage[]];

    for (const [id, fields] of claimed ?? []) {
      await this.handle(id, fields);
    }
  }

  @CreateRequestContext()
  private async handle(id: string, fields: string[]) {
    const event = this.parse(fields);

    if (!event) {
      this.logger.warn(`Invalid event ${id} — moving to DLQ`);
      await this.deadLetter(id, fields, 'invalid-schema');
      return;
    }

    try {
      const result = await this.service.processPayout(id, event);
      await this.redis.xack(PAYOUT_STREAM, PAYOUT_GROUP, id);
      if (result === 'processed') {
        this.logger.log(`Processed event ${id} for user ${event.userId}`);
      }
    } catch (err) {
      await this.handleFailure(id, fields, err);
    }
  }

  private async handleFailure(id: string, fields: string[], err: unknown) {
    const deliveries = await this.deliveryCount(id);
    if (deliveries >= this.maxRetries) {
      this.logger.error(
        `Event ${id} failed ${deliveries}x — moving to DLQ`,
        err,
      );
      await this.deadLetter(id, fields, String(err));
    } else {
      this.logger.warn(
        `Event ${id} failed (attempt ${deliveries}), will retry`,
      );
    }
  }

  private async deadLetter(id: string, fields: string[], reason: string) {
    await this.redis.xadd(PAYOUT_DLQ, '*', 'reason', reason, ...fields);
    await this.redis.xack(PAYOUT_STREAM, PAYOUT_GROUP, id);
  }

  private async deliveryCount(id: string): Promise<number> {
    const pending = (await this.redis.xpending(
      PAYOUT_STREAM,
      PAYOUT_GROUP,
      'IDLE',
      0,
      id,
      id,
      1,
    )) as [string, string, number, number][];
    return pending[0]?.[3] ?? 1;
  }

  private parse(fields: string[]) {
    const raw: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) raw[fields[i]] = fields[i + 1];
    const result = payoutEventSchema.safeParse(raw);
    return result.success ? result.data : null;
  }

  private async backoff(err: unknown) {
    this.logger.error('Consumer loop error', err);
    this.errorBackoffMs = Math.min(
      this.errorBackoffMs === 0 ? 500 : this.errorBackoffMs * 2,
      10000,
    );
    await new Promise((r) => setTimeout(r, this.errorBackoffMs));
  }
}
