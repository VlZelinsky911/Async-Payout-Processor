import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { EntityManager, MikroORM } from '@mikro-orm/core';
import * as request from 'supertest';
import { Server } from 'http';
import { Redis } from 'ioredis';
import { AppModule } from '../src/app.module';
import { REDIS_CLIENT } from '../src/redis/redis.module';
import { PAYOUT_GROUP, PAYOUT_STREAM } from '../src/payouts/payouts.constants';
import { User } from '../src/users/user.entity';
import { PayoutLedger } from '../src/payouts/payout-ledger.entity';
import { PayoutsService } from '../src/payouts/payouts.service';

describe('Payouts (e2e)', () => {
  let app: INestApplication;
  let em: EntityManager;
  let redis: Redis;
  let userId: number;

  const waitFor = async (fn: () => Promise<boolean>, attempts = 50) => {
    for (let i = 0; i < attempts; i++) {
      if (await fn()) return;
      await new Promise((r) => setTimeout(r, 200));
    }
    throw new Error('condition not met in time');
  };

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleRef.createNestApplication();

    const cleaner = new Redis({
      host: process.env.REDIS_HOST ?? 'localhost',
      port: Number(process.env.REDIS_PORT ?? 6379),
    });
    await cleaner.del(PAYOUT_STREAM);
    await cleaner.quit();

    await app.init();

    redis = app.get(REDIS_CLIENT);
    em = app.get(MikroORM).em.fork();

    const user = em.create(User, {
      name: 'E2E User',
      balance: 0,
      multiplier: 2,
    });
    await em.persistAndFlush(user);
    userId = user.id;
  });

  afterAll(async () => {
    await em.nativeDelete(PayoutLedger, { user: userId });
    await em.nativeDelete(User, { id: userId });
    await redis.xgroup('DESTROY', PAYOUT_STREAM, PAYOUT_GROUP).catch(() => {});
    await app.close();
  });

  it('GET /health reports both dependencies up', async () => {
    const res = await request(app.getHttpServer() as Server)
      .get('/health')
      .expect(200);
    expect(res.body).toMatchObject({
      status: 'ok',
      redis: true,
      postgres: true,
    });
  });

  it('processes a published payout and updates balance + ledger', async () => {
    await request(app.getHttpServer() as Server)
      .post('/payouts')
      .send({ userId, offerId: 'offer-1', basePayout: 10 })
      .expect(202);

    const fork = em.fork();
    await waitFor(async () => {
      const u = await fork.findOne(User, { id: userId });
      return !!u && Number(u.balance) >= 20;
    });

    const user = await fork.findOne(User, { id: userId });
    expect(Number(user!.balance)).toBeCloseTo(20);

    const ledger = await fork.find(PayoutLedger, { user: userId });
    expect(ledger).toHaveLength(1);
    expect(Number(ledger[0].payoutAmount)).toBeCloseTo(20);
  }, 20000);

  it('is idempotent: re-processing the same eventId credits only once', async () => {
    const setup = em.fork();
    const u = await setup.findOne(User, { id: userId });
    u!.balance = 0;
    await setup.flush();
    await setup.nativeDelete(PayoutLedger, { user: userId });

    const service = app.get(PayoutsService);
    const event = { userId, offerId: 'dup', basePayout: 5 };

    const first = await service.processPayout('fixed-event-id', event);
    const second = await service.processPayout('fixed-event-id', event);

    expect(first).toBe('processed');
    expect(second).toBe('duplicate');

    const fork = em.fork();
    const ledger = await fork.find(PayoutLedger, { user: userId });
    expect(ledger).toHaveLength(1);
    const user = await fork.findOne(User, { id: userId });
    expect(Number(user!.balance)).toBeCloseTo(10);
  }, 20000);
});
