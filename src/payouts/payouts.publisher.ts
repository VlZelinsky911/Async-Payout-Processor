import { Inject, Injectable } from '@nestjs/common';
import { Redis } from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.module';
import { PAYOUT_STREAM } from './payouts.constants';
import { PayoutEvent } from './payouts.schema';

@Injectable()
export class PayoutsPublisher {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  publish(event: PayoutEvent): Promise<string | null> {
    return this.redis.xadd(
      PAYOUT_STREAM,
      '*',
      'userId',
      String(event.userId),
      'offerId',
      event.offerId,
      'basePayout',
      String(event.basePayout),
    );
  }
}
