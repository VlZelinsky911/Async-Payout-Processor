import { Module } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { RedisModule } from '../redis/redis.module';
import { User } from '../users/user.entity';
import { PayoutLedger } from './payout-ledger.entity';
import { PayoutsController } from './payouts.controller';
import { PayoutsPublisher } from './payouts.publisher';
import { PayoutsService } from './payouts.service';
import { PayoutsConsumer } from './payouts.consumer';

@Module({
  imports: [RedisModule, MikroOrmModule.forFeature([User, PayoutLedger])],
  controllers: [PayoutsController],
  providers: [PayoutsPublisher, PayoutsService, PayoutsConsumer],
})
export class PayoutsModule {}
