import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { RedisModule } from './redis/redis.module';
import mikroOrmConfig from './mikro-orm.config';
import { PayoutsModule } from './payouts/payouts.module';
import { CommonModule } from './common/common.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    MikroOrmModule.forRoot(mikroOrmConfig),
    RedisModule,
    CommonModule,
    PayoutsModule,
  ],
})
export class AppModule {}
