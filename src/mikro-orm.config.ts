import { Options } from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { PayoutLedger } from './payouts/payout-ledger.entity';
import { User } from './users/user.entity';
import * as dotenv from 'dotenv';

dotenv.config();

const config: Options = {
  entities: [User, PayoutLedger],
  dbName: process.env.POSTGRES_DB ?? 'postgres_db',
  driver: PostgreSqlDriver,
  host: process.env.POSTGRES_HOST ?? 'postgres',
  port: parseInt(process.env.POSTGRES_PORT ?? '5432', 10),
  user: process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_PASSWORD ?? 'postgres',
  debug: process.env.NODE_ENV !== 'production',
  discovery: {
    warnWhenNoEntities: false,
  },
};

export default config;
