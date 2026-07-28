import {
  Entity,
  PrimaryKey,
  Property,
  OneToMany,
  Collection,
} from '@mikro-orm/core';
import { PayoutLedger } from '../payouts/payout-ledger.entity';

@Entity({ tableName: 'users' })
export class User {
  @PrimaryKey()
  id: number;

  @Property()
  name: string;

  @Property({ type: 'decimal', precision: 10, scale: 2, default: 0.0 })
  balance: number;

  @Property({ type: 'decimal', precision: 5, scale: 2, default: 1.0 })
  multiplier: number;

  @OneToMany(() => PayoutLedger, (ledger) => ledger.user)
  payoutLedgers = new Collection<PayoutLedger>(this);
}
