import {
  Entity,
  PrimaryKey,
  Property,
  ManyToOne,
  Rel,
  Unique,
} from '@mikro-orm/core';
import { User } from '../users/user.entity';

@Entity({ tableName: 'payout_ledgers' })
export class PayoutLedger {
  @PrimaryKey()
  id: number;

  @ManyToOne(() => User)
  user: Rel<User>;

  @Property({ fieldName: 'event_id' })
  @Unique()
  eventId: string;

  @Property({ fieldName: 'offer_id' })
  offerId: string;

  @Property({
    fieldName: 'payout_amount',
    type: 'decimal',
    precision: 10,
    scale: 2,
  })
  payoutAmount: number;

  @Property({ fieldName: 'created_at', defaultRaw: 'CURRENT_TIMESTAMP' })
  createdAt: Date = new Date();
}
