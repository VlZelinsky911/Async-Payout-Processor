import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import {
  EntityManager,
  LockMode,
  UniqueConstraintViolationException,
} from '@mikro-orm/core';
import { User } from '../users/user.entity';
import { PayoutLedger } from './payout-ledger.entity';
import { PayoutEvent } from './payouts.schema';

export type ProcessResult = 'processed' | 'duplicate';

@Injectable()
export class PayoutsService {
  private readonly logger = new Logger(PayoutsService.name);

  constructor(private readonly em: EntityManager) {}

  async processPayout(
    eventId: string,
    event: PayoutEvent,
  ): Promise<ProcessResult> {
    try {
      await this.em.transactional(async (em) => {
        const user = await em.findOne(
          User,
          { id: event.userId },
          { lockMode: LockMode.PESSIMISTIC_WRITE },
        );
        if (!user) {
          throw new NotFoundException(`User ${event.userId} not found`);
        }

        const amount = this.calculatePayout(event.basePayout, user.multiplier);
        user.balance = this.addMoney(user.balance, amount);
        em.create(PayoutLedger, {
          user,
          eventId,
          offerId: event.offerId,
          payoutAmount: amount,
          createdAt: new Date(),
        });
      });
      return 'processed';
    } catch (err) {
      if (err instanceof UniqueConstraintViolationException) {
        this.logger.warn(`Event ${eventId} already processed — skipping`);
        return 'duplicate';
      }
      throw err;
    }
  }

  private calculatePayout(basePayout: number, multiplier: number): number {
    const cents = Math.round(basePayout * Number(multiplier) * 100);
    return cents / 100;
  }

  private addMoney(balance: number | string, amount: number): number {
    const cents = Math.round(Number(balance) * 100) + Math.round(amount * 100);
    return cents / 100;
  }
}
