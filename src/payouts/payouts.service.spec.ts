import { Test } from '@nestjs/testing';
import {
  EntityManager,
  UniqueConstraintViolationException,
} from '@mikro-orm/core';
import { NotFoundException } from '@nestjs/common';
import { PayoutsService } from './payouts.service';
import { User } from '../users/user.entity';
import { PayoutLedger } from './payout-ledger.entity';

const EVENT = { userId: 1, offerId: 'offer-1', basePayout: 10 };

describe('PayoutsService', () => {
  let service: PayoutsService;
  let txEm: { findOne: jest.Mock; create: jest.Mock };

  beforeEach(async () => {
    txEm = { findOne: jest.fn(), create: jest.fn() };
    const em = {
      transactional: jest.fn((cb: (em: typeof txEm) => unknown) => cb(txEm)),
    };

    const moduleRef = await Test.createTestingModule({
      providers: [PayoutsService, { provide: EntityManager, useValue: em }],
    }).compile();

    service = moduleRef.get(PayoutsService);
  });

  it('applies the user multiplier and records eventId + offerId', async () => {
    const user = { id: 1, balance: 100, multiplier: 1.5 } as User;
    txEm.findOne.mockResolvedValue(user);

    const result = await service.processPayout('evt-1', EVENT);

    expect(result).toBe('processed');
    expect(user.balance).toBe(115);
    expect(txEm.create).toHaveBeenCalledWith(
      PayoutLedger,
      expect.objectContaining({
        user,
        eventId: 'evt-1',
        offerId: 'offer-1',
        payoutAmount: 15,
      }),
    );
  });

  it('uses integer-cents math to avoid floating-point drift', async () => {
    const user = { id: 1, balance: 0, multiplier: 1.1 } as User;
    txEm.findOne.mockResolvedValue(user);

    await service.processPayout('evt-2', { ...EVENT, basePayout: 0.1 });

    expect(user.balance).toBe(0.11);
  });

  it('handles string decimals returned by the driver', async () => {
    const user = {
      id: 1,
      balance: '50.00',
      multiplier: '1.20',
    } as unknown as User;
    txEm.findOne.mockResolvedValue(user);

    await service.processPayout('evt-3', EVENT);

    expect(user.balance).toBeCloseTo(62);
  });

  it('treats a duplicate event as a no-op (idempotency)', async () => {
    const user = { id: 1, balance: 100, multiplier: 1 } as User;
    txEm.findOne.mockResolvedValue(user);
    txEm.create.mockImplementation(() => {
      throw new UniqueConstraintViolationException(new Error('duplicate key'));
    });

    const result = await service.processPayout('evt-dup', EVENT);

    expect(result).toBe('duplicate');
  });

  it('throws when the user does not exist', async () => {
    txEm.findOne.mockResolvedValue(null);

    await expect(service.processPayout('evt-4', EVENT)).rejects.toBeInstanceOf(
      NotFoundException,
    );
    expect(txEm.create).not.toHaveBeenCalled();
  });
});
