import { z } from 'zod';

export const payoutEventSchema = z.object({
  userId: z.coerce.number().int().positive(),
  offerId: z.string().min(1),
  basePayout: z.coerce.number().positive(),
});

export type PayoutEvent = z.infer<typeof payoutEventSchema>;
