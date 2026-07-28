import {
  BadRequestException,
  Body,
  Controller,
  HttpCode,
  Post,
} from '@nestjs/common';
import { PayoutsPublisher } from './payouts.publisher';
import { payoutEventSchema } from './payouts.schema';

@Controller('payouts')
export class PayoutsController {
  constructor(private readonly publisher: PayoutsPublisher) {}

  @Post()
  @HttpCode(202)
  async publish(@Body() body: unknown) {
    const result = payoutEventSchema.safeParse(body);
    if (!result.success) {
      throw new BadRequestException(result.error.issues);
    }
    const id = await this.publisher.publish(result.data);
    return { status: 'accepted', id };
  }
}
