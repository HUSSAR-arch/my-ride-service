import { Module } from '@nestjs/common';
import { RidesService } from './rides.service';
import { RidesController } from './rides.controller';
import { RidesListener } from './listeners/rides.listener';

@Module({
  controllers: [RidesController],
  providers: [RidesService, RidesListener],
})
export class RidesModule {}
