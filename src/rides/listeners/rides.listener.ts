import { Injectable, Logger } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { RideRequestedEvent, RideTimeoutEvent } from '../events/ride.events';
import { RidesService } from '../rides.service';

@Injectable()
export class RidesListener {
  private readonly logger = new Logger(RidesListener.name);

  constructor(
    private readonly ridesService: RidesService,
    private readonly eventEmitter: EventEmitter2,
  ) {}

  // 1. INSTANT REACTION: Logic runs 0.01s after user clicks "Request"
  @OnEvent('ride.requested')
  async handleRideRequest(payload: RideRequestedEvent) {
    this.logger.log(`⚡ Event Received: Processing Ride ${payload.rideId}`);

    // Trigger the matching logic
    await this.ridesService.matchDriversForRide(
      payload.rideId,
      payload.batchNumber,
    );

    // Schedule the next check (Wait 20s before expanding search)
    // Note: This replaces the Cron Job loop
    setTimeout(() => {
      this.eventEmitter.emit(
        'ride.check_progress',
        new RideTimeoutEvent(payload.rideId),
      );
    }, 20000);
  }

  // 2. DELAYED CHECK: Logic runs if the ride is still pending after 20s
  @OnEvent('ride.check_progress')
  async handleRideProgress(payload: RideTimeoutEvent) {
    const isStillPending = await this.ridesService.isRideStillPending(
      payload.rideId,
    );

    if (isStillPending) {
      this.logger.warn(
        `⏳ Ride ${payload.rideId} still pending. expanding search...`,
      );

      // Expand search radius (increment batch) and RE-EMIT the request event
      const updatedRide = await this.ridesService.incrementRideBatch(
        payload.rideId,
      );

      this.eventEmitter.emit(
        'ride.requested',
        new RideRequestedEvent(
          updatedRide.id,
          updatedRide.fare_estimate,
          updatedRide.dispatch_batch,
        ),
      );
    }
  }
}
