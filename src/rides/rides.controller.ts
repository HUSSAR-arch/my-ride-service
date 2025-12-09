import { Controller, Post, Body, BadRequestException } from '@nestjs/common';
import { RidesService } from './rides.service';

@Controller('rides')
export class RidesController {
  constructor(private readonly ridesService: RidesService) {}

  @Post('request')
  requestRide(@Body() body: any) {
    if (!body || !body.pickup || !body.dropoff) {
      throw new BadRequestException('Invalid request body');
    }

    // Update: Removed 'body.fare' from the arguments list
    return this.ridesService.requestRide(
      body.passengerId,
      body.pickup,
      body.dropoff,
    );
  }
  @Post('accept')
  acceptRide(@Body() body: any) {
    return this.ridesService.acceptRide(body.rideId, body.driverId);
  }

  @Post('update-location')
  updateLocation(@Body() body: any) {
    return this.ridesService.updateDriverLocation(
      body.driverId,
      body.lat,
      body.lng,
    );
  }
  // Add inside RidesController class

  @Post('arrived')
  driverArrived(@Body() body: any) {
    return this.ridesService.updateRideStatus(
      body.rideId,
      body.driverId,
      'ARRIVED',
    );
  }

  @Post('start')
  startTrip(@Body() body: any) {
    return this.ridesService.updateRideStatus(
      body.rideId,
      body.driverId,
      'IN_PROGRESS',
    );
  }

  @Post('complete')
  completeTrip(@Body() body: any) {
    return this.ridesService.updateRideStatus(
      body.rideId,
      body.driverId,
      'COMPLETED',
    );
  }
}
