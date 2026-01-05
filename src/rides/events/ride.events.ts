// This simple class holds the data passed from the Service to the Listener
export class RideRequestedEvent {
  constructor(
    public readonly rideId: string,
    public readonly fare: number,
    public readonly batchNumber: number = 1,
  ) {}
}

// This event fires when we need to check if a ride is still waiting
export class RideTimeoutEvent {
  constructor(public readonly rideId: string) {}
}
