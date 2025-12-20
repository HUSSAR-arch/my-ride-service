import {
  Injectable,
  BadRequestException,
  InternalServerErrorException,
  ServiceUnavailableException,
  Logger,
} from '@nestjs/common';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { latLngToCell, gridDisk } from 'h3-js';

import { Cron, CronExpression } from '@nestjs/schedule';

@Injectable()
export class RidesService {
  private supabase: SupabaseClient;
  private readonly logger = new Logger(RidesService.name);

  constructor() {
    this.supabase = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
    );
  }

  // --- HELPER: Prevents H3 Crashes ---
  private validateCoordinates(lat: number, lng: number): void {
    if (
      lat === null ||
      lng === null ||
      typeof lat !== 'number' ||
      typeof lng !== 'number'
    ) {
      throw new BadRequestException(
        'Latitude and Longitude are required and must be numbers.',
      );
    }
    if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
      throw new BadRequestException(`Invalid GPS coordinates: ${lat}, ${lng}`);
    }
  }

  private getDistanceFromLatLonInKm(lat1, lon1, lat2, lon2) {
    var R = 6371; // Radius of the earth in km
    var dLat = this.deg2rad(lat2 - lat1);
    var dLon = this.deg2rad(lon2 - lon1);
    var a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(this.deg2rad(lat1)) *
        Math.cos(this.deg2rad(lat1)) *
        Math.sin(dLon / 2) *
        Math.sin(dLon / 2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    var d = R * c; // Distance in km
    return d * 1000; // Return meters
  }

  private deg2rad(deg) {
    return deg * (Math.PI / 180);
  }
  async updateRideStatus(
    rideId: string,
    driverId: string,
    status: 'ARRIVED' | 'IN_PROGRESS' | 'COMPLETED',
  ) {
    if (status === 'ARRIVED') {
      // A. Get Ride & Driver Location
      const { data: ride } = await this.supabase
        .from('rides')
        .select('pickup_lat, pickup_lng')
        .eq('id', rideId)
        .single();
      const { data: driverLoc } = await this.supabase
        .from('driver_locations')
        .select('lat, lng')
        .eq('driver_id', driverId)
        .single();

      if (ride && driverLoc) {
        const distance = this.getDistanceFromLatLonInKm(
          driverLoc.lat,
          driverLoc.lng,
          ride.pickup_lat,
          ride.pickup_lng,
        );

        // B. If driver is more than 300 meters away, reject it
        if (distance > 300) {
          throw new BadRequestException(
            `You are too far from pickup (${Math.round(distance)}m). Get closer to mark Arrived.`,
          );
        }
      }
    }
    console.log(`Driver ${driverId} updating ride ${rideId} to ${status}`);

    const { data, error } = await this.supabase
      .from('rides')
      .update({
        status: status,
        updated_at: new Date().toISOString(),
      })
      .eq('id', rideId)
      .eq('driver_id', driverId) // SECURITY: Only the assigned driver can update!
      .select()
      .single();

    if (error || !data) {
      console.error(`Error updating status to ${status}:`, error);
      throw new BadRequestException(
        'Could not update ride status. Check permissions.',
      );
    }

    if (status === 'COMPLETED') {
      // We need to know the payment method and fare, so ensure 'data' (the updated ride) has these fields
      if (data.payment_method === 'WALLET') {
        await this.processWalletPayment(
          data.id,
          data.fare_estimate,
          data.passenger_id,
          driverId,
        );
      }
    }

    return { success: true, ride: data };
  }

  async removeDriverLocation(driverId: string) {
    await this.supabase
      .from('driver_locations')
      .delete()
      .eq('driver_id', driverId);
    return { success: true };
  }

  async updateDriverLocation(
    driverId: string,
    lat: number,
    lng: number,
    heading: any,
  ) {
    try {
      // FIX 1: Validate input before H3 calculation
      this.validateCoordinates(lat, lng);

      // 1. Calculate H3 Index (Resolution 8)
      const h3Index = latLngToCell(lat, lng, 8);

      // 2. Upsert to Supabase
      const { error } = await this.supabase.from('driver_locations').upsert(
        {
          driver_id: driverId,
          lat: lat,
          lng: lng,
          heading: heading,
          current_h3_index: h3Index,
          updated_at: new Date().toISOString(),
        },
        { onConflict: 'driver_id' },
      );

      if (error) throw error;

      return { success: true };
    } catch (err) {
      this.handleError(err, 'updateDriverLocation');
    }
  }

  async requestRide(
    passengerId: string,
    pickup: { lat: number; lng: number },
    dropoff: { lat: number; lng: number },
    pickupAddress: string,
    dropoffAddress: string,
    paymentMethod: 'CASH' | 'WALLET' = 'CASH', // <--- ✅ ACCEPT ARGUMENT
    note?: string,
  ) {
    console.log('🛑 SERVICE HIT! 🛑');
    console.log('Pickup Addr:', pickupAddress);
    console.log('Dropoff Addr:', dropoffAddress);
    console.log('Processing ride for:', passengerId);

    try {
      // 1. Validate Input
      if (!pickup || !dropoff) {
        throw new BadRequestException(
          'Pickup and Dropoff locations are required',
        );
      }
      this.validateCoordinates(pickup.lat, pickup.lng);
      this.validateCoordinates(dropoff.lat, dropoff.lng);

      // 2. SECURITY: Calculate Fare on Server
      // We call your existing SQL function directly from here.
      const { data: calculatedFare, error: fareError } =
        await this.supabase.rpc('calculate_fare_estimate', {
          pickup_lat: pickup.lat,
          pickup_lng: pickup.lng,
          dropoff_lat: dropoff.lat,
          dropoff_lng: dropoff.lng,
        });

      if (fareError || !calculatedFare) {
        console.error('Fare Calculation Failed:', fareError);
        throw new BadRequestException(
          'Could not calculate fare for this route.',
        );
      }
      if (paymentMethod === 'WALLET') {
        const { data: profile } = await this.supabase
          .from('profiles')
          .select('balance')
          .eq('id', passengerId)
          .single();

        // If balance is less than fare, block the request
        if (!profile || profile.balance < calculatedFare) {
          throw new BadRequestException(
            `Insufficient balance. Fare: ${calculatedFare} DZD, You have: ${profile?.balance || 0} DZD`,
          );
        }
      }

      console.log(`Secure Fare Calculated: ${calculatedFare} DZD`);

      // 3. Calculate Search Area (H3 Hexagons)
      const originIndex = latLngToCell(pickup.lat, pickup.lng, 8);
      const nearbyIndices = gridDisk(originIndex, 1);

      // 4. Insert into Supabase
      const { data: rideData, error: insertError } = await this.supabase
        .from('rides')
        .insert({
          passenger_id: passengerId,
          pickup_lat: pickup.lat,
          pickup_lng: pickup.lng,
          dropoff_lat: dropoff.lat,
          dropoff_lng: dropoff.lng,

          pickup_address: pickupAddress, // <--- Must match variable name
          dropoff_address: dropoffAddress, // <--- Must match variable name

          fare_estimate: calculatedFare,
          status: 'PENDING',
          nearby_h3_indices: nearbyIndices,
          note: note || null,

          // ✅ Initialize Batch Logic
          dispatch_batch: 1,
          last_offer_sent_at: new Date().toISOString(),
        })
        .select()
        .single();

      if (insertError) throw insertError;

      // 5. Trigger the Matcher (This sends Batch 1 immediately)
      await this.supabase.rpc('find_and_offer_ride', {
        target_ride_id: rideData.id,
      });

      return rideData;
    } catch (err) {
      this.handleError(err, 'requestRide');
    }
  }

  async acceptRide(rideId: string, driverId: string) {
    console.log(`Driver ${driverId} is attempting to accept ride ${rideId}`);

    try {
      const { data, error } = await this.supabase
        .from('rides')
        .update({
          status: 'ACCEPTED',
          driver_id: driverId,
          updated_at: new Date().toISOString(),
        })
        .eq('id', rideId)
        .eq('status', 'PENDING')
        .select()
        .single();

      if (error || !data) {
        // If Supabase has a network error, it might end up here
        if (error) throw error;
        throw new BadRequestException(
          'Ride is no longer available or already accepted.',
        );
      }
      const { data: passenger } = await this.supabase
        .from('profiles')
        .select('push_token')
        .eq('id', data.passenger_id) // Use data.passenger_id from the updated ride
        .single();

      if (passenger?.push_token) {
        // We don't await this so it doesn't block the response
        this.sendPushNotification(
          passenger.push_token,
          'Yalla! Driver Found 🚗',
          'A driver has accepted your request and is on the way.',
        );
      }

      console.log('Ride accepted successfully!');
      return { success: true, ride: data };
    } catch (err) {
      this.handleError(err, 'acceptRide');
    }
  }
  private async sendPushNotification(
    expoPushToken: string,
    title: string,
    body: string,
  ) {
    if (!expoPushToken || !expoPushToken.startsWith('ExponentPushToken')) {
      console.log('Invalid or missing push token');
      return;
    }

    const message = {
      to: expoPushToken,
      sound: 'default',
      title: title,
      body: body,
      data: { type: 'RIDE_UPDATE' },
    };

    try {
      await fetch('https://exp.host/--/api/v2/push/send', {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Accept-encoding': 'gzip, deflate',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(message),
      });
    } catch (error) {
      console.error('Error sending push notification:', error);
    }
  }
  // --- HELPER: Centralized Error Handling ---
  private handleError(err: any, context: string) {
    console.error(`Crash in ${context}:`, err);

    // FIX 2: Detect Cloudflare HTML errors
    const errorMessage = err.message || JSON.stringify(err);
    if (
      errorMessage.includes('<!DOCTYPE html>') ||
      errorMessage.includes('<html>')
    ) {
      console.error(
        'Supabase/Cloudflare is returning HTML instead of JSON. Service may be down.',
      );
      throw new ServiceUnavailableException(
        'Database service is temporarily unavailable. Please try again later.',
      );
    }

    // Re-throw NestJS exceptions (like BadRequest)
    if (
      err instanceof BadRequestException ||
      err instanceof ServiceUnavailableException
    ) {
      throw err;
    }

    // Default to 500
    throw new InternalServerErrorException(
      'An internal error occurred processing the ride.',
    );
  }

  @Cron(CronExpression.EVERY_30_SECONDS)
  async handleStaleRides() {
    this.logger.log('🕵️ Cron Job: Checking for stuck rides...');

    // Define "Stale" as any ride created more than 2 minutes ago that is still PENDING
    const twoMinutesAgo = new Date(Date.now() - 2 * 60 * 1000).toISOString();

    try {
      // 1. Cancel the stuck rides in the database
      const { data, error } = await this.supabase
        .from('rides')
        .update({
          status: 'NO_DRIVERS_AVAILABLE',
          updated_at: new Date().toISOString(),
        })
        .eq('status', 'PENDING')
        .lt('created_at', twoMinutesAgo)
        .select();

      if (error) {
        this.logger.error('Error cancelling stale rides:', error);
      } else if (data && data.length > 0) {
        this.logger.warn(`⚠️ Timeout: Cancelled ${data.length} stuck rides.`);
      }

      // 2. Clean up old offers
      const { error: rpcError } = await this.supabase.rpc(
        'cleanup_expired_offers',
      );

      if (rpcError) {
        this.logger.error('Error cleaning offers:', rpcError);
      }
    } catch (err) {
      this.logger.error('Cron job failed:', err);
    }
  }

  private async sendDriverPush(driverIds: string[], rideId: string) {
    if (!driverIds || driverIds.length === 0) return;

    try {
      // 1. Fetch Push Tokens for these drivers
      const { data: drivers, error } = await this.supabase
        .from('profiles')
        .select('push_token')
        .in('id', driverIds)
        .not('push_token', 'is', null);

      if (error) {
        this.logger.error('Error fetching push tokens for dispatch:', error);
        return;
      }
      if (!drivers || drivers.length === 0) return;

      // 2. Prepare Expo Messages
      // Filter out empty tokens just in case
      const validTokens = drivers
        .map((d) => d.push_token)
        .filter((t) => t && t.startsWith('ExponentPushToken'));

      if (validTokens.length === 0) return;

      const messages = validTokens.map((token) => ({
        to: token,
        sound: 'default',
        title: 'New Ride Request 🚖',
        body: 'Tap to accept immediately!',
        data: { rideId: rideId, type: 'NEW_OFFER' }, // Critical for "tap to open" logic
        priority: 'high',
        channelId: 'ride-requests-v4', // Must match the channel ID in your frontend
      }));

      // 3. Send to Expo (Using simple fetch for valid chunks)
      // Note: Expo recommends chunking arrays > 100 items.
      // For this scale, a direct send is usually fine.
      await fetch('https://exp.host/--/api/v2/push/send', {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Accept-encoding': 'gzip, deflate',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(messages),
      });

      this.logger.log(
        `📲 Sent push notifications to ${validTokens.length} drivers.`,
      );
    } catch (err) {
      this.logger.error('Failed to send driver push notifications:', err);
    }
  }

  @Cron('*/10 * * * * *') // Every 10 seconds
  async handleDispatchWaves() {
    // 1. Find rides that are stuck in PENDING and need a new batch

    // LATENCY BUFFER: We wait 20 seconds (Server) vs 15 seconds (Client)
    // This gives a 5-second safety window for network delays.
    const twentySecondsAgo = new Date(Date.now() - 310 * 1000).toISOString();

    const { data: stuckRides, error } = await this.supabase
      .from('rides')
      .select('id, dispatch_batch')
      .eq('status', 'PENDING')
      .lt('last_offer_sent_at', twentySecondsAgo);

    if (error) {
      this.logger.error('Error fetching stuck rides:', error);
      return;
    }

    if (!stuckRides || stuckRides.length === 0) return;

    this.logger.log(`🌊 Processing waves for ${stuckRides.length} rides...`);

    for (const ride of stuckRides) {
      // Cap the batch level at 3 so we don't increment forever
      const nextBatch = ride.dispatch_batch >= 3 ? 3 : ride.dispatch_batch + 1;

      // A. Update the batch level in DB
      await this.supabase
        .from('rides')
        .update({
          dispatch_batch: nextBatch,
          // We update 'last_offer_sent_at' here so the Cron doesn't pick it up again immediately
          last_offer_sent_at: new Date().toISOString(),
        })
        .eq('id', ride.id);

      // B. Trigger the SQL function to find new drivers
      // CRITICAL: We now capture the 'data' return value which contains the list of notified drivers
      const { data: offeredDriverIds, error: rpcError } =
        await this.supabase.rpc('find_and_offer_ride', {
          target_ride_id: ride.id,
        });

      if (rpcError) {
        this.logger.error(`Error dispatching ride ${ride.id}:`, rpcError);
      } else {
        this.logger.log(
          `Ride ${ride.id} promoted to Batch ${nextBatch}. Notified ${
            offeredDriverIds?.length || 0
          } drivers.`,
        );

        // C. Send "Wake Up" Pushes to the drivers who just got the offer
        if (
          offeredDriverIds &&
          Array.isArray(offeredDriverIds) &&
          offeredDriverIds.length > 0
        ) {
          // We don't await this to keep the loop moving fast
          this.sendDriverPush(offeredDriverIds, ride.id);
        }
      }
    }
  }

  async updateDriverLocationBatch(driverId: string, locations: any[]) {
    if (!locations || locations.length === 0) return { success: true };

    // 1. Get the latest location
    const latest = locations[locations.length - 1];

    try {
      this.validateCoordinates(latest.lat, latest.lng);
      const h3Index = latLngToCell(latest.lat, latest.lng, 8);

      // 2. Upsert with HEADING
      const { error: liveError } = await this.supabase
        .from('driver_locations')
        .upsert(
          {
            driver_id: driverId,
            lat: latest.lat,
            lng: latest.lng,
            heading: latest.heading || 0, // <--- ADD THIS LINE
            current_h3_index: h3Index,
            updated_at: new Date().toISOString(),
          },
          { onConflict: 'driver_id' },
        );

      if (liveError) throw liveError;

      return { success: true };
    } catch (err) {
      this.handleError(err, 'updateDriverLocationBatch');
    }
  }

  private async processWalletPayment(
    rideId: string,
    fare: number,
    passengerId: string,
    driverId: string,
  ) {
    this.logger.log(`💰 Processing Wallet Payment: ${fare} DZD`);

    // 1. Deduct from Passenger
    const { error: pError } = await this.supabase.rpc('decrement_balance', {
      user_id: passengerId,
      amount: fare,
    });

    if (pError) {
      this.logger.error('Failed to deduct from passenger', pError);
      // In a real app, you might want to flag this for manual review
    }

    // 2. Add to Driver
    const { error: dError } = await this.supabase.rpc('increment_balance', {
      user_id: driverId,
      amount: fare,
    });

    // 3. Mark Ride as Paid
    await this.supabase
      .from('rides')
      .update({ payment_status: 'PAID' })
      .eq('id', rideId);
  }
}
