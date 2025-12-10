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
  async updateRideStatus(
    rideId: string,
    driverId: string,
    status: 'ARRIVED' | 'IN_PROGRESS' | 'COMPLETED',
  ) {
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

    return { success: true, ride: data };
  }

  async updateDriverLocation(driverId: string, lat: number, lng: number) {
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
    pickupAddress: string, // <--- ADD ARGUMENT
    dropoffAddress: string, // <--- ADD ARGUMENT
  ) {
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

          pickup_address: pickupAddress,
          dropoff_address: dropoffAddress,
          fare_estimate: calculatedFare,
          status: 'PENDING',
          nearby_h3_indices: nearbyIndices,

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

      console.log('Ride accepted successfully!');
      return { success: true, ride: data };
    } catch (err) {
      this.handleError(err, 'acceptRide');
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

  @Cron('*/10 * * * * *') // Every 10 seconds
  async handleDispatchWaves() {
    // 1. Find rides that are stuck in PENDING and need a new batch
    // Condition: Status is PENDING AND last offer was sent > 15 seconds ago
    const fifteenSecondsAgo = new Date(Date.now() - 15 * 1000).toISOString();

    const { data: stuckRides, error } = await this.supabase
      .from('rides')
      .select('id, dispatch_batch')
      .eq('status', 'PENDING')
      .lt('last_offer_sent_at', fifteenSecondsAgo);

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
          // We update 'last_offer_sent_at' inside the SQL function,
          // but updating it here ensures the Cron doesn't pick it up again immediately if the SQL fails.
          last_offer_sent_at: new Date().toISOString(),
        })
        .eq('id', ride.id);

      // B. Trigger the SQL function to find new drivers for this batch
      await this.supabase.rpc('find_and_offer_ride', {
        target_ride_id: ride.id,
      });

      this.logger.log(`Ride ${ride.id} promoted to Batch ${nextBatch}`);
    }
  }
}
