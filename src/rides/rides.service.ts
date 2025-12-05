import {
  Injectable,
  BadRequestException,
  InternalServerErrorException,
  ServiceUnavailableException,
} from '@nestjs/common';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { latLngToCell, gridDisk } from 'h3-js';

@Injectable()
export class RidesService {
  private supabase: SupabaseClient;

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
    pickup: { lat: number; lng: number }, // Typed explicitly
    dropoff: { lat: number; lng: number }, // Typed explicitly
    fare: number,
  ) {
    console.log('Processing ride for:', passengerId);

    try {
      // FIX 1: Validate input before H3 calculation
      if (!pickup || !dropoff)
        throw new BadRequestException(
          'Pickup and Dropoff locations are required',
        );
      this.validateCoordinates(pickup.lat, pickup.lng);
      this.validateCoordinates(dropoff.lat, dropoff.lng);

      // 1. Calculate Search Area (Hexagons)
      const originIndex = latLngToCell(pickup.lat, pickup.lng, 8);
      const nearbyIndices = gridDisk(originIndex, 1);

      // 2. Insert into Supabase
      const { data: rideData, error: insertError } = await this.supabase
        .from('rides')
        .insert({
          passenger_id: passengerId,
          pickup_lat: pickup.lat,
          pickup_lng: pickup.lng,
          dropoff_lat: dropoff.lat,
          dropoff_lng: dropoff.lng,
          fare_estimate: fare,
          status: 'PENDING',
          nearby_h3_indices: nearbyIndices,
        })
        .select()
        .single();

      if (insertError) throw insertError;

      console.log('Ride created successfully:', rideData.id);

      // 3. Trigger the Matcher
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
}
