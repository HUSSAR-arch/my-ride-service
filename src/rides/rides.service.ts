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

  // --- MAIN STATUS UPDATE LOGIC ---
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
        .select()
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

    // ✅ NEW LOGIC: Handle Payments based on method
    if (status === 'COMPLETED') {
      if (data.payment_method === 'WALLET') {
        await this.processWalletPayment(
          data.id,
          data.fare_estimate,
          data.passenger_id,
          driverId,
        );
      } else if (data.payment_method === 'CASH') {
        await this.processCashCommission(data.id, data.fare_estimate, driverId);
      }
    }

    return { success: true, ride: data };
  }

  // --- PAYMENT HELPERS ---

  private async processWalletPayment(
    rideId: string,
    fare: number,
    passengerId: string,
    driverId: string,
  ) {
    this.logger.log(`💰 Processing Wallet Payment: ${fare} DZD`);

    // 1. Deduct FULL FARE from Passenger
    const { error: pError } = await this.supabase.rpc('decrement_balance', {
      user_id: passengerId,
      amount: fare,
    });

    if (pError) {
      this.logger.error('Failed to deduct from passenger', pError);
      // TODO: Handle failure (e.g., mark ride as "PAYMENT_FAILED")
      return;
    }

    // 2. Calculate Driver Earnings (Fare - 12%)
    const commission = fare * 0.12;
    const driverEarnings = fare - commission;

    // 3. Add NET EARNINGS to Driver
    const { error: dError } = await this.supabase.rpc('increment_balance', {
      user_id: driverId,
      amount: driverEarnings,
    });

    if (dError) {
      this.logger.error('Failed to pay driver', dError);
    }

    // 4. Record the Commission Transaction
    await this.supabase.from('transactions').insert({
      driver_id: driverId,
      amount: -commission,
      description: 'Ride Commission (12%) - Wallet Ride',
      ride_id: rideId,
    });

    // 5. Mark Ride as Paid
    await this.supabase
      .from('rides')
      .update({ payment_status: 'PAID' })
      .eq('id', rideId);
  }

  private async processCashCommission(
    rideId: string,
    fare: number,
    driverId: string,
  ) {
    this.logger.log(`💵 Processing Cash Commission for: ${fare} DZD`);

    const commission = fare * 0.12;

    // 1. Deduct Commission from Driver's Balance
    // (We use 'decrement_balance' because they owe us this money)
    const { error } = await this.supabase.rpc('decrement_balance', {
      user_id: driverId,
      amount: commission,
    });

    if (error) {
      this.logger.error('Failed to deduct commission from driver', error);
    }

    // 2. Record Transaction
    await this.supabase.from('transactions').insert({
      driver_id: driverId,
      amount: -commission,
      description: 'Ride Commission (12%) - Cash Ride',
      ride_id: rideId,
    });

    // 3. Mark Ride as Paid (Driver collected cash)
    await this.supabase
      .from('rides')
      .update({ payment_status: 'PAID' })
      .eq('id', rideId);
  }

  // --- OTHER SERVICE METHODS ---

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
    paymentMethod: 'CASH' | 'WALLET' = 'CASH',
    note?: string,
    offeredFare?: number, // <--- ✅ 1. Accept optional custom fare
    scheduledTime?: string,
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

      // 2. SECURITY: Calculate Floor Price on Server
      const {
        data: floorPrice,
        error: fareError,
      } = // <--- Renamed variable for clarity
        await this.supabase.rpc('calculate_fare_estimate', {
          pickup_lat: pickup.lat,
          pickup_lng: pickup.lng,
          dropoff_lat: dropoff.lat,
          dropoff_lng: dropoff.lng,
        });

      if (fareError || !floorPrice) {
        console.error('Fare Calculation Failed:', fareError);
        throw new BadRequestException(
          'Could not calculate fare for this route.',
        );
      }

      // ✅ 3. HYBRID PRICING LOGIC
      // If user offers a price, take the higher of the two.
      // This prevents undercutting but allows "bidding up".
      let finalPrice = floorPrice;

      if (offeredFare && typeof offeredFare === 'number') {
        if (offeredFare > floorPrice) {
          console.log(`🚀 User boosted price: ${floorPrice} -> ${offeredFare}`);
          finalPrice = offeredFare;
        } else {
          console.log(
            `🛡️ User offer too low (${offeredFare}). Enforcing floor: ${floorPrice}`,
          );
          // We keep finalPrice as floorPrice
        }
      }

      // 4. Wallet Balance Check (Using finalPrice)
      if (paymentMethod === 'WALLET') {
        const { data: profile } = await this.supabase
          .from('profiles')
          .select('balance')
          .eq('id', passengerId)
          .single();

        // If balance is less than fare, block the request
        if (!profile || profile.balance < finalPrice) {
          throw new BadRequestException(
            `Insufficient balance. Fare: ${finalPrice} DZD, You have: ${profile?.balance || 0} DZD`,
          );
        }
      }

      console.log(`✅ Final Secure Fare: ${finalPrice} DZD`);

      // 5. Calculate Search Area (H3 Hexagons)
      // 5. Calculate Search Area (H3 Hexagons)
      const originIndex = latLngToCell(pickup.lat, pickup.lng, 8);
      const nearbyIndices = gridDisk(originIndex, 10);

      // ✅ FIX: Define status based on schedule
      const initialStatus = scheduledTime ? 'SCHEDULED' : 'PENDING';

      // 6. Insert into Supabase
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

          fare_estimate: finalPrice,

          status: initialStatus, // <--- ✅ Correctly uses the variable
          scheduled_time: scheduledTime || null,

          payment_method: paymentMethod,
          nearby_h3_indices: nearbyIndices,
          note: note || null,

          dispatch_batch: 1,
          last_offer_sent_at: new Date().toISOString(),
        })
        .select()
        .single();

      if (insertError) throw insertError;

      // 7. Trigger the Matcher
      if (!scheduledTime) {
        await this.supabase.rpc('find_and_offer_ride', {
          target_ride_id: rideData.id,
        });
      } else {
        this.logger.log(
          `📅 Ride ${rideData.id} scheduled for ${scheduledTime}`,
        );
      }

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
        if (error) throw error;
        throw new BadRequestException(
          'Ride is no longer available or already accepted.',
        );
      }
      const { data: passenger } = await this.supabase
        .from('profiles')
        .select('push_token')
        .eq('id', data.passenger_id)
        .single();

      if (passenger?.push_token) {
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

    if (
      err instanceof BadRequestException ||
      err instanceof ServiceUnavailableException
    ) {
      throw err;
    }

    throw new InternalServerErrorException(
      'An internal error occurred processing the ride.',
    );
  }

  @Cron(CronExpression.EVERY_30_SECONDS)
  async handleStaleRides() {
    this.logger.log('🕵️ Cron Job: Checking for stuck rides...');

    const twoMinutesAgo = new Date(Date.now() - 2 * 60 * 1000).toISOString();

    try {
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

      const validTokens = drivers
        .map((d) => d.push_token)
        .filter((t) => t && t.startsWith('ExponentPushToken'));

      if (validTokens.length === 0) return;

      const messages = validTokens.map((token) => ({
        to: token,
        sound: 'default',
        title: 'New Ride Request 🚖',
        body: 'Tap to accept immediately!',
        data: { rideId: rideId, type: 'NEW_OFFER' },
        priority: 'high',
        channelId: 'ride-requests-v4',
      }));

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
      const nextBatch = ride.dispatch_batch >= 3 ? 3 : ride.dispatch_batch + 1;

      await this.supabase
        .from('rides')
        .update({
          dispatch_batch: nextBatch,
          last_offer_sent_at: new Date().toISOString(),
        })
        .eq('id', ride.id);

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

        if (
          offeredDriverIds &&
          Array.isArray(offeredDriverIds) &&
          offeredDriverIds.length > 0
        ) {
          this.sendDriverPush(offeredDriverIds, ride.id);
        }
      }
    }
  }

  @Cron(CronExpression.EVERY_MINUTE)
  async activateScheduledRides() {
    this.logger.log('⏰ Checking for scheduled rides to activate...');

    // We want to activate rides 20 minutes before their scheduled time
    const now = new Date();
    const twentyMinutesFromNow = new Date(
      now.getTime() + 20 * 60000,
    ).toISOString();

    try {
      // 1. Find rides that are SCHEDULED and due soon
      const { data: ridesToActivate, error } = await this.supabase
        .from('rides')
        .select('*')
        .eq('status', 'SCHEDULED')
        .lte('scheduled_time', twentyMinutesFromNow); // Time is <= Now + 20min

      if (error) {
        this.logger.error('Error fetching scheduled rides', error);
        return;
      }

      if (!ridesToActivate || ridesToActivate.length === 0) return;

      this.logger.log(
        `🚀 Activating ${ridesToActivate.length} scheduled rides!`,
      );

      // 2. Loop through and activate them
      for (const ride of ridesToActivate) {
        // A. Update Status to PENDING (Live)
        await this.supabase
          .from('rides')
          .update({
            status: 'PENDING',
            updated_at: new Date().toISOString(),
            // Reset dispatch timers so it looks fresh
            last_offer_sent_at: new Date().toISOString(),
            dispatch_batch: 1,
          })
          .eq('id', ride.id);

        // B. Trigger Dispatcher
        await this.supabase.rpc('find_and_offer_ride', {
          target_ride_id: ride.id,
        });

        // C. Notify Passenger
        const { data: passenger } = await this.supabase
          .from('profiles')
          .select('push_token')
          .eq('id', ride.passenger_id)
          .single();

        if (passenger?.push_token) {
          this.sendPushNotification(
            passenger.push_token,
            'Ride Activating ⏰',
            'We are now looking for a driver for your scheduled ride.',
          );
        }
      }
    } catch (err) {
      this.handleError(err, 'activateScheduledRides');
    }
  }

  async updateDriverLocationBatch(driverId: string, locations: any[]) {
    if (!locations || locations.length === 0) return { success: true };

    const latest = locations[locations.length - 1];

    try {
      this.validateCoordinates(latest.lat, latest.lng);
      const h3Index = latLngToCell(latest.lat, latest.lng, 8);

      const { error: liveError } = await this.supabase
        .from('driver_locations')
        .upsert(
          {
            driver_id: driverId,
            lat: latest.lat,
            lng: latest.lng,
            heading: latest.heading || 0,
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
  async processNoShowFee(rideId: string, driverId: string) {
    const NO_SHOW_FEE = 150;
    const COMMISSION_RATE = 0.12;

    this.logger.log(`🚫 Processing No-Show for Ride ${rideId}`);

    // 1. Verify Ride Status
    const { data: ride, error: rideError } = await this.supabase
      .from('rides')
      .select('status, passenger_id, payment_method')
      .eq('id', rideId)
      .eq('driver_id', driverId)
      .single();

    if (rideError || !ride) {
      throw new BadRequestException('Ride not found or access denied.');
    }

    if (ride.status !== 'ARRIVED') {
      throw new BadRequestException(
        'Ride must be in ARRIVED status to charge no-show.',
      );
    }

    // 2. Update Status to CANCELLED
    const { error: updateError } = await this.supabase
      .from('rides')
      .update({
        status: 'CANCELLED',
        cancellation_reason: 'PASSENGER_NO_SHOW',
        payment_status: 'PAID',
        fare_estimate: NO_SHOW_FEE,
        updated_at: new Date().toISOString(),
      })
      .eq('id', rideId);

    if (updateError) {
      throw new InternalServerErrorException('Failed to cancel ride.');
    }

    // 3. MOVE THE MONEY (Balances Only)

    // A. Charge Passenger
    const { error: pError } = await this.supabase.rpc('decrement_balance', {
      user_id: ride.passenger_id,
      amount: NO_SHOW_FEE,
    });

    // If this fails, we log it but don't crash the request
    if (pError) this.logger.error('Failed to charge passenger', pError);

    // B. Pay Driver (Fee minus 12% commission)
    const driverEarnings = NO_SHOW_FEE - NO_SHOW_FEE * COMMISSION_RATE;

    const { error: dError } = await this.supabase.rpc('increment_balance', {
      user_id: driverId,
      amount: driverEarnings,
    });

    if (dError) this.logger.error('Failed to credit driver', dError);

    // ✅ REMOVED: The transactions table insert is gone.
    // The request will now succeed even if that table is broken.

    return { success: true, message: 'No-show processed successfully' };
  }
}
