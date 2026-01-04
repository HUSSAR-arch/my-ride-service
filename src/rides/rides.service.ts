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
  // In rides.service.ts

  // Find the existing updateRideStatus method and replace it with this:
  async updateRideStatus(
    rideId: string,
    driverId: string,
    status: 'ARRIVED' | 'IN_PROGRESS' | 'COMPLETED',
    lat?: number, // Optional: Client-provided Lat
    lng?: number, // Optional: Client-provided Lng
  ) {
    // 1. ENFORCED Distance Check for ARRIVED
    if (status === 'ARRIVED') {
      const MAX_DISTANCE_METERS = 500; // Increased from 300 to 500 for GPS drift safety

      // Fetch Ride Pickup Coordinates
      const { data: ride } = await this.supabase
        .from('rides')
        .select('pickup_lat, pickup_lng')
        .eq('id', rideId)
        .single();

      let currentLat = lat;
      let currentLng = lng;

      // If client didn't send coords, fetch from DB (Fallback)
      if (!currentLat || !currentLng) {
        const { data: driverLoc } = await this.supabase
          .from('driver_locations')
          .select('lat, lng')
          .eq('driver_id', driverId)
          .single();

        if (driverLoc) {
          currentLat = driverLoc.lat;
          currentLng = driverLoc.lng;
        }
      }

      if (!ride || !currentLat || !currentLng) {
        // Log warning but allow it to proceed if location is missing (prevents getting stuck)
        this.logger.warn(
          `⚠️ skipping distance check for Ride ${rideId} (Location missing)`,
        );
      } else {
        const distance = this.getDistanceFromLatLonInKm(
          currentLat,
          currentLng,
          ride.pickup_lat,
          ride.pickup_lng,
        );

        this.logger.log(
          `Distance Check: ${distance.toFixed(0)}m (Max: ${MAX_DISTANCE_METERS}m)`,
        );

        if (distance > MAX_DISTANCE_METERS) {
          throw new BadRequestException(
            `GPS says you are ${Math.floor(distance)}m away. Please move closer to pickup.`,
          );
        }
      }
    }

    console.log(`Driver ${driverId} updating ride ${rideId} to ${status}`);

    // 2. (Existing) Update the Ride Status in DB
    const { data, error } = await this.supabase
      .from('rides')
      .update({
        status: status,
        updated_at: new Date().toISOString(),
      })
      .eq('id', rideId)
      .eq('driver_id', driverId)
      .select()
      .single();

    if (error || !data) {
      console.error(`Error updating status to ${status}:`, error);
      throw new BadRequestException(
        'Could not update ride status. Check permissions.',
      );
    }

    (async () => {
      try {
        const { data: passenger } = await this.supabase
          .from('profiles')
          .select('push_token')
          .eq('id', data.passenger_id)
          .single();

        if (passenger?.push_token) {
          let title = 'Ride Update';
          let body = '';

          switch (status) {
            case 'ARRIVED':
              title = 'Driver Arrived 🚖';
              body = 'Your driver is waiting at the pickup location.';
              break;
            case 'IN_PROGRESS':
              title = 'Ride Started 🚀';
              body = 'You are on your way to the destination!';
              break;
            case 'COMPLETED':
              title = 'Ride Completed ✅';
              body = 'You have arrived. Thank you for riding with us!';
              break;
          }
          await this.sendPushNotification(passenger.push_token, title, body);
        }
      } catch (err) {
        this.logger.error('Failed to notify passenger', err);
      }
    })();

    // 4. (Existing) Handle Payments logic
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

    let finalPaymentStatus = 'PAID';

    // 1. Attempt to Deduct from Passenger
    const { error: pError } = await this.supabase.rpc('decrement_balance', {
      user_id: passengerId,
      amount: fare,
    });

    if (pError) {
      this.logger.error(
        `🚨 Wallet Drainage Detected! Passenger ${passengerId} could not pay.`,
        pError,
      );
      // FIX: Do NOT return. Mark as FAILED so we know they owe money.
      finalPaymentStatus = 'PAYMENT_FAILED';
    }

    // 2. Calculate Driver Earnings (Fare - 12%)
    // NOTE: We pay the driver even if passenger failed (Platform covers it)
    const commission = fare * 0.12;
    const driverEarnings = fare - commission;

    // 3. Add NET EARNINGS to Driver
    const { error: dError } = await this.supabase.rpc('increment_balance', {
      user_id: driverId,
      amount: driverEarnings,
    });

    if (dError) {
      this.logger.error('Failed to pay driver', dError);
    } else {
      // Only record the commission transaction if the driver was actually paid
      await this.supabase.from('transactions').insert({
        driver_id: driverId,
        amount: -commission,
        description: 'Ride Commission (12%) - Wallet Ride',
        ride_id: rideId,
      });
    }

    // 4. Update Ride Status
    // We save 'PAYMENT_FAILED' if the deduction failed.
    // This creates a record of the debt.
    await this.supabase
      .from('rides')
      .update({ payment_status: finalPaymentStatus })
      .eq('id', rideId);

    this.logger.log(
      `Payment processing complete. Status: ${finalPaymentStatus}`,
    );
  }

  private async processCashCommission(
    rideId: string,
    fare: number,
    driverId: string,
  ) {
    this.logger.log(`💵 Processing Cash Commission for: ${fare} DZD`);

    const commission = fare * 0.12;
    let paymentStatus = 'PAID'; // Default to success

    // 1. Attempt to Deduct Commission from Driver
    // We expect the RPC to return an error if balance is insufficient
    const { error: deductError } = await this.supabase.rpc(
      'decrement_balance',
      {
        user_id: driverId,
        amount: commission,
      },
    );

    if (deductError) {
      this.logger.error(
        `❌ Failed to collect commission from Driver ${driverId}`,
        deductError,
      );

      // FIX: Do NOT proceed as normal. Mark this debt!
      // This prevents the "Free Pass" where drivers keep 100% because the deduction failed.
      paymentStatus = 'COMMISSION_OWED';

      // Optional: You might want to block the driver here or send a warning
    } else {
      // 2. ONLY Record Transaction if Deduction Succeeded
      // This prevents "Ghost Deductions" (Receipt exists, but money wasn't taken)
      const { error: txnError } = await this.supabase
        .from('transactions')
        .insert({
          driver_id: driverId,
          amount: -commission,
          description: 'Ride Commission (12%) - Cash Ride',
          ride_id: rideId, // Linked for disputes
          status: 'COMPLETED',
        });

      if (txnError) {
        // Critical Edge Case: Money was taken, but receipt failed.
        // Log this with HIGH priority for support to see.
        this.logger.error(
          `🚨 MONEY LOST? Deduction success but txn failed for Ride ${rideId}`,
          txnError,
        );
      }
    }

    // 3. Update the Ride Status
    // We save the status as 'PAID' (all good) or 'COMMISSION_OWED' (debt)
    const { error: updateError } = await this.supabase
      .from('rides')
      .update({
        payment_status: paymentStatus,
        updated_at: new Date().toISOString(),
      })
      .eq('id', rideId);

    if (updateError) {
      this.logger.error('Failed to update ride payment status', updateError);
    }
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

      // 0. BLOCK DEBTORS: Check for outstanding failed payments
      const { data: debtRides } = await this.supabase
        .from('rides')
        .select('id')
        .eq('passenger_id', passengerId)
        .eq('payment_status', 'PAYMENT_FAILED');

      if (debtRides && debtRides.length > 0) {
        throw new BadRequestException(
          'You have an outstanding payment from a previous ride. Please settle your balance.',
        );
      }

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
        this.logger.log(
          `⚡ Triggering immediate matcher for Ride ${rideData.id}`,
        );

        // 1. Run the Database Matcher
        const { data: offeredDriverIds, error: rpcError } =
          await this.supabase.rpc('find_and_offer_ride', {
            target_ride_id: rideData.id,
          });

        if (rpcError) {
          this.logger.error('❌ Matcher RPC failed:', rpcError);
        } else {
          this.logger.log(
            `🔍 Matcher found ${offeredDriverIds?.length || 0} drivers.`,
          );

          // 2. Send Push Notification IMMEDIATELY
          if (offeredDriverIds && offeredDriverIds.length > 0) {
            // ✅ Pass the fare (3rd argument) so the driver sees "Earn X DZD"
            this.sendDriverPush(
              offeredDriverIds,
              rideData.id,
              rideData.fare_estimate,
            ).catch((e) =>
              this.logger.error('Failed to send immediate push', e),
            );
          }
        }
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

  // In rides.service.ts

  async acceptRide(rideId: string, driverId: string) {
    this.logger.log(`Driver ${driverId} accepting ride ${rideId}`);

    try {
      // 1. Fetch the ride first to check who owns it
      const { data: ride } = await this.supabase
        .from('rides')
        .select('status, passenger_id, driver_id, scheduled_time')
        .eq('id', rideId)
        .single();

      if (!ride) throw new BadRequestException('Ride not found.');

      // 2. Validate Status
      // We allow PENDING, SCHEDULED, or NO_DRIVERS_AVAILABLE (for rescue)
      const validStatuses = ['PENDING', 'SCHEDULED', 'NO_DRIVERS_AVAILABLE'];

      // Allow if status is valid OR if it's already accepted by THIS driver (Idempotency)
      const isAlreadyMyRide =
        ride.status === 'ACCEPTED' && ride.driver_id === driverId;

      if (!validStatuses.includes(ride.status) && !isAlreadyMyRide) {
        throw new BadRequestException('Ride is no longer available.');
      }

      // 3. Determine Handling based on Driver ID
      let updateQuery;
      const newStatus = ride.status === 'SCHEDULED' ? 'SCHEDULED' : 'ACCEPTED';

      if (ride.driver_id === driverId) {
        // ✅ SCENARIO A: You are ALREADY the assigned driver (Scheduled Activation)
        // We just update the status to confirm you are live.
        updateQuery = this.supabase
          .from('rides')
          .update({
            status: newStatus,
            updated_at: new Date().toISOString(),
            // Don't need to set driver_id, it's already you
          })
          .eq('id', rideId)
          .eq('driver_id', driverId); // Security check: Ensure it's still you
      } else {
        // ✅ SCENARIO B: It's an open ride (New Request)
        // We must ensure driver_id is NULL so two people don't grab it
        updateQuery = this.supabase
          .from('rides')
          .update({
            status: newStatus,
            driver_id: driverId,
            updated_at: new Date().toISOString(),
            cancellation_reason: null,
          })
          .eq('id', rideId)
          .is('driver_id', null); // Security check: Ensure it's empty
      }

      // 4. Execute the Query
      const { data, error } = await updateQuery.select().single();

      if (error || !data) {
        this.logger.error('Accept failed', error);
        throw new BadRequestException('Ride taken or unavailable.');
      }

      // 5. Send Notification to Passenger
      const { data: passenger } = await this.supabase
        .from('profiles')
        .select('push_token')
        .eq('id', data.passenger_id)
        .single();

      if (passenger?.push_token) {
        const isScheduled = ride.status === 'SCHEDULED';

        const title = isScheduled
          ? 'Ride Confirmed 📅'
          : 'Yalla! Driver Found 🚗';

        const body = isScheduled
          ? `A driver has accepted your scheduled ride for ${new Date(ride.scheduled_time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}.`
          : 'A driver has accepted your request and is on the way.';

        this.sendPushNotification(passenger.push_token, title, body);
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

    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    try {
      const { data, error } = await this.supabase
        .from('rides')
        .update({
          status: 'NO_DRIVERS_AVAILABLE',
          updated_at: new Date().toISOString(),
        })
        .eq('status', 'PENDING')
        .lt('updated_at', fiveMinutesAgo)
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

  // --- CRON JOB: Fixes "Driver Hoarding" ---
  @Cron(CronExpression.EVERY_MINUTE)
  async handleStuckAcceptedRides() {
    this.logger.log(
      '🕵️ Cron Job: Checking for hoarded/stuck ACCEPTED rides...',
    );

    // TIMEOUT CONFIG: 20 Minutes
    // If status is still just 'ACCEPTED' after 20 mins, the driver is likely not coming.
    const timeoutThreshold = new Date(
      Date.now() - 20 * 60 * 1000,
    ).toISOString();

    try {
      // 1. Find rides stuck in 'ACCEPTED' state
      const { data: stuckRides, error } = await this.supabase
        .from('rides')
        .select('id, passenger_id, driver_id')
        .eq('status', 'ACCEPTED')
        .lt('updated_at', timeoutThreshold);

      if (error) {
        this.logger.error('Error fetching stuck accepted rides', error);
        return;
      }

      if (!stuckRides || stuckRides.length === 0) return;

      this.logger.warn(
        `⚠️ Found ${stuckRides.length} hoarded rides. Releasing drivers...`,
      );

      for (const ride of stuckRides) {
        // 2. The "Kick" Logic: Unassign driver and return to pool
        // We set it back to PENDING so the 'handleDispatchWaves' can find a NEW driver.
        const { error: updateError } = await this.supabase
          .from('rides')
          .update({
            status: 'PENDING', // Back to pool
            driver_id: null, // Remove the hoarder
            dispatch_batch: 1, // Reset search radius
            updated_at: new Date().toISOString(),
            last_offer_sent_at: new Date().toISOString(), // Reset timer so dispatcher picks it up
            note: 'Previous driver unresponsive. Auto-reassigned.',
          })
          .eq('id', ride.id);

        if (updateError) {
          this.logger.error(`Failed to unassign ride ${ride.id}`, updateError);
          continue;
        }

        // 3. Notify the Passenger
        const { data: passenger } = await this.supabase
          .from('profiles')
          .select('push_token')
          .eq('id', ride.passenger_id)
          .single();

        if (passenger?.push_token) {
          await this.sendPushNotification(
            passenger.push_token,
            'Driver Unresponsive 🐢',
            "We detected your driver wasn't moving. We are finding you a new one!",
          );
        }

        // 4. Notify the Kicked Driver (Optional)
        const { data: driver } = await this.supabase
          .from('profiles')
          .select('push_token')
          .eq('id', ride.driver_id)
          .single();

        if (driver?.push_token) {
          await this.sendPushNotification(
            driver.push_token,
            'Ride Unassigned 🚫',
            'You were unassigned due to inactivity.',
          );
        }
      }
    } catch (err) {
      this.handleError(err, 'handleStuckAcceptedRides');
    }
  }

  // ✅ Update signature to accept 'fare'
  private async sendDriverPush(
    driverIds: string[],
    rideId: string,
    fare: number,
  ) {
    this.logger.log(`Attempting to notify drivers: ${driverIds.join(', ')}`);

    if (!driverIds || driverIds.length === 0) return;

    try {
      const { data: drivers, error } = await this.supabase
        .from('profiles')
        .select('id, push_token')
        .in('id', driverIds);

      if (error) {
        this.logger.error('❌ Database Error fetching tokens:', error);
        return;
      }

      const validDrivers = drivers?.filter(
        (d) => d.push_token && d.push_token.startsWith('ExponentPushToken'),
      );

      if (!validDrivers || validDrivers.length === 0) {
        // Quietly return if no tokens found
        return;
      }

      this.logger.log(`📲 Sending to ${validDrivers.length} devices...`);

      const messages = validDrivers.map((driver) => ({
        to: driver.push_token,
        sound: 'default',
        title: 'New Ride Request 🚖',
        // ✅ Use the fare variable here
        body: `Earn ${fare} DZD - Tap to accept!`,
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
    } catch (err) {
      this.logger.error('❌ CRASH in sendDriverPush:', err);
    }
  }

  // In rides.service.ts

  @Cron('*/10 * * * * *')
  async handleDispatchWaves() {
    const twentySecondsAgo = new Date(Date.now() - 20 * 1000).toISOString();

    // 1. Fetch stuck rides
    const { data: stuckRides, error } = await this.supabase
      .from('rides')
      .select('id, dispatch_batch, fare_estimate')
      .eq('status', 'PENDING')
      .lt('last_offer_sent_at', twentySecondsAgo)
      .limit(50);

    if (error) {
      this.logger.error('❌ Error fetching dispatch waves:', error);
      return;
    }

    if (!stuckRides || stuckRides.length === 0) return;

    this.logger.log(`🌊 Processing waves for ${stuckRides.length} rides...`);

    // 2. Process each ride individually
    for (const ride of stuckRides) {
      try {
        const nextBatch =
          ride.dispatch_batch >= 3 ? 3 : ride.dispatch_batch + 1;

        // A. Update Batch Count
        await this.supabase
          .from('rides')
          .update({
            dispatch_batch: nextBatch,
            last_offer_sent_at: new Date().toISOString(),
          })
          .eq('id', ride.id);

        // B. Find Drivers
        const { data: offeredDriverIds, error: rpcError } =
          await this.supabase.rpc('find_and_offer_ride', {
            target_ride_id: ride.id,
          });

        if (rpcError) {
          this.logger.error(
            `❌ RPC 'find_and_offer_ride' failed for ride ${ride.id}:`,
            rpcError,
          );
          continue;
        }

        // C. Send Notification
        if (offeredDriverIds && offeredDriverIds.length > 0) {
          // Add 'await' here so we can see the logs immediately
          await this.sendDriverPush(
            offeredDriverIds,
            ride.id,
            ride.fare_estimate,
          );
        } else {
          this.logger.warn(
            `⚠️ No drivers found for ride ${ride.id} (Batch ${nextBatch})`,
          );
        }
      } catch (err) {
        this.logger.error(`❌ Failed to process ride ${ride.id}`, err);
      }
    }
  }
  @Cron(CronExpression.EVERY_MINUTE)
  async activateScheduledRides() {
    const now = new Date();
    // Look for rides scheduled within the next 20 minutes
    const twentyMinutesFromNow = new Date(
      now.getTime() + 20 * 60000,
    ).toISOString();

    try {
      const { data: ridesToActivate, error } = await this.supabase
        .from('rides')
        .select('*')
        .eq('status', 'SCHEDULED')
        .lte('scheduled_time', twentyMinutesFromNow)
        .limit(50);

      if (error) {
        this.logger.error('Error fetching scheduled rides:', error);
        return;
      }

      if (!ridesToActivate || ridesToActivate.length === 0) return;

      this.logger.log(`Found ${ridesToActivate.length} rides to activate.`);

      await Promise.all(
        ridesToActivate.map(async (ride) => {
          try {
            // A. Update Status to PENDING (Live)
            // We update 'last_offer_sent_at' and 'updated_at' to NOW so the "Stale Check" cron job
            // sees this as a fresh ride and doesn't cancel it immediately.
            const { error: updateError } = await this.supabase
              .from('rides')
              .update({
                status: 'PENDING',
                updated_at: new Date().toISOString(),
                last_offer_sent_at: new Date().toISOString(),
                dispatch_batch: 1,
              })
              .eq('id', ride.id);

            if (updateError) throw updateError;

            // B. NOTIFY PASSENGER (Always notify that ride is starting)
            const { data: passenger } = await this.supabase
              .from('profiles')
              .select('push_token')
              .eq('id', ride.passenger_id)
              .single();

            if (passenger?.push_token) {
              await this.sendPushNotification(
                passenger.push_token,
                'Ride Activating ⏰',
                ride.driver_id
                  ? 'Your driver is getting ready to head to you.'
                  : 'We are now looking for a driver for your scheduled ride.',
              );
            }

            // C. HANDLE DRIVER LOGIC (The Critical Fix)
            if (ride.driver_id) {
              // CASE 1: Driver Already Assigned (Pre-booked)
              // We DO NOT call find_and_offer_ride here.
              // This prevents creating a "pending offer" that would expire and kill the ride.

              // Instead, just notify the assigned driver to start moving.
              const { data: driver } = await this.supabase
                .from('profiles')
                .select('push_token')
                .eq('id', ride.driver_id)
                .single();

              if (driver?.push_token) {
                await this.sendPushNotification(
                  driver.push_token,
                  'Scheduled Ride Starting 🏁',
                  'Your scheduled passenger is waiting. Please head to pickup.',
                );
              }
              this.logger.log(
                `Activated pre-assigned ride ${ride.id} for driver ${ride.driver_id}`,
              );
            } else {
              // CASE 2: No Driver Assigned Yet
              // Only NOW do we trigger the marketplace to find a driver
              this.supabase
                .rpc('find_and_offer_ride', {
                  target_ride_id: ride.id,
                })
                .then(({ error }) => {
                  if (error)
                    this.logger.error(
                      `Failed to dispatch activated ride ${ride.id}`,
                      error,
                    );
                });
              this.logger.log(
                `Activated unassigned ride ${ride.id}, searching for drivers...`,
              );
            }
          } catch (innerErr) {
            this.logger.error(`Failed to activate ride ${ride.id}`, innerErr);
          }
        }),
      );
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
  async cancelRideByPassenger(
    rideId: string,
    passengerId: string,
    reason: string,
  ) {
    // 1. Update DB
    const { data: ride } = await this.supabase
      .from('rides')
      .update({
        status: 'CANCELLED',
        cancellation_reason: reason,
        updated_at: new Date().toISOString(),
      })
      .eq('id', rideId)
      .eq('passenger_id', passengerId) // Security check
      .select('driver_id') // Get the driver ID
      .single();

    // 2. Notify Driver (if one was assigned)
    if (ride?.driver_id) {
      const { data: driver } = await this.supabase
        .from('profiles')
        .select('push_token')
        .eq('id', ride.driver_id)
        .single();

      if (driver?.push_token) {
        await this.sendPushNotification(
          driver.push_token,
          'Ride Cancelled 🚫',
          'Passenger cancelled the request. You are back online.',
        );
      }
    }

    return { success: true };
  }
}
