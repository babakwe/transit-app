// api/widget.js
// GET /api/widget?stop=STOP_ID&route=Bx28
// The core TownTrip endpoint — powers the "Leave in X min" experience.
//
// Response shape:
// {
//   route: "Bx28",
//   headsign: "Fordham Rd",
//   stop_name: "BOSTON RD/E GUN HILL RD",
//   next_bus: {
//     expected_arrival: "2026-03-07T14:23:00Z",
//     mins_away: 8,              // bus ETA from now
//     distance_m: 620,           // how far the bus is right now
//     bias_correction_sec: -45,  // how much we adjusted MTA's prediction
//     adjusted_arrival: "...",   // our corrected ETA
//     adjusted_mins_away: 9,     // corrected minutes
//   },
//   leave_in_mins: 6,            // when YOU should leave (= adjusted_mins_away - walk_mins)
//   walk_mins: 3,                // default walk time to stop (future: personalized)
//   subsequent_buses: [ ... ],   // next 2 buses after this one
//   last_updated: "...",
//   data_age_secs: 45            // how stale is our live data
// }

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const MTA_KEY     = process.env.MTA_API_KEY;

// Default walk time in minutes — future: pull from user profile
const DEFAULT_WALK_MINS = 3;

function agencyPrefix(route) {
  const r = route.toUpperCase();
  return (r.startsWith('BXM') || r.startsWith('QM') || r.startsWith('X'))
    ? 'MTA BC_' : 'MTA NYCT_';
}

// Fetch live arrivals from MTA BusTime for a stop + route
async function fetchLiveArrivals(stopId, route) {
  const lineRef   = encodeURIComponent(agencyPrefix(route) + route.toUpperCase());
  const monRef    = encodeURIComponent(`MTA_${stopId}`);
  const url = `https://bustime.mta.info/api/siri/stop-monitoring.json`
    + `?key=${MTA_KEY}&LineRef=${lineRef}&MonitoringRef=${monRef}`
    + `&MaximumStopVisits=3&StopMonitoringDetailLevel=minimum`;

  const res  = await fetch(url);
  const data = await res.json();

  const visits = data?.Siri?.ServiceDelivery
    ?.StopMonitoringDelivery?.[0]?.MonitoredStopVisit ?? [];

  return visits.map(v => {
    const mvj  = v.MonitoredVehicleJourney;
    const call = mvj?.MonitoredCall;
    const expected   = call?.ExpectedArrivalTime  ? new Date(call.ExpectedArrivalTime)  : null;
    const aimed      = call?.AimedArrivalTime     ? new Date(call.AimedArrivalTime)     : null;
    const headsign   = Array.isArray(mvj?.DestinationName)
      ? mvj.DestinationName[0] : mvj?.DestinationName ?? '';
    const distanceM  = call?.Extensions?.Distances?.PresentableDistance ?? null;
    const stopsAway  = call?.Extensions?.Distances?.StopsFromCall ?? null;

    return {
      vehicle_id:       mvj?.VehicleRef ?? null,
      trip_id:          mvj?.FramedVehicleJourneyRef?.DatedVehicleJourneyRef ?? null,
      headsign:         headsign,
      aimed_arrival:    aimed,
      expected_arrival: expected,
      distance_label:   distanceM,   // "0.3 miles away" from MTA
      stops_away:       stopsAway,
      fetched_at:       new Date(),
    };
  }).filter(b => b.expected_arrival);
}

// Look up our learned bias correction for this route/stop/hour/day
async function getBiasCorrection(route, stopName, hourOfDay, dayOfWeek) {
  const params = new URLSearchParams({
    route_id:    `eq.${route}`,
    stop_name:   `eq.${stopName}`,
    hour_of_day: `eq.${hourOfDay}`,
    day_of_week: `eq.${dayOfWeek}`,
    select:      'bias_correction_sec,confidence,sample_count',
    limit:       '1',
  });

  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/prediction_bias?${params}`,
      { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}`, 'apikey': SUPABASE_KEY } }
    );
    const rows = await res.json();
    return rows?.[0] ?? null;
  } catch {
    return null;
  }
}

// Get stop name from our monitored stops list (or fallback)
async function getStopName(stopId) {
  // Try to find the stop name from bus_arrivals history
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/bus_arrivals?stop_id=eq.${stopId}&select=stop_name&limit=1`,
      { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}`, 'apikey': SUPABASE_KEY } }
    );
    const rows = await res.json();
    return rows?.[0]?.stop_name ?? null;
  } catch {
    return null;
  }
}

export default async function handler(req, res) {
  // CORS — widget will be embedded in other pages eventually
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).end();

  const { stop, route, walk_mins } = req.query;
  if (!stop || !route) {
    return res.status(400).json({ error: 'stop and route are required' });
  }

  const walkMins = parseInt(walk_mins ?? DEFAULT_WALK_MINS, 10);
  const now      = new Date();
  const hourOfDay  = now.getHours();
  const dayOfWeek  = now.getDay();

  try {
    // Fetch live MTA data and stop metadata in parallel
    const [buses, stopName] = await Promise.all([
      fetchLiveArrivals(stop, route),
      getStopName(stop),
    ]);

    if (!buses.length) {
      return res.status(200).json({
        route,
        stop_id:   stop,
        stop_name: stopName,
        status:    'no_buses',
        message:   'No buses found for this route at this stop right now.',
        last_updated: now.toISOString(),
      });
    }

    // Get our learned bias correction (may be null if not enough data yet)
    const bias = await getBiasCorrection(route, stopName, hourOfDay, dayOfWeek);
    const correctionSec = bias?.bias_correction_sec ?? 0;

    // Build enriched bus objects
    const enrichedBuses = buses.map(bus => {
      const rawMinsAway     = Math.round((bus.expected_arrival - now) / 60000);
      const adjustedArrival = new Date(bus.expected_arrival.getTime() + correctionSec * 1000);
      const adjustedMins    = Math.round((adjustedArrival - now) / 60000);
      const leaveInMins     = Math.max(0, adjustedMins - walkMins);

      return {
        vehicle_id:           bus.vehicle_id,
        headsign:             bus.headsign,
        aimed_arrival:        bus.aimed_arrival?.toISOString() ?? null,
        expected_arrival:     bus.expected_arrival.toISOString(),
        adjusted_arrival:     adjustedArrival.toISOString(),
        mins_away:            rawMinsAway,
        adjusted_mins_away:   adjustedMins,
        leave_in_mins:        leaveInMins,
        distance_label:       bus.distance_label,
        stops_away:           bus.stops_away,
        bias_correction_sec:  correctionSec,
        bias_confidence:      bias?.confidence ?? null,
        bias_sample_count:    bias?.sample_count ?? 0,
      };
    });

    // Sort by adjusted arrival
    enrichedBuses.sort((a, b) => a.adjusted_mins_away - b.adjusted_mins_away);

    const nextBus = enrichedBuses[0];

    return res.status(200).json({
      route,
      stop_id:            stop,
      stop_name:          stopName,
      headsign:           nextBus.headsign,
      // The headline numbers — what the UI shows big
      leave_in_mins:      nextBus.leave_in_mins,
      next_bus_mins:      nextBus.adjusted_mins_away,
      walk_mins:          walkMins,
      // Full next bus detail
      next_bus:           nextBus,
      // Up to 2 more buses after this one
      subsequent_buses:   enrichedBuses.slice(1, 3).map(b => ({
        mins_away:        b.adjusted_mins_away,
        leave_in_mins:    b.leave_in_mins,
        headsign:         b.headsign,
        distance_label:   b.distance_label,
      })),
      // Meta
      bias_active:        correctionSec !== 0,
      last_updated:       now.toISOString(),
      hour_of_day:        hourOfDay,
      day_of_week:        dayOfWeek,
    });

  } catch (err) {
    console.error('widget error', err.message);
    return res.status(500).json({ error: err.message });
  }
}
