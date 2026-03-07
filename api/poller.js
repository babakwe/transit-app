// api/poller.js
// Cron: every 2 min. Polls MTA VehicleMonitoring for tracked stops.
// Detects arrivals by matching vehicle next-stop to monitored stops.
// Writes to bus_arrivals AND prediction_audit (self-improving loop).
//
// Self-improving loop:
//   Phase 1 (every poll): log each bus's predicted arrival at each stop it's approaching
//   Phase 2 (every poll): reconcile past predictions — fill in actual_arrival + error_seconds
//   Phase 3 (nightly, future): aggregate errors into prediction_bias table by route/stop/hour/day
//   Phase 4 (query time, future): apply bias corrections to live MTA predictions before showing user

const MTA_KEY = process.env.MTA_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

// How close (meters) a bus must be to count as arrived
const ARRIVAL_THRESHOLD_M = 50;
// How far away (meters) to start logging a prediction
const PREDICTION_HORIZON_M = 800; // ~0.5 mile out — when user needs "leave home" info

const MONITOR_STOPS = [
  { stopId: '100080', name: 'BOSTON RD/E GUN HILL RD',         routes: ['Bx28','Bx38'], lat: 40.8744, lng: -73.8698 },
  { stopId: '100998', name: 'FORDHAM RD/GRAND CONCOURSE',      routes: ['Bx12','Bx17','Bx19'], lat: 40.8598, lng: -73.8902 },
  { stopId: '101002', name: 'FORDHAM RD/JEROME AV',            routes: ['Bx12','Bx17'], lat: 40.8601, lng: -73.8975 },
  { stopId: '103042', name: 'VALENTINE AV/E 192 ST',           routes: ['Bx28'], lat: 40.8593, lng: -73.8953 },
  { stopId: '200987', name: 'WHITE PLAINS RD/GUN HILL RD',     routes: ['Bx39','Bx41'], lat: 40.8769, lng: -73.8659 },
  { stopId: '301036', name: 'PELHAM PKWY/WHITE PLAINS RD',     routes: ['Bx28','Bx39'], lat: 40.8598, lng: -73.8545 },
  { stopId: '400721', name: 'JEROME AV/BURNSIDE AV',           routes: ['Bx13','Bx41'], lat: 40.8480, lng: -73.9107 },
  { stopId: '500814', name: 'TREMONT AV/GRAND CONCOURSE',      routes: ['Bx1','Bx2'], lat: 40.8493, lng: -73.9069 },
  { stopId: '200640', name: 'E GUN HILL RD/HULL AV',           routes: ['Bx28','Bx38'], lat: 40.8761, lng: -73.8671 },
  { stopId: '104026', name: 'E 233 ST/WHITE PLAINS RD',        routes: ['Bx39','Bx41'], lat: 40.8981, lng: -73.8534 },
];

const STOP_BY_ID = Object.fromEntries(MONITOR_STOPS.map(s => [s.stopId, s]));
const ALL_ROUTES = [...new Set(MONITOR_STOPS.flatMap(s => s.routes))];

function agencyPrefix(route) {
  const r = route.toUpperCase();
  if (r.startsWith('BXM') || r.startsWith('QM') || r.startsWith('X')) return 'MTA BC_';
  return 'MTA NYCT_';
}

function distanceMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1 * Math.PI/180) * Math.cos(lat2 * Math.PI/180) * Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

async function fetchVehicles(route) {
  const lineRef = encodeURIComponent(agencyPrefix(route) + route.toUpperCase());
  const url = `https://bustime.mta.info/api/siri/vehicle-monitoring.json` +
    `?key=${MTA_KEY}&LineRef=${lineRef}&VehicleMonitoringDetailLevel=calls`;
  const data = await fetch(url).then(r => r.json());
  const deliveries = data?.Siri?.ServiceDelivery?.VehicleMonitoringDelivery;
  if (!Array.isArray(deliveries)) return [];
  return deliveries
    .flatMap(d => d.VehicleActivity ?? [])
    .map(a => a.MonitoredVehicleJourney)
    .filter(Boolean);
}

// Returns { arrival: record|null, prediction: record|null }
// arrival = bus is AT the stop (within 50m)
// prediction = bus is approaching the stop (within 800m) — log to audit
function extractRecords(mvj, route) {
  const loc = mvj.VehicleLocation;
  if (!loc?.Longitude || !loc?.Latitude) return { arrival: null, prediction: null };
  const busLat = parseFloat(loc.Latitude);
  const busLng = parseFloat(loc.Longitude);
  const recordedAt = loc.RecordedAtTime ? new Date(loc.RecordedAtTime) : new Date();

  const calls = mvj.OnwardCalls?.OnwardCall;
  if (!calls) return { arrival: null, prediction: null };
  const nextCall = Array.isArray(calls) ? calls[0] : calls;
  if (!nextCall) return { arrival: null, prediction: null };

  const nextStopId = nextCall.StopPointRef;
  const stop = STOP_BY_ID[nextStopId];
  if (!stop) return { arrival: null, prediction: null };

  const dist = distanceMeters(busLat, busLng, stop.lat, stop.lng);
  const aimedArrival = nextCall.AimedArrivalTime ? new Date(nextCall.AimedArrivalTime) : null;
  const expectedArrival = nextCall.ExpectedArrivalTime ? new Date(nextCall.ExpectedArrivalTime) : null;
  const routeName = Array.isArray(mvj.PublishedLineName) ? mvj.PublishedLineName[0] : mvj.PublishedLineName || route;
  const headsign = Array.isArray(mvj.DestinationName) ? mvj.DestinationName[0] : mvj.DestinationName || '';
  const vehicleId = mvj.VehicleRef || null;
  const tripId = mvj.FramedVehicleJourneyRef?.DatedVehicleJourneyRef ?? null;
  const directionRef = mvj.DirectionRef ?? null;

  let arrival = null;
  let prediction = null;

  // Bus is AT the stop — write arrival record
  if (dist <= ARRIVAL_THRESHOLD_M) {
    const adherenceSec = aimedArrival ? Math.round((recordedAt - aimedArrival) / 1000) : null;
    arrival = {
      stop_id: stop.stopId, stop_name: stop.name, route: routeName,
      headsign: headsign.slice(0, 80), vehicle_id: vehicleId,
      direction_ref: directionRef, trip_id: tripId,
      recorded_at: recordedAt.toISOString(),
      aimed_arrival: aimedArrival?.toISOString() ?? null,
      expected_arrival: expectedArrival?.toISOString() ?? null,
      adherence_seconds: adherenceSec,
      proximity_meters: Math.round(dist),
      day_of_week: recordedAt.getDay(),
      hour_of_day: recordedAt.getHours(),
    };
  }

  // Bus is approaching within prediction horizon — log prediction for audit
  if (dist <= PREDICTION_HORIZON_M && expectedArrival) {
    prediction = {
      route_id: routeName,
      stop_id: stop.stopId,
      stop_name: stop.name,
      direction: directionRef,
      vehicle_id: vehicleId,
      trip_id: tripId,
      predicted_at: new Date().toISOString(),
      predicted_arrival: expectedArrival.toISOString(),
      // actual_arrival and error_seconds filled in by reconciler below
      hour_of_day: recordedAt.getHours(),
      day_of_week: recordedAt.getDay(),
      month: recordedAt.getMonth() + 1,
      is_weekend: recordedAt.getDay() === 0 || recordedAt.getDay() === 6,
      distance_at_prediction: Math.round(dist),
      reconciled: false,
    };
  }

  return { arrival, prediction };
}

// Reconciler: find unreconciled predictions where we now have actual arrival data.
// For each unreconciled prediction, check if that vehicle/trip has an arrival record.
// If yes, fill in actual_arrival and error_seconds.
async function reconcilePredictions() {
  // Get recent unreconciled predictions (last 2 hours)
  const cutoff = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/prediction_audit?reconciled=eq.false&predicted_at=gte.${cutoff}&select=id,vehicle_id,trip_id,stop_name,predicted_arrival`,
    { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}`, 'apikey': SUPABASE_KEY } }
  );
  if (!res.ok) return 0;
  const pending = await res.json();
  if (!pending.length) return 0;

  // Get recent arrivals that can resolve these predictions
  const arrRes = await fetch(
    `${SUPABASE_URL}/rest/v1/bus_arrivals?observed_at=gte.${cutoff}&select=vehicle_id,trip_id,stop_name,recorded_at`,
    { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}`, 'apikey': SUPABASE_KEY } }
  );
  if (!arrRes.ok) return 0;
  const arrivals = await arrRes.json();

  // Build lookup: vehicle_id+trip_id+stop_name → recorded_at
  const arrivalMap = {};
  for (const a of arrivals) {
    const key = `${a.vehicle_id}|${a.trip_id}|${a.stop_name}`;
    if (!arrivalMap[key]) arrivalMap[key] = a.recorded_at;
  }

  // Reconcile matches
  const updates = [];
  for (const p of pending) {
    const key = `${p.vehicle_id}|${p.trip_id}|${p.stop_name}`;
    const actualArrival = arrivalMap[key];
    if (!actualArrival) continue;
    const errorSec = Math.round((new Date(actualArrival) - new Date(p.predicted_arrival)) / 1000);
    updates.push({ id: p.id, actual_arrival: actualArrival, error_seconds: errorSec, reconciled: true, reconciled_at: new Date().toISOString() });
  }

  if (!updates.length) return 0;

  // Patch each reconciled record
  await Promise.allSettled(updates.map(u =>
    fetch(`${SUPABASE_URL}/rest/v1/prediction_audit?id=eq.${u.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${SUPABASE_KEY}`, 'apikey': SUPABASE_KEY },
      body: JSON.stringify({ actual_arrival: u.actual_arrival, error_seconds: u.error_seconds, reconciled: u.reconciled, reconciled_at: u.reconciled_at }),
    })
  ));

  return updates.length;
}

async function supabaseInsert(table, rows, preferHeader = 'resolution=ignore-duplicates,return=minimal') {
  if (!rows.length) return 0;
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'apikey': SUPABASE_KEY,
      'Prefer': preferHeader,
    },
    body: JSON.stringify(rows),
  });
  if (!res.ok) { const err = await res.text(); throw new Error(`${table} write failed: ${err}`); }
  return rows.length;
}

export default async function handler(req, res) {
  if (req.method !== 'GET') return res.status(405).end();
  const start = Date.now();

  const arrivals = [];
  const predictions = [];

  // Poll all routes in batches of 3
  for (let i = 0; i < ALL_ROUTES.length; i += 3) {
    const batch = ALL_ROUTES.slice(i, i + 3);
    const results = await Promise.allSettled(batch.map(r => fetchVehicles(r)));
    for (let j = 0; j < results.length; j++) {
      if (results[j].status === 'rejected') { console.error('fetch error', batch[j], results[j].reason?.message); continue; }
      for (const mvj of results[j].value) {
        const { arrival, prediction } = extractRecords(mvj, batch[j]);
        if (arrival) arrivals.push(arrival);
        if (prediction) predictions.push(prediction);
      }
    }
    if (i + 3 < ALL_ROUTES.length) await new Promise(r => setTimeout(r, 400));
  }

  // Write arrivals and predictions in parallel
  let written = 0, predicted = 0, reconciled = 0;
  const [arrResult, predResult, recResult] = await Promise.allSettled([
    supabaseInsert('bus_arrivals', arrivals),
    supabaseInsert('prediction_audit', predictions),
    reconcilePredictions(),
  ]);

  if (arrResult.status === 'fulfilled') written = arrResult.value;
  else console.error('arrivals write error', arrResult.reason?.message);

  if (predResult.status === 'fulfilled') predicted = predResult.value;
  else console.error('predictions write error', predResult.reason?.message);

  if (recResult.status === 'fulfilled') reconciled = recResult.value;
  else console.error('reconciler error', recResult.reason?.message);

  return res.status(200).json({
    ok: true,
    written,       // arrivals logged this poll
    predicted,     // new predictions logged
    reconciled,    // predictions matched to actual arrivals
    elapsed: Date.now() - start,
    routes: ALL_ROUTES.length,
  });
}
