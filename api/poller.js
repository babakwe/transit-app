// api/poller.js
// Cron: every 2 min. Polls MTA VehicleMonitoring for tracked stops.
// Detects arrivals by matching vehicle next-stop to monitored stops.
// Writes to bus_arrivals table with aimed_arrival + recorded_at for adherence.
// adherence_seconds = recorded_at - aimed_arrival (positive = late, negative = early)

const MTA_KEY          = process.env.MTA_API_KEY;
const SUPABASE_URL     = process.env.SUPABASE_URL;
const SUPABASE_KEY     = process.env.SUPABASE_SERVICE_KEY;

// How close (meters) a bus must be to the stop lat/lng to count as arrived
const ARRIVAL_THRESHOLD_M = 50;

// Stops to monitor — add lat/lng so we can do proximity detection
const MONITOR_STOPS = [
  { stopId: '100080', name: 'BOSTON RD/E GUN HILL RD',      routes: ['Bx28','Bx38'],        lat: 40.8744, lng: -73.8698 },
  { stopId: '100998', name: 'FORDHAM RD/GRAND CONCOURSE',   routes: ['Bx12','Bx17','Bx19'], lat: 40.8598, lng: -73.8902 },
  { stopId: '101002', name: 'FORDHAM RD/JEROME AV',         routes: ['Bx12','Bx17'],        lat: 40.8601, lng: -73.8975 },
  { stopId: '103042', name: 'VALENTINE AV/E 192 ST',        routes: ['Bx28'],               lat: 40.8593, lng: -73.8953 },
  { stopId: '200987', name: 'WHITE PLAINS RD/GUN HILL RD',  routes: ['Bx39','Bx41'],        lat: 40.8769, lng: -73.8659 },
  { stopId: '301036', name: 'PELHAM PKWY/WHITE PLAINS RD',  routes: ['Bx28','Bx39'],        lat: 40.8598, lng: -73.8545 },
  { stopId: '400721', name: 'JEROME AV/BURNSIDE AV',        routes: ['Bx13','Bx41'],        lat: 40.8480, lng: -73.9107 },
  { stopId: '500814', name: 'TREMONT AV/GRAND CONCOURSE',   routes: ['Bx1','Bx2'],          lat: 40.8493, lng: -73.9069 },
  { stopId: '200640', name: 'E GUN HILL RD/HULL AV',        routes: ['Bx28','Bx38'],        lat: 40.8761, lng: -73.8671 },
  { stopId: '104026', name: 'E 233 ST/WHITE PLAINS RD',     routes: ['Bx39','Bx41'],        lat: 40.8981, lng: -73.8534 },
];

// Build a lookup map: stopId → stop object for fast matching
const STOP_BY_ID = Object.fromEntries(MONITOR_STOPS.map(s => [s.stopId, s]));

// All unique line refs to poll
const ALL_ROUTES = [...new Set(MONITOR_STOPS.flatMap(s => s.routes))];

function agencyPrefix(route) {
  const r = route.toUpperCase();
  if (r.startsWith('BXM') || r.startsWith('QM') || r.startsWith('X')) return 'MTA BC_';
  return 'MTA NYCT_';
}

// Haversine distance in meters between two lat/lng points
function distanceMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 +
            Math.cos(lat1 * Math.PI/180) * Math.cos(lat2 * Math.PI/180) * Math.sin(dLng/2)**2;
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

function extractArrivalRecord(mvj, route) {
  const loc = mvj.VehicleLocation;
  if (!loc?.Longitude || !loc?.Latitude) return null;

  const busLat = parseFloat(loc.Latitude);
  const busLng = parseFloat(loc.Longitude);
  // recorded_at = the timestamp the MTA recorded this vehicle position
  const recordedAt = loc.RecordedAtTime ? new Date(loc.RecordedAtTime) : new Date();

  // Next stop from OnwardCalls
  const calls = mvj.OnwardCalls?.OnwardCall;
  if (!calls) return null;
  const nextCall = Array.isArray(calls) ? calls[0] : calls;
  if (!nextCall) return null;

  const nextStopId = nextCall.StopPointRef;
  const stop = STOP_BY_ID[nextStopId];
  if (!stop) return null; // not a stop we're monitoring

  // Proximity check: is bus actually at the stop?
  const dist = distanceMeters(busLat, busLng, stop.lat, stop.lng);
  if (dist > ARRIVAL_THRESHOLD_M) return null;

  const aimedArrival    = nextCall.AimedArrivalTime    ? new Date(nextCall.AimedArrivalTime)    : null;
  const expectedArrival = nextCall.ExpectedArrivalTime ? new Date(nextCall.ExpectedArrivalTime) : null;

  // adherence: how many seconds late (positive) or early (negative)
  const adherenceSec = aimedArrival
    ? Math.round((recordedAt - aimedArrival) / 1000)
    : null;

  const routeName = Array.isArray(mvj.PublishedLineName) ? mvj.PublishedLineName[0] : mvj.PublishedLineName || route;
  const headsign  = Array.isArray(mvj.DestinationName)   ? mvj.DestinationName[0]  : mvj.DestinationName || '';

  return {
    stop_id:           stop.stopId,
    stop_name:         stop.name,
    route:             routeName,
    headsign:          headsign.slice(0, 80),
    vehicle_id:        mvj.VehicleRef || null,
    direction_ref:     mvj.DirectionRef ?? null,
    trip_id:           mvj.FramedVehicleJourneyRef?.DatedVehicleJourneyRef ?? null,
    // Core adherence columns
    recorded_at:       recordedAt.toISOString(),
    aimed_arrival:     aimedArrival    ? aimedArrival.toISOString()    : null,
    expected_arrival:  expectedArrival ? expectedArrival.toISOString() : null,
    adherence_seconds: adherenceSec,
    proximity_meters:  Math.round(dist),
    // Context
    day_of_week:       recordedAt.getDay(),
    hour_of_day:       recordedAt.getHours(),
  };
}

async function writeArrivals(rows) {
  if (!rows.length) return 0;
  const res = await fetch(`${SUPABASE_URL}/rest/v1/bus_arrivals`, {
    method: 'POST',
    headers: {
      'Content-Type':  'application/json',
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'apikey':        SUPABASE_KEY,
      // Upsert on vehicle+stop+trip to avoid duplicates within same poll window
      'Prefer':        'resolution=ignore-duplicates,return=minimal',
    },
    body: JSON.stringify(rows),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Supabase write failed: ${err}`);
  }
  return rows.length;
}

export default async function handler(req, res) {
  if (req.method !== 'GET') return res.status(405).end();
  const start = Date.now();

  const arrivals = [];

  // Poll routes in batches of 3 to avoid rate limits
  for (let i = 0; i < ALL_ROUTES.length; i += 3) {
    const batch = ALL_ROUTES.slice(i, i + 3);
    const results = await Promise.allSettled(batch.map(r => fetchVehicles(r)));

    for (let j = 0; j < results.length; j++) {
      const result = results[j];
      if (result.status === 'rejected') {
        console.error('poller fetch error', batch[j], result.reason?.message);
        continue;
      }
      for (const mvj of result.value) {
        const record = extractArrivalRecord(mvj, batch[j]);
        if (record) arrivals.push(record);
      }
    }

    if (i + 3 < ALL_ROUTES.length) {
      await new Promise(r => setTimeout(r, 400));
    }
  }

  let written = 0;
  try {
    written = await writeArrivals(arrivals);
  } catch (e) {
    console.error('poller write error', e.message);
    return res.status(500).json({ ok: false, error: e.message });
  }

  return res.status(200).json({
    ok:      true,
    written,
    elapsed: Date.now() - start,
    routes:  ALL_ROUTES.length,
  });
}
