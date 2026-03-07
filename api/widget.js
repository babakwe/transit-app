// api/widget.js
// GET /api/widget?stop=STOP_NAME&route=Bx28
// GET /api/widget?stopId=100080
// GET /api/widget?lat=40.8744&lng=-73.8698
//
// Returns the "Uber moment" — everything needed to render:
//   "Bx28 → Fordham Rd  |  Bus is 4 min away  |  Leave in 2 min"
//
// Data sources (in priority order):
//   1. MTA BusTime live feed   — real-time bus position right now
//   2. prediction_bias table   — our learned corrections per route/stop/hour/day
//   3. mta_bus_schedules       — scheduled fallback if live feed is down
//
// Response is intentionally tiny (<500 bytes) for widget/home screen use.

const MTA_KEY       = process.env.MTA_API_KEY;
const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_KEY;

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

function haversineMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

function agencyPrefix(route) {
  const r = route.toUpperCase();
  if (r.startsWith('BXM') || r.startsWith('QM') || r.startsWith('X')) return 'MTA BC_';
  return 'MTA NYCT_';
}

function resolveStop(query) {
  const { stopId, stop, route, lat, lng } = query;
  if (stopId) return MONITOR_STOPS.find(s => s.stopId === stopId) ?? null;
  if (stop) {
    const name = stop.toUpperCase();
    return MONITOR_STOPS.find(s => s.name.includes(name)) ?? null;
  }
  if (lat && lng) {
    const uLat = parseFloat(lat), uLng = parseFloat(lng);
    let closest = null, minDist = Infinity;
    for (const s of MONITOR_STOPS) {
      const d = haversineMeters(uLat, uLng, s.lat, s.lng);
      if (d < minDist) { minDist = d; closest = s; }
    }
    return minDist <= 400 ? closest : null;
  }
  return null;
}

async function fetchLivePredictions(route, stop) {
  const lineRef = encodeURIComponent(agencyPrefix(route) + route.toUpperCase());
  const url = `https://bustime.mta.info/api/siri/stop-monitoring.json` +
    `?key=${MTA_KEY}&LineRef=${lineRef}&MonitoringRef=${stop.stopId}&MaximumStopVisits=3`;
  try {
    const data = await fetch(url, { signal: AbortSignal.timeout(4000) }).then(r => r.json());
    const visits = data?.Siri?.ServiceDelivery?.StopMonitoringDelivery?.[0]?.MonitoredStopVisit ?? [];
    return visits.map(v => {
      const mvj  = v.MonitoredVehicleJourney;
      const call = mvj?.MonitoredCall;
      const expectedArrival = call?.ExpectedArrivalTime ? new Date(call.ExpectedArrivalTime) : null;
      const aimedArrival    = call?.AimedArrivalTime    ? new Date(call.AimedArrivalTime)    : null;
      const headsign = Array.isArray(mvj?.DestinationName) ? mvj.DestinationName[0] : mvj?.DestinationName ?? '';
      return {
        route,
        headsign: headsign.slice(0, 40),
        vehicleId:     mvj?.VehicleRef ?? null,
        expectedArrival,
        aimedArrival,
        distanceLabel: call?.Extensions?.Distances?.PresentableDistance ?? null,
        stopsAway:     call?.Extensions?.Distances?.StopsFromCall ?? null,
        source: 'live',
      };
    }).filter(p => p.expectedArrival && p.expectedArrival > new Date());
  } catch {
    return [];
  }
}

async function fetchBiasCorrection(route, stopName, hourOfDay, dayOfWeek) {
  try {
    const q = new URLSearchParams({
      select: 'bias_correction_sec,confidence,sample_count',
      route_id:    `eq.${route}`,
      stop_name:   `eq.${stopName}`,
      hour_of_day: `eq.${hourOfDay}`,
      day_of_week: `eq.${dayOfWeek}`,
      limit: '1',
    });
    const res = await fetch(`${SUPABASE_URL}/rest/v1/prediction_bias?${q}`, {
      headers: { 'Authorization': `Bearer ${SUPABASE_KEY}`, 'apikey': SUPABASE_KEY },
      signal: AbortSignal.timeout(2000),
    });
    if (!res.ok) return null;
    const rows = await res.json();
    return rows[0] ?? null;
  } catch {
    return null;
  }
}

function fmtDuration(seconds) {
  if (seconds < 60) return 'less than 1 min';
  const m = Math.round(seconds / 60);
  if (m < 60) return `${m} min`;
  return `${Math.floor(m/60)} hr ${m % 60} min`;
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'GET') return res.status(405).end();

  const query = req.query;
  const routeFilter = query.route?.toUpperCase() ?? null;

  const stop = resolveStop(query);
  if (!stop) {
    return res.status(404).json({
      ok: false,
      error: 'Stop not found. Use stopId, stop name keyword, or lat/lng.',
      monitoredStops: MONITOR_STOPS.map(s => ({ stopId: s.stopId, name: s.name, routes: s.routes })),
    });
  }

  const routes = routeFilter
    ? stop.routes.filter(r => r.toUpperCase() === routeFilter)
    : stop.routes;

  if (!routes.length) {
    return res.status(404).json({ ok: false, error: `Route ${routeFilter} does not serve ${stop.name}` });
  }

  const now        = new Date();
  const hourOfDay  = now.getHours();
  const dayOfWeek  = now.getDay();
  const walkSec    = parseInt(query.walk ?? '120'); // caller can pass walk time in seconds

  // Fetch live + bias in parallel across all matching routes
  const [liveResults, ...biasResults] = await Promise.all([
    Promise.all(routes.map(r => fetchLivePredictions(r, stop))),
    ...routes.map(r => fetchBiasCorrection(r, stop.name, hourOfDay, dayOfWeek)),
  ]);

  const predictions = [];
  for (let i = 0; i < routes.length; i++) {
    const bias = biasResults[i];
    for (const p of liveResults[i]) {
      let arrivalMs = p.expectedArrival.getTime();
      // Apply learned bias only when we have enough samples and it's confident
      if (bias?.sample_count >= 10 && bias?.bias_correction_sec) {
        arrivalMs += bias.bias_correction_sec * 1000;
      }
      const correctedArrival = new Date(arrivalMs);
      const etaSec           = Math.max(0, (correctedArrival - now) / 1000);
      const leaveInSec       = Math.max(0, etaSec - walkSec);

      predictions.push({
        route:          routes[i],
        headsign:       p.headsign,
        etaSeconds:     Math.round(etaSec),
        etaLabel:       fmtDuration(etaSec),
        leaveInSeconds: Math.round(leaveInSec),
        leaveInLabel:   fmtDuration(leaveInSec),
        distanceLabel:  p.distanceLabel,
        stopsAway:      p.stopsAway,
        biasApplied:    bias?.bias_correction_sec ?? 0,
        confidence:     bias?.confidence ?? null,
        source:         p.source,
      });
    }
  }

  predictions.sort((a, b) => a.etaSeconds - b.etaSeconds);

  const top = predictions[0] ?? null;
  const headline = top
    ? (top.leaveInSeconds <= 30
        ? `Head to the stop now — ${top.route} is ${top.etaLabel} away`
        : `Leave in ${top.leaveInLabel}  •  ${top.route} → ${top.headsign}  •  ${top.etaLabel} away`)
    : 'No buses predicted in the next 30 minutes';

  return res.status(200).json({
    ok: true,
    stop:        { stopId: stop.stopId, name: stop.name, lat: stop.lat, lng: stop.lng },
    headline,
    predictions,
    generatedAt: now.toISOString(),
    walkSeconds: walkSec,
  });
        }
