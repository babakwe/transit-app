// api/poller.js
// Cron: every 2 min. Polls MTA arrivals for key stops, writes to Supabase.
// This is the data engine for predictive routing - NYC today, KMC tomorrow.

const MTA_KEY = process.env.MTA_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

const MONITOR_STOPS = [
  { stopId: '100080', name: 'BOSTON RD/E GUN HILL RD',      routes: ['Bx28','Bx38'] },
  { stopId: '100998', name: 'FORDHAM RD/GRAND CONCOURSE',   routes: ['Bx12','Bx17','Bx19'] },
  { stopId: '101002', name: 'FORDHAM RD/JEROME AV',         routes: ['Bx12','Bx17'] },
  { stopId: '103042', name: 'VALENTINE AV/E 192 ST',        routes: ['Bx28'] },
  { stopId: '200987', name: 'WHITE PLAINS RD/GUN HILL RD',  routes: ['Bx39','Bx41'] },
  { stopId: '301036', name: 'PELHAM PKWY/WHITE PLAINS RD',  routes: ['Bx28','Bx39'] },
  { stopId: '400721', name: 'JEROME AV/BURNSIDE AV',        routes: ['Bx13','Bx41'] },
  { stopId: '500814', name: 'TREMONT AV/GRAND CONCOURSE',   routes: ['Bx1','Bx2'] },
  { stopId: '200640', name: 'E GUN HILL RD/HULL AV',        routes: ['Bx28','Bx38'] },
  { stopId: '104026', name: 'E 233 ST/WHITE PLAINS RD',     routes: ['Bx39','Bx41'] },
];

function agencyPrefix(route) {
  const r = route.toUpperCase();
  if (r.startsWith('BXM') || r.startsWith('QM') || r.startsWith('X')) return 'MTA BC_';
  return 'MTA NYCT_';
}

async function pollStop(stop) {
  const obs = [];
  for (const route of stop.routes) {
    try {
      const lineRef = encodeURIComponent(agencyPrefix(route) + route.toUpperCase());
      const url = `https://bustime.mta.info/api/siri/stop-monitoring.json?key=${MTA_KEY}&MonitoringRef=${stop.stopId}&LineRef=${lineRef}&MaximumStopVisits=5`;
      const data = await fetch(url).then(r => r.json());
      const visits = data?.Siri?.ServiceDelivery?.StopMonitoringDelivery?.[0]?.MonitoredStopVisit || [];
      for (const v of visits) {
        const j = v?.MonitoredVehicleJourney;
        if (!j) continue;
        const call = j?.MonitoredCall;
        const expectedTime = call?.ExpectedArrivalTime || call?.AimedArrivalTime;
        if (!expectedTime) continue;
        const expectedMins = Math.round((new Date(expectedTime) - Date.now()) / 60000);
        const headsign = Array.isArray(j?.DestinationName) ? j.DestinationName[0] : j?.DestinationName || '';
        const routeName = Array.isArray(j?.PublishedLineName) ? j.PublishedLineName[0] : j?.PublishedLineName || route;
        obs.push({
          stop_id: stop.stopId,
          stop_name: stop.name,
          route: routeName,
          headsign: headsign.slice(0, 80),
          vehicle_id: j?.VehicleRef || null,
          expected_mins: expectedMins,
          day_of_week: new Date().getDay(),
          hour_of_day: new Date().getHours(),
        });
      }
    } catch(e) { console.error('poller', stop.stopId, route, e.message); }
  }
  return obs;
}

async function writeToSupabase(rows) {
  if (!rows.length) return 0;
  const res = await fetch(`${SUPABASE_URL}/rest/v1/bus_observations`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'apikey': SUPABASE_SERVICE_KEY,
      'Prefer': 'return=minimal',
    },
    body: JSON.stringify(rows),
  });
  if (!res.ok) throw new Error(await res.text());
  return rows.length;
}

export default async function handler(req, res) {
  if (req.method !== 'GET') return res.status(405).end();
  const start = Date.now();
  const allObs = [];
  for (let i = 0; i < MONITOR_STOPS.length; i += 3) {
    const results = await Promise.all(MONITOR_STOPS.slice(i, i+3).map(pollStop));
    results.forEach(r => allObs.push(...r));
    if (i + 3 < MONITOR_STOPS.length) await new Promise(r => setTimeout(r, 400));
  }
  const written = await writeToSupabase(allObs);
  return res.status(200).json({ ok: true, written, elapsed: Date.now()-start, stops: MONITOR_STOPS.length });
}