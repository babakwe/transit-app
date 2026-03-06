// api/poller.js — Vercel cron job, runs every 30 min
// Polls MTA arrivals for monitored stops and logs to Supabase

const MTA_KEY = process.env.MTA_API_KEY;
const SUPABASE_URL = 'https://xyfyuvikqmxcazqgqoxb.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inh5Znl1dmlrcW14Y2F6cWdxb3hiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY2ODc1OCwiZXhwIjoyMDg4MjQ0NzU4fQ.jPRln_LQRrIGF5iA-H_DBsRW2FjPaf3ys5yBvy908eo';

// Key stops to monitor — high-traffic Bronx stops across multiple routes
const MONITORED_STOPS = [
  { stopId: '100080', routes: ['BX28','BX23','BX26','BX30'] }, // East Gun Hill Rd/Hull Av
  { stopId: '100017', routes: ['BX28','BX38'] },               // Co-op City area
  { stopId: '200461', routes: ['BX1','BX2'] },                 // Fordham Rd hub
  { stopId: '302386', routes: ['BX12','BX15'] },               // Tremont Av
  { stopId: '308214', routes: ['BX41'] },                      // White Plains Rd
];

async function fetchArrivals(stopId, route) {
  const agency = route.startsWith('BXM') || route.startsWith('X') ? 'MTA+BC' : 'MTA+NYCT';
  const url = `https://bustime.mta.info/api/siri/stop-monitoring.json?key=${MTA_KEY}&MonitoringRef=${stopId}&LineRef=${agency}_${route}&MaximumStopVisits=3`;
  try {
    const r = await fetch(url);
    const d = await r.json();
    const visits = d?.Siri?.ServiceDelivery?.StopMonitoringDelivery?.[0]?.MonitoredStopVisit || [];
    return visits.map(v => {
      const call = v.MonitoredVehicleJourney?.MonitoredCall;
      const aimed = call?.AimedArrivalTime || call?.AimedDepartureTime;
      const expected = call?.ExpectedArrivalTime || call?.ExpectedDepartureTime;
      const now = Date.now();
      const aimedMs = aimed ? new Date(aimed).getTime() : null;
      const expectedMs = expected ? new Date(expected).getTime() : null;
      const aimedMins = aimedMs ? Math.round((aimedMs - now) / 60000) : null;
      const expectedMins = expectedMs ? Math.round((expectedMs - now) / 60000) : null;
      return { aimed_mins: aimedMins, expected_mins: expectedMins, vehicle_id: v.MonitoredVehicleJourney?.VehicleRef || null };
    });
  } catch (e) {
    return [];
  }
}

async function writeToSupabase(rows) {
  if (!rows.length) return;
  const res = await fetch(`${SUPABASE_URL}/rest/v1/bus_observations`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'apikey': SUPABASE_KEY,
      'Prefer': 'return=minimal'
    },
    body: JSON.stringify(rows)
  });
  return res.status;
}

export default async function handler(req, res) {
  const now = new Date();
  const rows = [];

  for (const stop of MONITORED_STOPS) {
    for (const route of stop.routes) {
      const arrivals = await fetchArrivals(stop.stopId, route);
      for (const a of arrivals) {
        if (a.expected_mins !== null && a.expected_mins >= 0 && a.expected_mins < 90) {
          rows.push({
            stop_id: stop.stopId,
            route,
            observed_at: now.toISOString(),
            day_of_week: now.getDay(),       // 0=Sun, 6=Sat
            hour_of_day: now.getHours(),
            aimed_mins: a.aimed_mins,
            expected_mins: a.expected_mins,
            delay_mins: (a.expected_mins !== null && a.aimed_mins !== null) ? a.expected_mins - a.aimed_mins : null,
            vehicle_id: a.vehicle_id
          });
        }
      }
    }
  }

  const status = await writeToSupabase(rows);
  res.status(200).json({ written: rows.length, supabase_status: status, at: now.toISOString() });
}
