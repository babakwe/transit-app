// api/ingest-mobility.js
// GET /api/ingest-mobility?dataset=citibike_stations&token=passwordbi1
// GET /api/ingest-mobility?dataset=citibike_status&token=passwordbi1
// GET /api/ingest-mobility?dataset=dot_traffic&token=passwordbi1
// GET /api/ingest-mobility?dataset=school_calendar&token=passwordbi1
// GET /api/ingest-mobility?dataset=bus_violations&token=passwordbi1
// GET /api/ingest-mobility?dataset=mta_alerts&token=passwordbi1
// GET /api/ingest-mobility?dataset=all_quick&token=passwordbi1
//
// SMART STRATEGY: Filter to Bronx-relevant data BEFORE writing to Supabase.
// Citi Bike trips need the local bulk script (too large for serverless).

const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const INGEST_SECRET = process.env.INGEST_SECRET;

const BRONX = { minLat: 40.785, maxLat: 40.915, minLng: -73.933, maxLng: -73.748 };
function inBronx(lat, lng) {
  const la = parseFloat(lat), lo = parseFloat(lng);
  return la >= BRONX.minLat && la <= BRONX.maxLat && lo >= BRONX.minLng && lo <= BRONX.maxLng;
}

async function supabaseInsert(table, rows) {
  if (!rows.length) return 0;
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${SUPABASE_KEY}`, 'apikey': SUPABASE_KEY, 'Prefer': 'resolution=ignore-duplicates,return=minimal' },
    body: JSON.stringify(rows),
  });
  if (!res.ok) throw new Error(`${table}: ${await res.text()}`);
  return rows.length;
}

async function ingestCitibikeStations() {
  const data = await fetch('https://gbfs.citibikenyc.com/gbfs/en/station_information.json').then(r => r.json());
  const rows = data.data.stations
    .filter(s => inBronx(s.lat, s.lon))
    .map(s => ({ station_id: String(s.station_id), name: s.name, lat: s.lat, lng: s.lon, capacity: s.capacity, region_id: String(s.region_id || '') }));
  return supabaseInsert('citibike_stations', rows);
}

async function ingestCitibikeStatus() {
  const stRes = await fetch(`${SUPABASE_URL}/rest/v1/citibike_stations?select=station_id`, { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}`, 'apikey': SUPABASE_KEY } });
  const bronxIds = new Set((await stRes.json()).map(s => s.station_id));
  const data = await fetch('https://gbfs.citibikenyc.com/gbfs/en/station_status.json').then(r => r.json());
  const now = new Date().toISOString();
  const rows = data.data.stations
    .filter(s => bronxIds.has(String(s.station_id)))
    .map(s => ({ station_id: String(s.station_id), observed_at: now, bikes_available: s.num_bikes_available, docks_available: s.num_docks_available, is_installed: s.is_installed === 1, is_renting: s.is_renting === 1 }));
  return supabaseInsert('citibike_status', rows);
}

async function ingestDotTraffic() {
  const url = `https://data.cityofnewyork.us/resource/i4gi-tjb9.json?\$where=borough=%27Bronx%27&\$limit=500&\$order=data_as_of+DESC`;
  const data = await fetch(url).then(r => r.json());
  const rows = data.map(d => ({ observed_at: d.data_as_of || new Date().toISOString(), segment_id: d.id, street_name: d.street, speed_mph: parseFloat(d.speed) || null, travel_time_sec: parseInt(d.travel_time) || null, borough: 'Bronx' })).filter(r => r.speed_mph);
  return supabaseInsert('dot_traffic_speed', rows);
}

async function ingestSchoolCalendar() {
  const data = await fetch('https://data.cityofnewyork.us/resource/9yzm-zhjf.json?$limit=300').then(r => r.json());
  const rows = data.map(d => ({ signal_date: d.date?.split('T')[0], hour_of_day: null, signal_type: 'school', signal_value: d.daytype?.toLowerCase().includes('no school') ? 'no_school' : 'early_dismissal', severity: 'medium', description: d.daytype, source: 'nyc_doe' })).filter(r => r.signal_date);
  return supabaseInsert('context_signals', rows);
}

async function ingestBusViolations() {
  const url = `https://data.cityofnewyork.us/resource/pvqr-7yc4.json?\$where=violation_county=%27BX%27&\$limit=1000&\$order=issue_date+DESC`;
  const data = await fetch(url).then(r => r.json());
  const rows = data.map(d => ({ signal_date: d.issue_date?.split('T')[0], hour_of_day: d.violation_time ? parseInt(d.violation_time.substring(0,2)) : null, signal_type: 'bus_lane_violation', signal_value: d.street_name, severity: 'low', affected_borough: 'Bronx', description: `${d.violation_description} at ${d.street_name}`, source: 'nyc_dot_camera' })).filter(r => r.signal_date);
  return supabaseInsert('context_signals', rows);
}

async function ingestMtaAlerts() {
  const MTA_KEY = process.env.MTA_API_KEY;
  try {
    const data = await fetch(`https://bustime.mta.info/api/siri/situation-exchange.json?key=${MTA_KEY}&MaximumNumberOfSituationElements=50`).then(r => r.json());
    const situations = data?.Siri?.ServiceDelivery?.SituationExchangeDelivery?.[0]?.Situations?.PtSituationElement ?? [];
    const now = new Date();
    const rows = situations.map(s => ({ signal_date: now.toISOString().split('T')[0], hour_of_day: now.getHours(), signal_type: 'alert', signal_value: s.Summary?.[0]?.value || 'MTA Alert', severity: s.Severity === 'noImpact' ? 'low' : 'high', affected_routes: s.Affects?.VehicleJourneys?.AffectedVehicleJourney?.map(j => j.LineRef?.split('_').pop()).filter(Boolean) || null, description: s.Description?.[0]?.value, source: 'mta_alerts' }));
    return supabaseInsert('context_signals', rows);
  } catch { return 0; }
}

export default async function handler(req, res) {
  if (req.query.token !== INGEST_SECRET) return res.status(401).json({ error: 'unauthorized' });
  const { dataset } = req.query;
  const start = Date.now();
  try {
    let result;
    switch(dataset) {
      case 'citibike_stations': result = await ingestCitibikeStations(); break;
      case 'citibike_status':   result = await ingestCitibikeStatus();   break;
      case 'dot_traffic':       result = await ingestDotTraffic();        break;
      case 'school_calendar':   result = await ingestSchoolCalendar();    break;
      case 'bus_violations':    result = await ingestBusViolations();     break;
      case 'mta_alerts':        result = await ingestMtaAlerts();         break;
      case 'all_quick': {
        const [a,b,c,d] = await Promise.allSettled([ingestCitibikeStations(), ingestCitibikeStatus(), ingestDotTraffic(), ingestMtaAlerts()]);
        result = { stations: a.value, status: b.value, traffic: c.value, alerts: d.value };
        break;
      }
      default: return res.status(400).json({ error: 'unknown dataset', valid: ['citibike_stations','citibike_status','dot_traffic','school_calendar','bus_violations','mta_alerts','all_quick'] });
    }
    return res.status(200).json({ ok: true, dataset, result, elapsed: Date.now() - start });
  } catch(e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
}
