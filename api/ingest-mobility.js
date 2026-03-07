// api/ingest-mobility.js
// Bronx-filtered ingest for all mobility intelligence datasets.
// Smart strategy: filter at source, never store junk.

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const INGEST_SECRET = process.env.INGEST_SECRET;

const BRONX = { minLat: 40.785, maxLat: 40.915, minLng: -73.933, maxLng: -73.748 };
function inBronx(lat, lng) {
  const la = parseFloat(lat), lo = parseFloat(lng);
  return la >= BRONX.minLat && la <= BRONX.maxLat && lo >= BRONX.minLng && lo <= BRONX.maxLng;
}

async function supabaseUpsert(table, rows, onConflict = null) {
  if (!rows.length) return 0;
  const prefer = onConflict
    ? `resolution=merge-duplicates,return=minimal`
    : `resolution=ignore-duplicates,return=minimal`;
  const url = onConflict
    ? `${SUPABASE_URL}/rest/v1/${table}?on_conflict=${onConflict}`
    : `${SUPABASE_URL}/rest/v1/${table}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'apikey': SUPABASE_KEY,
      'Prefer': prefer,
    },
    body: JSON.stringify(rows),
  });
  if (!res.ok) throw new Error(`${table}: ${await res.text()}`);
  return rows.length;
}

async function ingestCitibikeStations() {
  const data = await fetch('https://gbfs.citibikenyc.com/gbfs/en/station_information.json').then(r => r.json());
  const rows = data.data.stations
    .filter(s => inBronx(s.lat, s.lon))
    .map(s => ({
      station_id: String(s.station_id), name: s.name,
      lat: s.lat, lng: s.lon, capacity: s.capacity,
      region_id: String(s.region_id || '')
    }));
  return supabaseUpsert('citibike_stations', rows, 'station_id');
}

async function ingestCitibikeStatus() {
  const stRes = await fetch(`${SUPABASE_URL}/rest/v1/citibike_stations?select=station_id`, {
    headers: { 'Authorization': `Bearer ${SUPABASE_KEY}`, 'apikey': SUPABASE_KEY }
  });
  const bronxIds = new Set((await stRes.json()).map(s => s.station_id));
  const data = await fetch('https://gbfs.citibikenyc.com/gbfs/en/station_status.json').then(r => r.json());
  const now = new Date().toISOString();
  const rows = data.data.stations
    .filter(s => bronxIds.has(String(s.station_id)))
    .map(s => ({
      station_id: String(s.station_id), observed_at: now,
      bikes_available: s.num_bikes_available, docks_available: s.num_docks_available,
      is_installed: s.is_installed === 1, is_renting: s.is_renting === 1
    }));
  return supabaseUpsert('citibike_status', rows);
}

async function ingestDotTraffic() {
  const url = `https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$where=borough=%27Bronx%27&$limit=500&$order=data_as_of+DESC`;
  const data = await fetch(url).then(r => r.json());
  const rows = data.map(d => ({
    observed_at: d.data_as_of || new Date().toISOString(),
    segment_id: d.id, street_name: d.street,
    speed_mph: parseFloat(d.speed) || null,
    travel_time_sec: parseInt(d.travel_time) || null,
    borough: 'Bronx'
  })).filter(r => r.speed_mph);
  return supabaseUpsert('dot_traffic_speed', rows);
}

async function ingestSchoolCalendar() {
  // NYC DOE school year calendar — use the correct dataset ID
  const url = 'https://data.cityofnewyork.us/resource/9yzm-zhjf.json?$limit=300';
  const raw = await fetch(url).then(r => r.json());
  // Handle both array and object responses
  const data = Array.isArray(raw) ? raw : (raw.data || []);
  if (!data.length) {
    // Fallback: try alternate dataset
    const alt = await fetch('https://data.cityofnewyork.us/resource/jkm7-sfbi.json?$limit=300').then(r => r.json());
    const altData = Array.isArray(alt) ? alt : [];
    if (!altData.length) return 0;
    const rows = altData.map(d => ({
      signal_date: (d.date || d.start_date || '').split('T')[0],
      hour_of_day: null,
      signal_type: 'school',
      signal_value: (d.description || d.event || d.daytype || 'school_event').toLowerCase().substring(0,50),
      severity: 'medium',
      description: d.description || d.event || d.daytype,
      source: 'nyc_doe'
    })).filter(r => r.signal_date);
    return supabaseUpsert('context_signals', rows);
  }
  const rows = data.map(d => ({
    signal_date: (d.date || d.start_date || '').split('T')[0],
    hour_of_day: null,
    signal_type: 'school',
    signal_value: (d.daytype || d.description || 'school_event').toLowerCase().substring(0,50),
    severity: 'medium',
    description: d.daytype || d.description,
    source: 'nyc_doe'
  })).filter(r => r.signal_date);
  return supabaseUpsert('context_signals', rows);
}

async function ingestBusViolations() {
  const url = `https://data.cityofnewyork.us/resource/pvqr-7yc4.json?$where=violation_county=%27BX%27&$limit=1000&$order=issue_date+DESC`;
  const data = await fetch(url).then(r => r.json());
  if (!Array.isArray(data)) return 0;
  const rows = data.map(d => ({
    signal_date: (d.issue_date || '').split('T')[0],
    hour_of_day: d.violation_time ? parseInt(d.violation_time.substring(0,2)) : null,
    signal_type: 'bus_lane_violation',
    signal_value: (d.street_name || 'unknown').substring(0,50),
    severity: 'low',
    affected_borough: 'Bronx',
    description: `${d.violation_description || ''} at ${d.street_name || ''}`.trim().substring(0,200),
    source: 'nyc_dot_camera'
  })).filter(r => r.signal_date);
  return supabaseUpsert('context_signals', rows);
}

async function ingestWeather() {
  // NWS free API — Bronx / NYC forecast
  try {
    const points = await fetch('https://api.weather.gov/points/40.8448,-73.8648').then(r => r.json());
    const forecastUrl = points.properties?.forecast;
    if (!forecastUrl) return 0;
    const forecast = await fetch(forecastUrl).then(r => r.json());
    const periods = forecast.properties?.periods?.slice(0, 14) || [];
    const today = new Date().toISOString().split('T')[0];
    const rows = periods.map(p => ({
      signal_date: today,
      hour_of_day: p.isDaytime ? 12 : 0,
      signal_type: 'weather',
      signal_value: p.shortForecast?.toLowerCase()?.substring(0,50) || 'unknown',
      severity: (p.windSpeed?.includes('mph') && parseInt(p.windSpeed) > 25) ? 'high' : 'low',
      description: `${p.name}: ${p.detailedForecast}`.substring(0,200),
      source: 'nws'
    }));
    return supabaseUpsert('context_signals', rows);
  } catch { return 0; }
}

async function ingestMtaAlerts() {
  const MTA_KEY = process.env.MTA_API_KEY;
  try {
    const data = await fetch(`https://bustime.mta.info/api/siri/situation-exchange.json?key=${MTA_KEY}&MaximumNumberOfSituationElements=50`).then(r => r.json());
    const situations = data?.Siri?.ServiceDelivery?.SituationExchangeDelivery?.[0]?.Situations?.PtSituationElement ?? [];
    const now = new Date();
    const rows = situations.map(s => ({
      signal_date: now.toISOString().split('T')[0],
      hour_of_day: now.getHours(),
      signal_type: 'alert',
      signal_value: (s.Summary?.[0]?.value || 'MTA Alert').substring(0,50),
      severity: s.Severity === 'noImpact' ? 'low' : 'high',
      affected_routes: s.Affects?.VehicleJourneys?.AffectedVehicleJourney?.map(j => j.LineRef?.split('_').pop()).filter(Boolean) || null,
      description: (s.Description?.[0]?.value || '').substring(0,200),
      source: 'mta_alerts'
    }));
    return supabaseUpsert('context_signals', rows);
  } catch { return 0; }
}

export default async function handler(req, res) {
  if (req.query.token !== INGEST_SECRET) return res.status(401).json({ error: 'unauthorized' });
  const { dataset } = req.query;
  const start = Date.now();
  try {
    let result;
    switch(dataset) {
      case 'citibike_stations':  result = await ingestCitibikeStations(); break;
      case 'citibike_status':    result = await ingestCitibikeStatus(); break;
      case 'dot_traffic':        result = await ingestDotTraffic(); break;
      case 'school_calendar':    result = await ingestSchoolCalendar(); break;
      case 'bus_violations':     result = await ingestBusViolations(); break;
      case 'weather':            result = await ingestWeather(); break;
      case 'mta_alerts':         result = await ingestMtaAlerts(); break;
      case 'all_quick': {
        const [a,b,c,d,e] = await Promise.allSettled([
          ingestCitibikeStations(), ingestCitibikeStatus(),
          ingestDotTraffic(), ingestWeather(), ingestMtaAlerts()
        ]);
        result = { stations: a.value, status: b.value, traffic: c.value, weather: d.value, alerts: e.value };
        break;
      }
      default:
        return res.status(400).json({ error: 'unknown dataset',
          valid: ['citibike_stations','citibike_status','dot_traffic','school_calendar','bus_violations','weather','mta_alerts','all_quick'] });
    }
    return res.status(200).json({ ok: true, dataset, result, elapsed: Date.now() - start });
  } catch(e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
}
