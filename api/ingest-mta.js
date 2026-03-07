const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ── Verified Socrata dataset IDs from official MTA catalog ─────────────────
const DATASETS = {
  // Bus Wait Assessment (evenly-spaced buses metric)
  bus_wait: [
    { id: 'bmix-dpzc', label: 'Bus Wait Assessment 2015-2019' },
    { id: 'swky-c3v4', label: 'Bus Wait Assessment 2020-2024' },
    { id: 'v4z4-2h6n', label: 'Bus Wait Assessment 2025+' },
  ],
  // Bus Service Delivered (% of scheduled buses that ran)
  bus_service: [
    { id: 'tw28-zvtk', label: 'Bus Service Delivered 2015-2019' },
    { id: '2e6s-9gpm', label: 'Bus Service Delivered 2020-2024' },
    { id: '6qwi-vjde', label: 'Bus Service Delivered 2025+' },
  ],
  // Bus Speeds (avg speed by route)
  bus_speeds: [
    { id: 'cudb-vcni', label: 'Bus Speeds Beginning 2015' },
  ],
  // Bus Journey Time (additional stop time + travel time vs schedule)
  bus_journey: [
    { id: '8mkn-d32t', label: 'Bus Journey Time Beginning 2017' },
  ],
  // Bus Route Segment Speeds (speed between timepoints by hour/day — HIGH VALUE)
  bus_segment_speeds: [
    { id: '58t6-89vi', label: 'Bus Segment Speeds 2023-2024' },
    { id: 'kufs-yh3x', label: 'Bus Segment Speeds 2025+' },
  ],
  // Bus Schedules — scheduled timepoint stop times by year
  bus_schedules: [
    { id: 'udt9-hvjq', label: 'Bus Schedules 2024' },
    { id: 't4bz-xqa9', label: 'Bus Schedules 2025' },
  ],
  // Subway Wait Assessment
  subway_wait: [
    { id: '6b7q-snec', label: 'Subway Wait Assessment 2020-2024' },
    { id: '62c4-mvcx', label: 'Subway Wait Assessment 2025+' },
  ],
  // Subway Journey Time
  subway_journey: [
    { id: 'r7qk-6tcy', label: 'Subway Journey Time 2015-2019' },
    { id: '4apg-4kt9', label: 'Subway Journey Time 2020-2024' },
    { id: 's4u6-t435', label: 'Subway Journey Time 2025+' },
  ],
  // Daily ridership by mode (subway, bus, LIRR, MNR, etc)
  daily_ridership: [
    { id: 'vxuj-8kew', label: 'MTA Daily Ridership 2020+' },
  ],
  // Large datasets — run explicitly, not included in 'all'
  hourly_ridership: [
    { id: 'wujg-7c2s', label: 'Subway Hourly Ridership 2020-2024' },
  ],
  bus_hourly_ridership: [
    { id: 'kv7t-n8in', label: 'Bus Hourly Ridership 2020-2024' },
    { id: 'gxb3-akrn', label: 'Bus Hourly Ridership 2025+' },
  ],
};

async function fetchPage(datasetId, offset = 0, limit = 50000) {
  const url = `https://data.ny.gov/resource/${datasetId}.json?$limit=${limit}&$offset=${offset}&$order=:id`;
  const res = await fetch(url, {
    headers: { 'X-App-Token': process.env.SOCRATA_APP_TOKEN || '' },
  });
  if (!res.ok) throw new Error(`Socrata ${datasetId} error: ${res.status}`);
  return res.json();
}

async function fetchAll(datasetId, limit = 50000) {
  let rows = [];
  let offset = 0;
  while (true) {
    const page = await fetchPage(datasetId, offset, limit);
    rows = rows.concat(page);
    if (page.length < limit) break;
    offset += limit;
  }
  return rows;
}

function toDate(val) {
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d) ? null : d.toISOString().slice(0, 10);
}
function toNum(val) { const n = parseFloat(val); return isNaN(n) ? null : n; }
function toInt(val) { const n = parseInt(val); return isNaN(n) ? null : n; }

function transformBusWait(row) {
  return {
    month: toDate(row.month || row.period_year_month || row.date),
    route_id: (row.route_id || row.route || '').toUpperCase().trim(),
    borough: row.borough || null,
    agency: row.agency || null,
    wait_assessment: toNum(row.wait_assessment || row.wa_pct),
    scheduled_wt: toNum(row.scheduled_headway || row.sched_wt),
    actual_wt: toNum(row.actual_headway || row.actual_wt),
    period: row.period || row.time_period || null,
    day_type: row.day_type || row.day || null,
  };
}
function transformBusService(row) {
  return {
    month: toDate(row.month || row.date),
    route_id: (row.route_id || row.route || '').toUpperCase().trim(),
    borough: row.borough || null,
    agency: row.agency || null,
    service_delivered: toNum(row.service_delivered || row.sd_pct),
    scheduled_trips: toInt(row.scheduled_trips),
    actual_trips: toInt(row.actual_trips),
    period: row.period || null,
    day_type: row.day_type || null,
  };
}
function transformBusSpeeds(row) {
  return {
    month: toDate(row.month || row.date),
    route_id: (row.route_id || row.route || '').toUpperCase().trim(),
    borough: row.borough || null,
    agency: row.agency || null,
    avg_speed_mph: toNum(row.average_speed || row.avg_speed || row.speed_mph),
    period: row.period || null,
    day_type: row.day_type || null,
  };
}
function transformBusJourney(row) {
  return {
    month: toDate(row.month || row.date),
    route_id: (row.route_id || row.route || '').toUpperCase().trim(),
    borough: row.borough || null,
    agency: row.agency || null,
    additional_bus_stop_time: toNum(row.additional_bus_stop_time || row.add_stop_time),
    additional_travel_time: toNum(row.additional_travel_time || row.add_travel_time),
    total_additional_time: toNum(row.total_additional_time || row.total_add_time),
    journey_time_pct: toNum(row.customer_journey_time_performance || row.cjtp),
    period: row.period || null,
    day_type: row.day_type || null,
  };
}
// Bus segment speeds: speed between timepoints by route/hour/day — core prediction data
function transformBusSegmentSpeeds(row) {
  return {
    month: toDate(row.month || row.date),
    route_id: (row.route_id || row.route || '').toUpperCase().trim(),
    borough: row.borough || null,
    from_stop: row.timepoint_1 || row.from_stop_name || null,
    to_stop: row.timepoint_2 || row.to_stop_name || null,
    from_lat: toNum(row.timepoint_1_latitude || row.from_lat),
    from_lng: toNum(row.timepoint_1_longitude || row.from_lng),
    to_lat: toNum(row.timepoint_2_latitude || row.to_lat),
    to_lng: toNum(row.timepoint_2_longitude || row.to_lng),
    avg_speed_mph: toNum(row.average_speed_mph || row.avg_speed),
    avg_travel_time_min: toNum(row.average_travel_time_minutes || row.avg_travel_time),
    distance_miles: toNum(row.distance_miles || row.distance),
    trip_count: toInt(row.number_of_trips || row.trip_count),
    day_of_week: row.day_of_week || row.day || null,
    hour_of_day: toInt(row.hour_of_day || row.hour),
    trip_type: row.trip_type || null,
  };
}
// Bus schedules: planned timepoint arrivals
function transformBusSchedule(row) {
  return {
    month: toDate(row.month || row.date || row.service_date),
    route_id: (row.route_id || row.route || '').toUpperCase().trim(),
    trip_id: row.trip_id || null,
    stop_id: row.stop_id || null,
    stop_name: row.stop_name || row.timepoint_name || null,
    stop_sequence: toInt(row.stop_sequence),
    arrival_time: row.arrival_time || row.scheduled_arrival || null,
    departure_time: row.departure_time || row.scheduled_departure || null,
    direction: row.direction_id || row.direction || null,
    service_date: toDate(row.service_date || row.date),
  };
}
function transformSubwayWait(row) {
  return {
    month: toDate(row.month || row.date),
    line_name: (row.line_name || row.line || row.route || '').trim(),
    borough: row.borough || null,
    wait_assessment: toNum(row.wait_assessment || row.wa_pct),
    period: row.period || null,
    day_type: row.day_type || null,
  };
}
function transformSubwayJourney(row) {
  return {
    month: toDate(row.month || row.date),
    line_name: (row.line_name || row.line || '').trim(),
    borough: row.borough || null,
    additional_platform_time: toNum(row.additional_platform_time),
    additional_train_time: toNum(row.additional_train_time),
    total_additional_time: toNum(row.total_additional_time),
    journey_time_pct: toNum(row.customer_journey_time_performance || row.cjtp),
    period: row.period || null,
    day_type: row.day_type || null,
  };
}
// Daily ridership — one row per date per mode (subway, bus, LIRR, MNR, etc)
function transformDailyRidership(row) {
  const date = toDate(row.date || row.month);
  const results = [];
  const modes = {
    'Subway': row.subways_total_estimated_ridership,
    'Bus': row.buses_total_estimated_ridership,
    'LIRR': row.lirr_total_estimated_ridership,
    'Metro-North': row.metro_north_total_estimated_ridership,
    'Access-A-Ride': row.access_a_ride_total_scheduled_trips,
    'Bridges-Tunnels': row.bridges_and_tunnels_total_traffic,
    'Staten Island Railway': row.staten_island_railway_total_estimated_ridership,
  };
  for (const [mode, val] of Object.entries(modes)) {
    const ridership = toInt(val);
    if (ridership !== null) results.push({ month: date, mode, ridership });
  }
  return results;
}
function transformHourlyRidership(row) {
  return {
    transit_timestamp: row.transit_timestamp || null,
    station_complex_id: row.station_complex_id || null,
    station_complex: row.station_complex || row.station || '',
    borough: row.borough || null,
    line_name: row.routes || row.line_name || null,
    ridership: toInt(row.ridership),
    transfers: toInt(row.transfers),
    lat: toNum(row.latitude || row.lat),
    lng: toNum(row.longitude || row.lng),
  };
}
function transformBusHourlyRidership(row) {
  return {
    transit_timestamp: row.transit_timestamp || null,
    route_id: (row.route_id || row.route || '').toUpperCase().trim(),
    borough: row.borough || null,
    ridership: toInt(row.ridership || row.total_ridership),
    fare_class: row.fare_class_category || row.fare_class || null,
  };
}

async function upsertBatch(table, rows, conflictCol) {
  if (!rows.length) return { count: 0 };
  const valid = rows.filter(r => r.month || r.transit_timestamp || r.service_date);
  if (!valid.length) return { count: 0 };
  const opts = conflictCol
    ? { onConflict: conflictCol, ignoreDuplicates: true }
    : { ignoreDuplicates: true };
  const { error } = await supabase.from(table).upsert(valid, opts);
  if (error) throw new Error(`Supabase ${table} error: ${error.message}`);
  return { count: valid.length };
}

// Standard ingest for flat transforms (1 row in → 1 row out)
async function ingestDataset(config, table, transformFn, conflictCol) {
  const results = [];
  for (const ds of config) {
    try {
      const raw = await fetchAll(ds.id);
      const transformed = raw.map(transformFn).filter(r =>
        r.route_id || r.line_name || r.station_complex || r.mode || r.trip_id || r.from_stop
      );
      let total = 0;
      for (let i = 0; i < transformed.length; i += 5000) {
        const { count } = await upsertBatch(table, transformed.slice(i, i + 5000), conflictCol);
        total += count;
      }
      results.push({ dataset: ds.label, rows: total, status: 'ok' });
    } catch (e) {
      results.push({ dataset: ds.label, error: e.message, status: 'error' });
    }
  }
  return results;
}

// Special ingest for daily ridership (1 row in → multiple rows out, one per mode)
async function ingestDailyRidership() {
  const results = [];
  for (const ds of DATASETS.daily_ridership) {
    try {
      const raw = await fetchAll(ds.id);
      const allRows = raw.flatMap(transformDailyRidership).filter(r => r.month && r.mode);
      let total = 0;
      for (let i = 0; i < allRows.length; i += 5000) {
        const { count } = await upsertBatch('mta_monthly_ridership', allRows.slice(i, i + 5000), 'month,mode');
        total += count;
      }
      results.push({ dataset: ds.label, rows: total, status: 'ok' });
    } catch (e) {
      results.push({ dataset: ds.label, error: e.message, status: 'error' });
    }
  }
  return results;
}

module.exports = async function handler(req, res) {
  const token = req.query.token || req.headers['x-ingest-token'];
  if (token !== process.env.INGEST_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const dataset = req.query.dataset || 'all';
  const results = [];
  try {
    if (dataset === 'bus_wait' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.bus_wait, 'mta_bus_wait_assessment', transformBusWait, 'route_id,month,period,day_type'));
    if (dataset === 'bus_service' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.bus_service, 'mta_bus_service_delivered', transformBusService, 'route_id,month,period,day_type'));
    if (dataset === 'bus_speeds' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.bus_speeds, 'mta_bus_speeds', transformBusSpeeds, 'route_id,month,period,day_type'));
    if (dataset === 'bus_journey' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.bus_journey, 'mta_bus_journey_time', transformBusJourney, 'route_id,month,period,day_type'));
    if (dataset === 'bus_segment_speeds' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.bus_segment_speeds, 'mta_bus_segment_speeds', transformBusSegmentSpeeds, 'route_id,month,from_stop,to_stop,day_of_week,hour_of_day'));
    if (dataset === 'bus_schedules' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.bus_schedules, 'mta_bus_schedules', transformBusSchedule, 'route_id,trip_id,stop_id,service_date'));
    if (dataset === 'subway_wait' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.subway_wait, 'mta_subway_wait_assessment', transformSubwayWait, 'line_name,month,period,day_type'));
    if (dataset === 'subway_journey' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.subway_journey, 'mta_subway_journey_time', transformSubwayJourney, 'line_name,month,period,day_type'));
    if (dataset === 'daily_ridership' || dataset === 'all')
      results.push(...await ingestDailyRidership());
    // Large datasets — run explicitly only
    if (dataset === 'hourly_ridership')
      results.push(...await ingestDataset(DATASETS.hourly_ridership, 'mta_hourly_ridership', transformHourlyRidership, null));
    if (dataset === 'bus_hourly_ridership')
      results.push(...await ingestDataset(DATASETS.bus_hourly_ridership, 'mta_bus_hourly_ridership', transformBusHourlyRidership, null));
    return res.status(200).json({ success: true, results });
  } catch (e) {
    return res.status(500).json({ success: false, error: e.message, results });
  }
};
