// api/ingest-mta.js
// TownTrip — MTA Historical Data Ingestion
// Hit /api/ingest-mta?dataset=bus_wait to load a specific dataset
// Hit /api/ingest-mta?dataset=all to load everything (runs in background)
//
// Datasets: bus_wait | bus_service | bus_speeds | bus_journey |
//           subway_wait | subway_journey | hourly_ridership |
//           bus_hourly_ridership | monthly_ridership

import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

const DATASETS = {
  bus_wait: [
    { id: 'bmix-dpzc', label: 'Bus Wait Assessment 2015-2019' },
    { id: 'swky-c3v4', label: 'Bus Wait Assessment 2020-2024' },
    { id: 'v4z4-2h6n', label: 'Bus Wait Assessment 2025+' },
  ],
  bus_service: [
    { id: '6qwi-vjde', label: 'Bus Service Delivered 2015+' },
  ],
  bus_speeds: [
    { id: 'intd-i3as', label: 'Bus Speeds 2015-2019' },
    { id: 'q8ws-imkh', label: 'Bus Speeds 2020+' },
  ],
  bus_journey: [
    { id: 'gs4q-vdkw', label: 'Bus Journey Time 2020+' },
  ],
  subway_wait: [
    { id: '6b7q-snec', label: 'Subway Wait Assessment 2020-2024' },
    { id: '62c4-mvcx', label: 'Subway Wait Assessment 2025+' },
  ],
  subway_journey: [
    { id: 'zghv-vkra', label: 'Subway Journey Time 2020+' },
  ],
  hourly_ridership: [
    { id: 'wujg-7c2s', label: 'Subway Hourly Ridership 2020+' },
  ],
  bus_hourly_ridership: [
    { id: 'kv7t-n8in', label: 'Bus Hourly Ridership' },
  ],
  monthly_ridership: [
    { id: 'vxuj-8kew', label: 'MTA Monthly Ridership 2002+' },
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

function transformMonthlyRidership(row) {
  return {
    month: toDate(row.month || row.date),
    mode: row.mode || row.transit_mode || '',
    ridership: toInt(row.ridership || row.total_ridership),
  };
}

async function upsertBatch(table, rows, conflictCol) {
  if (!rows.length) return { count: 0 };
  const valid = rows.filter(r => r.month || r.transit_timestamp);
  if (!valid.length) return { count: 0 };
  const { error } = await supabase
    .from(table)
    .upsert(valid, { onConflict: conflictCol, ignoreDuplicates: true });
  if (error) throw new Error(`Supabase ${table} error: ${error.message}`);
  return { count: valid.length };
}

async function ingestDataset(config, table, transformFn, conflictCol) {
  const results = [];
  for (const ds of config) {
    try {
      const raw = await fetchAll(ds.id);
      const transformed = raw.map(transformFn).filter(r => r.route_id || r.line_name || r.station_complex || r.mode);
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

export default async function handler(req, res) {
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
    if (dataset === 'subway_wait' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.subway_wait, 'mta_subway_wait_assessment', transformSubwayWait, 'line_name,month,period,day_type'));
    if (dataset === 'subway_journey' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.subway_journey, 'mta_subway_journey_time', transformSubwayJourney, 'line_name,month,period,day_type'));
    if (dataset === 'monthly_ridership' || dataset === 'all')
      results.push(...await ingestDataset(DATASETS.monthly_ridership, 'mta_monthly_ridership', transformMonthlyRidership, 'month,mode'));
    if (dataset === 'hourly_ridership')
      results.push(...await ingestDataset(DATASETS.hourly_ridership, 'mta_hourly_ridership', transformHourlyRidership, null));
    if (dataset === 'bus_hourly_ridership')
      results.push(...await ingestDataset(DATASETS.bus_hourly_ridership, 'mta_bus_hourly_ridership', transformBusHourlyRidership, null));
    return res.status(200).json({ success: true, results });
  } catch (e) {
    return res.status(500).json({ success: false, error: e.message, results });
  }
}
