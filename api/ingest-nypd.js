import { createClient } from '@supabase/supabase-js';
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
function ninetyDaysAgo() {
  const d = new Date(); d.setDate(d.getDate() - 90);
  return d.toISOString().split('T')[0];
}
const DATASETS = [
  { id: 'nypd_collisions', url: 'https://data.cityofnewyork.us/resource/h9gi-nx95.json', pk: 'collision_id', dateField: 'crash_date',
    transform: (r) => ({ collision_id: r.collision_id, crash_date: r.crash_date, crash_time: r.crash_time, borough: r.borough, zip_code: r.zip_code, lat: r.latitude ? parseFloat(r.latitude) : null, lng: r.longitude ? parseFloat(r.longitude) : null, on_street: r.on_street_name, cross_street: r.cross_street_name, persons_injured: parseInt(r.number_of_persons_injured||0), persons_killed: parseInt(r.number_of_persons_killed||0), contributing_factor: r.contributing_factor_vehicle_1, vehicle_type: r.vehicle_type_code1 }) },
  { id: 'nypd_complaints', url: 'https://data.cityofnewyork.us/resource/qgea-i56i.json', pk: 'complaint_num', dateField: 'cmplnt_fr_dt',
    transform: (r) => ({ complaint_num: r.cmplnt_num, occurred_at: r.cmplnt_fr_dt, offense_desc: r.ofns_desc, law_cat: r.law_cat_cd, borough: r.boro_nm, lat: r.latitude ? parseFloat(r.latitude) : null, lng: r.longitude ? parseFloat(r.longitude) : null, patrol_boro: r.patrol_boro, premise_desc: r.prem_typ_desc }) }
];
export default async function handler(req, res) {
  if (req.headers['x-ingest-secret'] !== process.env.INGEST_SECRET) return res.status(401).json({ error: 'unauthorized' });
  const target = req.query.dataset || 'all';
  const results = {};
  for (const ds of DATASETS) {
    if (target !== 'all' && target !== ds.id) continue;
    try {
      const url = ds.url + `?$limit=50000&$order=${ds.dateField} DESC&$where=${ds.dateField} > '${ninetyDaysAgo()}'`;
      const r = await fetch(url);
      const rows = await r.json();
      if (!Array.isArray(rows)) { results[ds.id] = { error: 'bad response' }; continue; }
      const records = rows.map(ds.transform).filter(r => r.lat && r.lng);
      let upserted = 0;
      for (let i = 0; i < records.length; i += 500) {
        const { error } = await sb.from(ds.id).upsert(records.slice(i,i+500), { onConflict: ds.pk, ignoreDuplicates: true });
        if (error) { results[ds.id] = { error: error.message }; break; }
        upserted += 500;
      }
      results[ds.id] = { rows: rows.length, upserted };
    } catch(e) { results[ds.id] = { error: e.message }; }
  }
  return res.json({ ok: true, results, ts: new Date().toISOString() });
}
