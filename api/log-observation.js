export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_SERVICE_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: 'Supabase not configured' });
  }

  const observations = Array.isArray(req.body) ? req.body : [req.body];
  const now = new Date();

  const rows = observations.map(o => ({
    route:          o.route || null,
    stop_id:        o.stopId || null,
    stop_name:      o.stopName || null,
    direction:      o.direction || null,
    headsign:       o.headsign || null,
    minutes_away:   typeof o.mins === 'number' ? o.mins : null,
    passenger_count:typeof o.passengers === 'number' ? o.passengers : null,
    mode:           o.mode || 'bus',
    day_of_week:    now.getDay(),
    hour_of_day:    now.getHours(),
    observed_at:    now.toISOString(),
  }));

  try {
    const r = await fetch(`${supabaseUrl}/rest/v1/bus_observations`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'apikey': supabaseKey,
        'Authorization': `Bearer ${supabaseKey}`,
        'Prefer': 'return=minimal',
      },
      body: JSON.stringify(rows),
    });

    if (!r.ok) {
      const err = await r.text();
      throw new Error(err);
    }

    return res.status(200).json({ logged: rows.length });
  } catch (e) {
    console.error('Supabase log error:', e.message);
    return res.status(500).json({ error: e.message });
  }
}
