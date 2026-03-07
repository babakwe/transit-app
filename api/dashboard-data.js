// api/dashboard-data.js
// Reads from bus_arrivals table and returns adherence analytics.

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_SERVICE_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: 'Supabase not configured' });
  }

  const headers = {
    'apikey':        supabaseKey,
    'Authorization': `Bearer ${supabaseKey}`,
    'Content-Type':  'application/json',
  };

  const base = `${supabaseUrl}/rest/v1/bus_arrivals`;

  try {
    const [totalRes, routesRes, hoursRes, recentRes, stopsRes, adherenceRes] = await Promise.all([
      // Total count
      fetch(`${base}?select=id&limit=1`, {
        headers: { ...headers, 'Prefer': 'count=exact', 'Range': '0-0' }
      }),
      // By route
      fetch(`${base}?select=route,adherence_seconds&order=route`, { headers }),
      // By hour
      fetch(`${base}?select=hour_of_day,adherence_seconds&order=hour_of_day`, { headers }),
      // Recent 20
      fetch(`${base}?select=*&order=recorded_at.desc&limit=20`, { headers }),
      // Top stops
      fetch(`${base}?select=stop_name,adherence_seconds&limit=2000`, { headers }),
      // Adherence summary: last 500 arrivals with aimed_arrival
      fetch(`${base}?select=adherence_seconds&aimed_arrival=not.is.null&order=recorded_at.desc&limit=500`, { headers }),
    ]);

    const [routesData, hoursData, recentData, stopsData, adherenceData] = await Promise.all([
      routesRes.json(),
      hoursRes.json(),
      recentRes.json(),
      stopsRes.json(),
      adherenceRes.json(),
    ]);

    const totalCount = totalRes.headers.get('content-range')?.split('/')[1] || 0;

    // ── Routes ──────────────────────────────────────────────
    const routeMap = {};
    (routesData || []).forEach(r => {
      if (!routeMap[r.route]) routeMap[r.route] = { count: 0, totalAdherence: 0, withAdherence: 0 };
      routeMap[r.route].count++;
      if (r.adherence_seconds != null) {
        routeMap[r.route].totalAdherence += r.adherence_seconds;
        routeMap[r.route].withAdherence++;
      }
    });
    const routes = Object.entries(routeMap)
      .map(([route, d]) => [
        route,
        d.count,
        d.withAdherence > 0 ? Math.round(d.totalAdherence / d.withAdherence) : null
      ])
      .sort((a, b) => b[1] - a[1]);

    // ── Hours ──────────────────────────────────────────────
    const hourCounts = Array(24).fill(0);
    const hourAdherence = Array(24).fill(null).map(() => ({ total: 0, count: 0 }));
    (hoursData || []).forEach(r => {
      const h = r.hour_of_day;
      if (h != null) {
        hourCounts[h]++;
        if (r.adherence_seconds != null) {
          hourAdherence[h].total += r.adherence_seconds;
          hourAdherence[h].count++;
        }
      }
    });
    const hourAvgAdherence = hourAdherence.map(h =>
      h.count > 0 ? Math.round(h.total / h.count) : null
    );

    // ── Stops ──────────────────────────────────────────────
    const stopMap = {};
    (stopsData || []).forEach(r => {
      if (!stopMap[r.stop_name]) stopMap[r.stop_name] = { count: 0, total: 0, n: 0 };
      stopMap[r.stop_name].count++;
      if (r.adherence_seconds != null) {
        stopMap[r.stop_name].total += r.adherence_seconds;
        stopMap[r.stop_name].n++;
      }
    });
    const topStops = Object.entries(stopMap)
      .map(([name, d]) => [
        name,
        d.count,
        d.n > 0 ? Math.round(d.total / d.n) : null
      ])
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);

    // ── Overall adherence summary ──────────────────────────
    const adh = (adherenceData || []).map(r => r.adherence_seconds).filter(x => x != null);
    const onTime  = adh.filter(s => s >= -60 && s <= 300).length;
    const late    = adh.filter(s => s >  300).length;
    const early   = adh.filter(s => s <  -60).length;
    const avgAdh  = adh.length > 0 ? Math.round(adh.reduce((a,b) => a+b, 0) / adh.length) : null;

    return res.status(200).json({
      total:       parseInt(totalCount) || 0,
      routes,                  // [ [routeName, count, avgAdherenceSec], ... ]
      hours:       hourCounts,
      hourAvgAdherence,        // avg seconds late per hour (null if no data)
      recent:      recentData || [],
      topStops,                // [ [stopName, count, avgAdherenceSec], ... ]
      adherence: {
        total:    adh.length,
        onTime,
        late,
        early,
        avgSeconds: avgAdh,
        onTimePct: adh.length > 0 ? Math.round(onTime / adh.length * 100) : null,
      },
    });

  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
