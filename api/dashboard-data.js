export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_SERVICE_KEY;
  if (!supabaseUrl || !supabaseKey) return res.status(500).json({ error: 'Supabase not configured' });

  const headers = {
    'apikey': supabaseKey,
    'Authorization': `Bearer ${supabaseKey}`,
    'Content-Type': 'application/json',
  };

  const q = (query) => fetch(`${supabaseUrl}/rest/v1/rpc/run_query`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ query })
  });

  // Use direct table queries instead of RPC
  const base = `${supabaseUrl}/rest/v1/bus_observations`;

  try {
    const [totalRes, routesRes, hoursRes, recentRes, stopsRes] = await Promise.all([
      // Total count
      fetch(`${base}?select=id&limit=1`, { headers: { ...headers, 'Prefer': 'count=exact', 'Range': '0-0' } }),
      // By route
      fetch(`${base}?select=route,id&order=route`, { headers }),
      // By hour
      fetch(`${base}?select=hour_of_day,id&order=hour_of_day`, { headers }),
      // Recent 20
      fetch(`${base}?select=*&order=observed_at.desc&limit=20`, { headers }),
      // Top stops
      fetch(`${base}?select=stop_name,id&order=stop_name&limit=500`, { headers }),
    ]);

    const [routesData, hoursData, recentData, stopsData] = await Promise.all([
      routesRes.json(),
      hoursRes.json(),
      recentRes.json(),
      stopsRes.json(),
    ]);

    const totalCount = totalRes.headers.get('content-range')?.split('/')[1] || 0;

    // Group routes
    const routeCounts = {};
    (routesData || []).forEach(r => { routeCounts[r.route] = (routeCounts[r.route] || 0) + 1; });

    // Group hours
    const hourCounts = Array(24).fill(0);
    (hoursData || []).forEach(r => { if (r.hour_of_day != null) hourCounts[r.hour_of_day]++; });

    // Group stops
    const stopCounts = {};
    (stopsData || []).forEach(r => { stopCounts[r.stop_name] = (stopCounts[r.stop_name] || 0) + 1; });
    const topStops = Object.entries(stopCounts).sort((a,b) => b[1]-a[1]).slice(0,10);

    return res.status(200).json({
      total: parseInt(totalCount) || (routesData?.length || 0),
      routes: Object.entries(routeCounts).sort((a,b) => b[1]-a[1]),
      hours: hourCounts,
      recent: recentData || [],
      topStops,
    });
  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
}
