module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'public, max-age=1800');
  const { route } = req.query;
  const MTA_KEY = process.env.MTA_API_KEY;
  if (!MTA_KEY) return res.status(200).json({ stops: [], error: 'no key' });
  if (!route) return res.status(400).json({ error: 'route required' });
  const agencyRoute = `MTA NYCT_${route.replace('+', '-SBS')}`;
  try {
    const url = `https://bustime.mta.info/api/where/stops-for-route/${encodeURIComponent(agencyRoute)}.json?key=${MTA_KEY}&includePolylines=false&version=2`;
    const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
    if (!r.ok) throw new Error(`MTA ${r.status}`);
    const data = await r.json();
    const groupings = data?.data?.stopGroupings || [];
    let stops = [];
    for (const grouping of groupings) {
      for (const group of grouping.stopGroups || []) {
        const stopIds = group.stopIds || [];
        const stopRefs = data?.data?.references?.stops || [];
        const stopMap = {};
        stopRefs.forEach(s => { stopMap[s.id] = s; });
        const dirStops = stopIds.map(id => {
          const s = stopMap[id];
          if (!s) return null;
          return { id: id.replace('MTA_',''), name: s.name, lat: s.lat, lng: s.lon };
        }).filter(Boolean);
        if (dirStops.length > stops.length) stops = dirStops;
      }
    }
    return res.status(200).json({ stops, count: stops.length, route });
  } catch (e) {
    return res.status(200).json({ stops: [], error: e.message });
  }
};
