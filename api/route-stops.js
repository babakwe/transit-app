export default async function handler(req, res) {
  const { route } = req.query;
  if (!route) return res.status(400).json({ error: 'route required' });
  const key = process.env.MTA_API_KEY || '';
  const routeUpper = route.toUpperCase();
  const agency = routeUpper.startsWith('BXM') || routeUpper.startsWith('QM') || routeUpper.startsWith('X') ? 'MTA BC' : 'MTA NYCT';
  const routeId = encodeURIComponent(agency + '_' + routeUpper);
  const url = 'https://bustime.mta.info/api/where/stops-for-route/' + routeId + '.json?key=' + key + '&includePolylines=false';
  try {
    const r = await fetch(url);
    const d = await r.json();
    if (d.code !== 200) return res.status(404).json({ error: 'MTA API ' + d.code });
    const data = d.data;
    const stopMap = {};
    (data.stops || []).forEach(function(s) {
      stopMap[s.id] = { id: s.id, stopId: s.code, name: s.name, lat: s.lat, lng: s.lon, direction: s.direction || '' };
    });
    const groups = (data.stopGroupings && data.stopGroupings[0] && data.stopGroupings[0].stopGroups) || [];
    const directions = groups.map(function(g) {
      return {
        name: g.name && g.name.name || '',
        stops: (g.stopIds || []).map(function(id) { return stopMap[id]; }).filter(Boolean)
      };
    });
    res.setHeader('Cache-Control', 's-maxage=86400');
    return res.status(200).json({ route: routeUpper, directions: directions });
  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
}