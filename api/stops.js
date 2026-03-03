module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'public, max-age=3600');
  const { minLat, maxLat, minLng, maxLng } = req.query;
  const MTA_KEY = process.env.MTA_API_KEY;
  if (!MTA_KEY) return res.status(200).json({ stops: [], error: 'no key' });
  if (!minLat) return res.status(400).json({ error: 'bounds required' });
  const centerLat = ((parseFloat(minLat) + parseFloat(maxLat)) / 2).toFixed(6);
  const centerLng = ((parseFloat(minLng) + parseFloat(maxLng)) / 2).toFixed(6);
  const latSpan = Math.min(parseFloat(maxLat) - parseFloat(minLat), 0.03).toFixed(4);
  const lonSpan = Math.min(parseFloat(maxLng) - parseFloat(minLng), 0.03).toFixed(4);
  try {
    const url = `https://bustime.mta.info/api/where/stops-for-location.json?lat=${centerLat}&lon=${centerLng}&latSpan=${latSpan}&lonSpan=${lonSpan}&key=${MTA_KEY}`;
    const r = await fetch(url, { signal: AbortSignal.timeout(6000) });
    if (!r.ok) throw new Error(`MTA ${r.status}`);
    const data = await r.json();
    const stops = (data?.data?.stops || []).map(s => ({
      id: s.id?.replace('MTA_', ''),
      stopId: s.id?.replace('MTA_', ''),
      name: s.name,
      lat: s.lat,
      lng: s.lon,
      routes: (s.routes || []).map(r =>
        (r.shortName || r.id?.split('_').pop() || '').toUpperCase().replace('-SBS', '+')
      ).filter(Boolean),
    }));
    return res.status(200).json({ stops, count: stops.length });
  } catch (e) {
    return res.status(200).json({ stops: [], error: e.message });
  }
};
