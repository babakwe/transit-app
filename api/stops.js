export default async function handler(req, res) {
  const { minLat, maxLat, minLng, maxLng } = req.query;
  if (!minLat || !maxLat || !minLng || !maxLng) {
    return res.status(400).json({ error: 'minLat, maxLat, minLng, maxLng required' });
  }
  const centerLat = (parseFloat(minLat) + parseFloat(maxLat)) / 2;
  const centerLng = (parseFloat(minLng) + parseFloat(maxLng)) / 2;
  const latSpan = parseFloat(maxLat) - parseFloat(minLat);
  const lngSpan = parseFloat(maxLng) - parseFloat(minLng);
  const radius = Math.max(latSpan, lngSpan) * 111000 / 2;
  const clampedRadius = Math.min(Math.max(radius, 100), 1500);
  const key = process.env.MTA_API_KEY || '';
  const url = `https://bustime.mta.info/api/where/stops-for-location.json?lat=${centerLat}&lon=${centerLng}&radius=${clampedRadius}&key=${key}`;
  try {
    const r = await fetch(url);
    const d = await r.json();
    const stops = (d?.data?.stops || []).map(s => ({
      id: 'MTA_' + s.code,
      stopId: s.code,
      name: s.name,
      lat: s.lat,
      lng: s.lon,
      routes: (s.routes || []).map(r => r.shortName).filter(Boolean),
      direction: s.direction || '',
    }));
    res.setHeader('Cache-Control', 's-maxage=3600');
    return res.status(200).json({ stops });
  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
}