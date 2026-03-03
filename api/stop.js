// /api/stops — fetches real MTA stop coordinates for a viewport
// Uses MTA BusTime stops-for-location API (real lat/lng, real stop IDs)

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'public, max-age=3600');

  const { minLat, maxLat, minLng, maxLng } = req.query;
  if (!minLat) return res.status(400).json({ error: 'bounds required' });

  const MTA_KEY = process.env.MTA_API_KEY;
  if (!MTA_KEY) return res.status(200).json({ stops: [], error: 'no key' });

  // Calculate center and span of the viewport
  const centerLat = ((parseFloat(minLat) + parseFloat(maxLat)) / 2).toFixed(6);
  const centerLng = ((parseFloat(minLng) + parseFloat(maxLng)) / 2).toFixed(6);
  const latSpan   = (parseFloat(maxLat) - parseFloat(minLat)).toFixed(4);
  const lonSpan   = (parseFloat(maxLng) - parseFloat(minLng)).toFixed(4);

  try {
    const url = `https://bustime.mta.info/api/where/stops-for-location.json` +
      `?lat=${centerLat}&lon=${centerLng}&latSpan=${latSpan}&lonSpan=${lonSpan}` +
      `&key=${MTA_KEY}`;

    const r = await fetch(url, { signal: AbortSignal.timeout(6000) });
    if (!r.ok) throw new Error(`MTA returned ${r.status}`);
    const data = await r.json();

    const stops = (data?.data?.stops || []).map(s => ({
      id:      s.id,
      stopId:  s.id,
      name:    s.name,
      lat:     s.lat,
      lng:     s.lon,
      routes:  (s.routes || []).map(r => r.shortName || r.id?.split('_').pop() || ''),
    }));

    return res.status(200).json({ stops, count: stops.length });
  } catch (e) {
    console.error('stops-for-location failed:', e.message);
    return res.status(200).json({ stops: [], error: e.message });
  }
}
