// /api/stops — fetches real MTA stop coordinates
// Uses stops-for-location to get real coordinates for viewport

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'public, max-age=3600');

  const { minLat, maxLat, minLng, maxLng, stopIds } = req.query;

  const MTA_KEY = process.env.MTA_API_KEY;
  if (!MTA_KEY) return res.status(200).json({ stops: [], error: 'no key' });

  try {
    // If specific stop IDs requested, look them up individually
    if (stopIds) {
      const ids = stopIds.split(',').slice(0, 30);
      const results = await Promise.all(ids.map(async id => {
        try {
          const url = `https://bustime.mta.info/api/where/stop/MTA_${id.trim()}.json?key=${MTA_KEY}`;
          const r = await fetch(url, { signal: AbortSignal.timeout(4000) });
          if (!r.ok) return null;
          const data = await r.json();
          const s = data?.data;
          if (!s) return null;
          return {
            id: id.trim(),
            stopId: id.trim(),
            name: s.name,
            lat: s.lat,
            lng: s.lon,
            routes: (s.routes || []).map(r => r.shortName || r.id?.split('_').pop() || ''),
          };
        } catch { return null; }
      }));
      const stops = results.filter(Boolean);
      return res.status(200).json({ stops, count: stops.length });
    }

    // Otherwise use viewport bounds
    if (!minLat) return res.status(400).json({ error: 'bounds or stopIds required' });

    const centerLat = ((parseFloat(minLat) + parseFloat(maxLat)) / 2).toFixed(6);
    const centerLng = ((parseFloat(minLng) + parseFloat(maxLng)) / 2).toFixed(6);
    const latSpan   = Math.min(parseFloat(maxLat) - parseFloat(minLat), 0.02).toFixed(4);
    const lonSpan   = Math.min(parseFloat(maxLng) - parseFloat(minLng), 0.02).toFixed(4);

    const url = `https://bustime.mta.info/api/where/stops-for-location.json` +
      `?lat=${centerLat}&lon=${centerLng}&latSpan=${latSpan}&lonSpan=${lonSpan}&key=${MTA_KEY}`;

    const r = await fetch(url, { signal: AbortSignal.timeout(6000) });
    if (!r.ok) throw new Error(`MTA ${r.status}`);
    const data = await r.json();

    const stops = (data?.data?.stops || []).map(s => ({
      id:      s.id?.replace('MTA_','') || s.code,
      stopId:  s.id?.replace('MTA_','') || s.code,
      name:    s.name,
      lat:     s.lat,
      lng:     s.lon,
      routes:  (s.routes || []).map(r => r.shortName || r.id?.split('_').pop() || ''),
    }));

    return res.status(200).json({ stops, count: stops.length });
  } catch (e) {
    console.error('stops API error:', e.message);
    return res.status(200).json({ stops: [], error: e.message });
  }
}
