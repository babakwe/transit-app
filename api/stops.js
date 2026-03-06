export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const apiKey = process.env.MTA_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'MTA_API_KEY not configured' });

  const { minLat, maxLat, minLng, maxLng } = req.query;
  if (!minLat || !maxLat || !minLng || !maxLng) {
    return res.status(400).json({ error: 'minLat, maxLat, minLng, maxLng required' });
  }

  try {
    const url = new URL('https://bustime.mta.info/api/where/stops-for-location.json');
    url.searchParams.set('key', apiKey);
    url.searchParams.set('lat', ((parseFloat(minLat) + parseFloat(maxLat)) / 2).toString());
    url.searchParams.set('lon', ((parseFloat(minLng) + parseFloat(maxLng)) / 2).toString());
    url.searchParams.set('latSpan', (parseFloat(maxLat) - parseFloat(minLat)).toString());
    url.searchParams.set('lonSpan', (parseFloat(maxLng) - parseFloat(minLng)).toString());
    url.searchParams.set('version', '2');

    const response = await fetch(url.toString(), {
      signal: AbortSignal.timeout(8000)
    });

    if (!response.ok) throw new Error(`MTA API ${response.status}`);

    const data = await response.json();
    const stops = (data?.data?.list || []).map(s => ({
      id: s.id,
      stopId: s.id.replace('MTA_', '').replace('MTA NYCT_', ''),
      name: s.name,
      lat: s.lat,
      lng: s.lon,
      routes: (s.routes || []).map(r => r.shortName || r.id),
      direction: s.direction,
    }));

    res.setHeader('Cache-Control', 's-maxage=86400, stale-while-revalidate=604800');
    return res.status(200).json({ stops });
  } catch (err) {
    console.error('stops error:', err);
    return res.status(500).json({ error: err.message });
  }
}
