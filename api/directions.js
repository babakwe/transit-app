export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const { origin, destination } = req.query;
  if (!origin || !destination) return res.status(400).json({ error: 'origin and destination required' });

  const key = process.env.GOOGLE_MAPS_API_KEY;
  if (!key) return res.status(500).json({ error: 'No Google Maps API key configured' });

  try {
    const url = `https://maps.googleapis.com/maps/api/directions/json?origin=${encodeURIComponent(origin)}&destination=${encodeURIComponent(destination)}&mode=transit&alternatives=true&departure_time=now&key=${key}`;
    const r = await fetch(url);
    const data = await r.json();
    if (data.status !== 'OK') return res.status(200).json({ routes: [], status: data.status });
    return res.status(200).json({
      routes: data.routes.map(route => ({ legs: route.legs, summary: route.summary }))
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
