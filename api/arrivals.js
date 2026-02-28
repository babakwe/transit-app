export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const { stopId, route } = req.query;

  // ── DIRECTIONS endpoint (Google) ──
  if (req.url.includes('/api/directions') || req.query.origin) {
    const { origin, destination } = req.query;
    const key = process.env.GOOGLE_MAPS_API_KEY;
    if (!key) return res.status(500).json({ error: 'No Google API key' });

    const url = `https://maps.googleapis.com/maps/api/directions/json?origin=${encodeURIComponent(origin)}&destination=${encodeURIComponent(destination)}&mode=transit&alternatives=true&key=${key}`;
    try {
      const r = await fetch(url);
      const data = await r.json();
      return res.status(200).json({ routes: data.routes?.map(route => ({
        legs: route.legs,
        summary: route.summary,
      })) || [] });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // ── MTA BusTime endpoint ──
  if (!stopId) return res.status(400).json({ error: 'stopId required' });

  const MTA_KEY = process.env.MTA_API_KEY;
  if (!MTA_KEY) return res.status(500).json({ error: 'No MTA API key' });

  try {
    const url = `https://bustime.mta.info/api/siri/stop-monitoring.json?key=${MTA_KEY}&MonitoringRef=${stopId}&LineRef=${route || ''}&MaximumStopVisits=3`;
    const r = await fetch(url);
    const data = await r.json();

    const visits = data?.Siri?.ServiceDelivery?.StopMonitoringDelivery?.[0]?.MonitoredStopVisit || [];
    const arrivals = visits.map(v => {
      const journey = v.MonitoredVehicleJourney;
      const call = journey?.MonitoredCall;
      const expected = call?.ExpectedArrivalTime || call?.AimedArrivalTime;
      if (!expected) return null;
      const mins = Math.round((new Date(expected) - Date.now()) / 60000);
      return { mins, route: journey?.PublishedLineName, destination: journey?.DestinationName };
    }).filter(a => a && a.mins >= 0);

    return res.status(200).json({ arrivals });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
