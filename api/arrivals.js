export default async function handler(req, res) {
  const { stopId, route, debug } = req.query;
  if (!stopId || !route) return res.status(400).json({ error: 'stopId and route required' });
  const key = process.env.MTA_API_KEY || '';
  const agency = route.startsWith('BxM') || route.startsWith('QM') || route.startsWith('X') ? 'MTA BC' : 'MTA NYCT';
  const lineRef = encodeURIComponent(agency + '_' + route);
  const url = `https://bustime.mta.info/api/siri/stop-monitoring.json?key=${key}&MonitoringRef=${stopId}&LineRef=${lineRef}`;
  try {
    const r = await fetch(url);
    const d = await r.json();
    if (debug) return res.status(200).json({ url: url.replace(key, 'REDACTED'), raw: d });
    const visits = d?.Siri?.ServiceDelivery?.StopMonitoringDelivery?.[0]?.MonitoredStopVisit || [];
    const arrivals = visits.map(v => {
      const journey = v.MonitoredVehicleJourney;
      const call = journey?.MonitoredCall;
      const eta = call?.ExpectedArrivalTime || call?.AimedArrivalTime;
      const mins = eta ? Math.round((new Date(eta) - Date.now()) / 60000) : null;
      return {
        route: journey?.PublishedLineName?.[0] || route,
        headsign: journey?.DestinationName?.[0] || '',
        mins: mins ?? 0,
      };
    })
    .filter(a => a.mins !== null && a.mins >= 0)
    .sort((a, b) => a.mins - b.mins)
    .slice(0, 4);
    res.setHeader('Cache-Control', 'no-store');
    return res.status(200).json({ arrivals });
  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
}