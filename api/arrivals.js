export default async function handler(req, res) {
  const { stopId, route } = req.query;
  if (!stopId || !route) return res.status(400).json({ error: 'stopId and route required' });
  const key = process.env.MTA_API_KEY || '';
  const routeUpper = route.toUpperCase();
  const agency = routeUpper.startsWith('BXM') || routeUpper.startsWith('QM') || routeUpper.startsWith('X') ? 'MTA BC' : 'MTA NYCT';
  const lineRef = encodeURIComponent(agency + '_' + routeUpper);
  const url = 'https://bustime.mta.info/api/siri/stop-monitoring.json?key=' + key + '&MonitoringRef=' + stopId + '&LineRef=' + lineRef;
  try {
    const r = await fetch(url);
    const d = await r.json();
    const visits = d && d.Siri && d.Siri.ServiceDelivery && d.Siri.ServiceDelivery.StopMonitoringDelivery && d.Siri.ServiceDelivery.StopMonitoringDelivery[0] && d.Siri.ServiceDelivery.StopMonitoringDelivery[0].MonitoredStopVisit || [];
    const arrivals = visits.map(function(v) {
      const journey = v.MonitoredVehicleJourney;
      const call = journey && journey.MonitoredCall;
      const eta = call && (call.ExpectedArrivalTime || call.AimedArrivalTime);
      const mins = eta ? Math.round((new Date(eta) - Date.now()) / 60000) : null;
      const routeName = Array.isArray(journey.PublishedLineName) ? journey.PublishedLineName[0] : journey.PublishedLineName || route;
      const headsign = Array.isArray(journey.DestinationName) ? journey.DestinationName[0] : journey.DestinationName || '';
      return { route: routeName, headsign: headsign, mins: mins };
    })
    .filter(function(a) { return a.mins !== null && a.mins >= 0; })
    .sort(function(a, b) { return a.mins - b.mins; })
    .slice(0, 4);
    res.setHeader('Cache-Control', 'no-store');
    return res.status(200).json({ arrivals: arrivals });
  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
}