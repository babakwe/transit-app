export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const apiKey = process.env.MTA_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'MTA_API_KEY not configured' });

  const { route, direction } = req.query;
  if (!route) return res.status(400).json({ error: 'route required' });

  // direction: 0 = outbound, 1 = inbound (default 0)
  const dir = direction || '0';

  try {
    // Clean route name — strip + (SBS suffix), uppercase
    const cleanRoute = route.replace('+','').toUpperCase();
    // MTA BusTime stops-for-route endpoint
    const url = new URL('https://bustime.mta.info/api/where/stops-for-route/MTA_NYCT_' + encodeURIComponent(cleanRoute) + '.json');
    url.searchParams.set('key', apiKey);
    url.searchParams.set('version', '2');
    url.searchParams.set('includePolylines', 'true');

    const response = await fetch(url.toString(), {signal: AbortSignal.timeout(8000)});
    if (!response.ok) throw new Error(`MTA API ${response.status}`);

    const data = await response.json();
    const entry = data?.data;

    if (!entry) return res.status(404).json({ error: 'Route not found', route });

    // Get stop groups (directions)
    const stopGroups = entry?.entry?.stopGroupings?.[0]?.stopGroups || [];

    // Pick the right direction group
    const group = stopGroups[dir] || stopGroups[0];
    if (!group) return res.status(404).json({ error: 'No stop groups found' });

    // Stop IDs in order
    const stopIds = group?.stopIds || [];

    // Build a lookup map of all stops
    const allStops = {};
    (entry?.references?.stops || []).forEach(s => {
      allStops[s.id] = {
        id: s.id,
        stopId: s.id.replace('MTA_', '').replace('MTA NYCT_', ''),
        name: s.name,
        lat: s.lat,
        lng: s.lon,
        direction: s.direction,
      };
    });

    // Return stops in route order
    const stops = stopIds
      .map(id => allStops[id] || allStops['MTA_' + id])
      .filter(Boolean);

    // Also return polyline if available
    const polylines = group?.polylines || [];

    // Decode first polyline into coordinates
    let coords = [];
    if (polylines[0]?.points) {
      coords = decodePolyline(polylines[0].points);
    }

    // Get direction names
    const directions = stopGroups.map((g, i) => ({
      index: i,
      name: g?.name?.name || `Direction ${i}`,
    }));

    res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate=7200');
    return res.status(200).json({ stops, coords, directions, route, direction: dir });

  } catch (err) {
    console.error('route-stops error:', err);
    return res.status(500).json({ error: err.message });
  }
}

// Google encoded polyline decoder
function decodePolyline(encoded) {
  const coords = [];
  let index = 0, lat = 0, lng = 0;
  while (index < encoded.length) {
    let b, shift = 0, result = 0;
    do { b = encoded.charCodeAt(index++) - 63; result |= (b & 0x1f) << shift; shift += 5; } while (b >= 0x20);
    lat += (result & 1) ? ~(result >> 1) : (result >> 1);
    shift = 0; result = 0;
    do { b = encoded.charCodeAt(index++) - 63; result |= (b & 0x1f) << shift; shift += 5; } while (b >= 0x20);
    lng += (result & 1) ? ~(result >> 1) : (result >> 1);
    coords.push([lng / 1e5, lat / 1e5]);
  }
  return coords;
}
