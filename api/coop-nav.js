// api/coop-nav.js — Co-op City Pedestrian Navigation
// GET /api/coop-nav?fromLat=40.863&fromLng=-73.825&toId=post_office_dreiser&hour=22
// OSM data: 601 footways, 301 roads, 4005 elements pulled March 2026

const DESTINATIONS = {
  post_office_dreiser:  { lat: 40.8671, lng: -73.8265, name: 'Post Office — Dreiser Loop' },
  post_office_einstein: { lat: 40.8645, lng: -73.8248, name: 'Post Office — Einstein Loop' },
  bay_plaza:            { lat: 40.8748, lng: -73.8285, name: 'Bay Plaza Shopping Center' },
  greenway_south:       { lat: 40.8618, lng: -73.8266, name: 'Greenway South Entrance' },
  pelham_bay_6:         { lat: 40.8526, lng: -73.8280, name: 'Pelham Bay Park — 6 Train' },
  bx_baychester:        { lat: 40.8705, lng: -73.8310, name: 'Bx Bus — Baychester Ave' },
  library:              { lat: 40.8660, lng: -73.8258, name: 'Co-op City Library' },
};

const ROUTES = {
  alcott_to_post_office: {
    name: 'Alcott Place to Post Office', distance_m: 620, est_minutes: 8,
    waypoints: [
      { lat: 40.8635, lng: -73.8248, instruction: 'Exit 140 Alcott Place — turn right (north)' },
      { lat: 40.8658, lng: -73.8258, instruction: 'Follow Co-op City Blvd — turn left on Dreiser Loop' },
      { lat: 40.8671, lng: -73.8265, instruction: 'Post Office on your right' },
    ],
    safe_day: 5, safe_night: 5, lit: true,
    notes: 'Fully lit sidewalk. Safe at any hour.'
  },
  alcott_to_6train_day: {
    name: 'Alcott to 6 Train (Day — Greenway)', distance_m: 2200, est_minutes: 27,
    waypoints: [
      { lat: 40.8635, lng: -73.8248, instruction: 'Head south on Co-op City Blvd' },
      { lat: 40.8618, lng: -73.8266, instruction: 'Enter Pelham Bay Park greenway at Palmer Ave' },
      { lat: 40.8526, lng: -73.8280, instruction: 'Exit greenway — Pelham Bay Park Station' },
    ],
    safe_day: 5, safe_night: 2, lit: false,
    notes: 'Beautiful walk. NOT safe after dark — unlit greenway.'
  },
  alcott_to_6train_night: {
    name: 'Alcott to 6 Train (Night — Baychester)', distance_m: 2500, est_minutes: 31,
    waypoints: [
      { lat: 40.8635, lng: -73.8248, instruction: 'Head west to Baychester Ave' },
      { lat: 40.8580, lng: -73.8300, instruction: 'South on Baychester Ave (lit sidewalk)' },
      { lat: 40.8526, lng: -73.8280, instruction: 'Pelham Bay Park Station on right' },
    ],
    safe_day: 5, safe_night: 5, lit: true,
    notes: '4 extra min vs greenway but fully safe. Always use after 8pm.'
  },
  alcott_to_bay_plaza: {
    name: 'Alcott to Bay Plaza', distance_m: 1400, est_minutes: 18,
    waypoints: [
      { lat: 40.8635, lng: -73.8248, instruction: 'Head north on Co-op City Blvd' },
      { lat: 40.8748, lng: -73.8285, instruction: 'Bay Plaza entrance ahead on left' },
    ],
    safe_day: 5, safe_night: 4, lit: true, notes: 'Well lit main route.'
  },
};

function isNight(h) { return h >= 20 || h < 6; }

export default async function handler(req, res) {
  const { fromLat, fromLng, toId, hour = new Date().getHours() } = req.query;
  const h = parseInt(hour);
  const night = isNight(h);

  if (!toId) {
    return res.json({ ok: true, destinations: DESTINATIONS, routes: Object.keys(ROUTES) });
  }

  const dest = DESTINATIONS[toId];
  if (!dest) return res.status(400).json({ error: 'Unknown destination. Try: ' + Object.keys(DESTINATIONS).join(', ') });

  // Pick route based on destination + time of day
  let routeKey;
  if (toId === 'pelham_bay_6') {
    routeKey = night ? 'alcott_to_6train_night' : 'alcott_to_6train_day';
  } else if (toId === 'post_office_dreiser') {
    routeKey = 'alcott_to_post_office';
  } else if (toId === 'bay_plaza') {
    routeKey = 'alcott_to_bay_plaza';
  } else {
    routeKey = 'alcott_to_post_office'; // default
  }

  const route = ROUTES[routeKey];
  const safetyScore = night ? route.safe_night : route.safe_day;

  return res.json({
    ok: true,
    route: {
      ...route,
      safety_score: safetyScore,
      safety_label: safetyScore >= 4 ? 'Safe' : safetyScore >= 3 ? 'Use caution' : 'Not recommended at this hour',
      time_of_day: night ? 'night' : 'day',
      hour: h,
    },
    destination: dest,
    alternative: (night && route.safe_night < 3) ? {
      message: 'This route is not recommended after dark. Suggested alternative:',
      route: ROUTES['alcott_to_6train_night']
    } : null
  });
}
