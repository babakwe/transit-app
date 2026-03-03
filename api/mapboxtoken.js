export default function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const token = process.env.MAPBOX_TOKEN;
  if (!token) return res.status(500).json({ error: 'No token configured' });
  res.status(200).json({ token });
}
