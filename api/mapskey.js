export default function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const key = process.env.GOOGLE_MAPS_API_KEY;
  if (!key) return res.status(500).json({ error: 'No key configured' });
  res.status(200).json({ key });
}
