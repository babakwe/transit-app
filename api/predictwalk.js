export default async function handler(req, res) {

  if (req.method !== "POST") {
    return res.status(405).json({ error: "POST only" })
  }

  try {

    const body = req.body

    const mapboxDuration = body.mapbox_duration_sec
    const mapboxDistance = body.mapbox_distance_m

    const buffer = 120

    const predicted = mapboxDuration

    const total = predicted + buffer

    return res.json({
      predicted_walk_sec: predicted,
      buffer_sec: buffer,
      recommended_total_sec: total,
      distance_m: mapboxDistance,
      method: "baseline_mapbox"
    })

  } catch (err) {

    console.error(err)

    return res.status(500).json({
      error: "prediction_failed"
    })

  }

}
