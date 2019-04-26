package daemon

// Gauge is a Metric that represents a single numerical value that can
// arbitrarily go up and down.
type Gauge interface {
	// Inc increments the Gauge by 1. Use Add to increment it by arbitrary
	// values.
	Inc()
}

// GaugeVec is a Collector that bundles a set of Gauges that all share the same
// Desc, but have different values for their variable labels.
type GaugeVec interface {
	// WithLabelValues works as GetMetricWithLabelValues, but panics where
	// GetMetricWithLabelValues would have returned an error. By not returning an
	// error, WithLabelValues allows shortcuts like
	//     myVec.WithLabelValues("404", "GET").Add(42)
	WithLabelValues(lvs ...string) Gauge
}

// A Histogram counts individual observations from an event or sample stream in
// configurable buckets. Similar to a summary, it also provides a sum of
// observations and an observation count.
type Histogram interface {
	// Observe adds a single observation to the histogram.
	Observe(float64)
}

// HistogramVec is a Collector that bundles a set of Histograms that all share the
// same Desc, but have different values for their variable labels.
type HistogramVec interface {
	// WithLabelValues works as GetMetricWithLabelValues, but panics where
	// GetMetricWithLabelValues would have returned an error. By not returning an
	// error, WithLabelValues allows shortcuts like
	//     myVec.WithLabelValues("404", "GET").Observe(42.21)
	WithLabelValues(lvs ...string) Histogram
}

type APIMetrics struct {
	apiDuration      HistogramVec
	connectedClients GaugeVec
}

// NewAPIMetrics creates a new APIMetrics with sane defaults
func NewAPIMetrics(apiDuration HistogramVec, connectedClients GaugeVec) APIMetrics {
	return APIMetrics{
		apiDuration:      apiDuration,
		connectedClients: connectedClients,
	}
}

// APIDuration returns the HistogramVec for collecting api duration metrics
func (a APIMetrics) APIDuration() HistogramVec {
	return a.apiDuration
}

// ConnectedClients returns the GaugeVec for collecting the number of connected
// client metrics
func (a APIMetrics) ConnectedClients() GaugeVec {
	return a.connectedClients
}
