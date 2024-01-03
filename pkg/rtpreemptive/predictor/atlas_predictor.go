package predictor

import (
	"time"
)

/*
#cgo LDFLAGS: -lm
#include "llsp.h"
*/
import "C"

type Metrics []float64

type Predictor interface {
	Add(metrics Metrics, actualExecTime time.Duration)
	EstimateExecTime(metrics Metrics) time.Duration
}

type atlasPredictor struct {
	msize  int
	solver *C.llsp_t
}

func NewATLASPredictor(metricSize int) Predictor {
	return &atlasPredictor{
		msize:  metricSize,
		solver: C.llsp_new(C.size_t(metricSize)),
	}
}

func (a *atlasPredictor) padMetrics(metrics Metrics) Metrics {
	maxSize := 4
	if size := len(metrics); size < maxSize {
		maxSize = size
	}
	var m []float64
	m = append(m, metrics[:maxSize]...)
	// add zeroes as padding
	for i := len(metrics) - 1; i < a.msize; i++ {
		m = append(m, 0)
	}
	return m
}

func (a *atlasPredictor) Add(metrics Metrics, actualExecTime time.Duration) {
	C.llsp_add(a.solver, (*C.double)(&a.padMetrics(metrics)[0]), C.double(actualExecTime))
	C.llsp_solve(a.solver)
}

// EstimateExecTime returns the estimated execution time based on a set of metrics
func (a *atlasPredictor) EstimateExecTime(metrics Metrics) time.Duration {
	prediction := C.llsp_predict(a.solver, (*C.double)(&a.padMetrics(metrics)[0]))
	return time.Duration(prediction)
}
