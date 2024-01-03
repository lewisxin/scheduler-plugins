package laxity

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	gocache "github.com/patrickmn/go-cache"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	"sigs.k8s.io/scheduler-plugins/pkg/rtpreemptive/annotations"
	"sigs.k8s.io/scheduler-plugins/pkg/rtpreemptive/deadline"
	"sigs.k8s.io/scheduler-plugins/pkg/rtpreemptive/predictor"
)

const (
	// PredictorMetricSize describes the size of the metrics expected to be passed to ATLAS LLSP solver.
	// When a metrics passed has more items, it will be truncated.
	// If there are less items, 0 will be added for padding
	PredictorMetricSize = 10
)

var (
	ErrNotFound = errors.New("pod execution not found in laxity manager")
)

type Manager interface {
	// StartPodExecution starts the pod's execution
	StartPodExecution(pod *v1.Pod)
	// PausePodExecution pause the pod's execution
	PausePodExecution(pod *v1.Pod)
	// GetPodLaxity returns a pod's laxity
	GetPodLaxity(pod *v1.Pod) (time.Duration, error)
	// RemovePodExecution deletes the tracked execution of a pod
	RemovePodExecution(pod *v1.Pod)
}

type laxityManager struct {
	deadlineManager deadline.Manager
	atlas           predictor.Predictor
	podExecutions   *gocache.Cache
}

func NewLaxityManager() Manager {
	return &laxityManager{
		deadlineManager: deadline.NewDeadlineManager(),
		atlas:           predictor.NewATLASPredictor(PredictorMetricSize),
		podExecutions:   gocache.New(time.Second, time.Second),
	}
}

func getPodResources(pod *v1.Pod) (req, limit v1.ResourceList) {
	requests := resource.PodRequests(pod, resource.PodResourcesOptions{})
	limits := resource.PodLimits(pod, resource.PodResourcesOptions{})
	return requests, limits
}

func getPodAdditionalMetrics(pod *v1.Pod) []float64 {
	var metrics []float64
	metricsStr, ok := pod.Annotations[annotations.AnnotationKeyATLASMetrics]
	if !ok {
		return metrics
	}
	metricsTokens := strings.Split(metricsStr, ",")
	for _, token := range metricsTokens {
		num, _ := strconv.ParseFloat(token, 64)
		metrics = append(metrics, num)
	}
	return metrics
}

func getJobIndex(pod *v1.Pod) int {
	token := pod.Annotations["batch.kubernetes.io/job-completion-index"]
	idx, _ := strconv.Atoi(token)
	return idx
}

func getPodExecutionTime(pod *v1.Pod) time.Duration {
	if execTime, err := time.ParseDuration(pod.Annotations[annotations.AnnotationKeyExecTime]); err == nil {
		return execTime
	}
	return 0
}

func getATLASEnabled(pod *v1.Pod) bool {
	enabled, ok := pod.Annotations[annotations.AnnotationKeyATLASEnabled]
	return ok && enabled == "true"
}

func getPodMetrics(pod *v1.Pod) predictor.Metrics {
	var metrics predictor.Metrics
	req, limit := getPodResources(pod)
	metrics = append(metrics, req.Cpu().AsApproximateFloat64(), req.Memory().AsApproximateFloat64())
	metrics = append(metrics, limit.Cpu().AsApproximateFloat64(), limit.Memory().AsApproximateFloat64())
	if ddlRel, err := time.ParseDuration(pod.Annotations[annotations.AnnotationKeyDDL]); err == nil {
		metrics = append(metrics, float64(ddlRel))
	}
	metrics = append(metrics, float64(getJobIndex(pod)))
	metrics = append(metrics, getPodAdditionalMetrics(pod)...)
	return metrics
}

func (l *laxityManager) createPodExecutionIfNotExist(pod *v1.Pod) *podExecution {
	key := toCacheKey(pod)
	podExec, ok := l.podExecutions.Get(key)
	if !ok || podExec == nil {
		ddl := l.deadlineManager.GetPodDeadline(pod)
		execTime := getPodExecutionTime(pod)
		estExecTime := execTime
		if getATLASEnabled(pod) {
			metrics := getPodMetrics(pod)
			estExecTime = l.atlas.EstimateExecTime(metrics)
			if execTime != 0 && math.Abs(float64(execTime-estExecTime))/float64(execTime) > 0.6 {
				// if estimated execution time deviate from the execution time for more than 60%
				// update the model
				l.atlas.Add(metrics, execTime)
				estExecTime = l.atlas.EstimateExecTime(metrics)
			}
			klog.InfoS("laxityManager: EstimateExecTime", "estExecTime", estExecTime, "execTime", execTime, "metrics", metrics, "pod", klog.KObj(pod))
		}
		podExec = &podExecution{
			deadline:    ddl,
			estExecTime: estExecTime,
		}
		l.podExecutions.Add(key, podExec, -1)
	}
	return podExec.(*podExecution)
}

func (l *laxityManager) StartPodExecution(pod *v1.Pod) {
	podExec := l.createPodExecutionIfNotExist(pod)
	podExec.start()
}

func (l *laxityManager) PausePodExecution(pod *v1.Pod) {
	podExec := l.createPodExecutionIfNotExist(pod)
	podExec.pause()
}

func (l *laxityManager) GetPodLaxity(pod *v1.Pod) (time.Duration, error) {
	podExec := l.createPodExecutionIfNotExist(pod)
	laxity, err := podExec.laxity()
	if err == ErrBeyondEstimation {
		metrics := getPodMetrics(pod)
		klog.V(5).ErrorS(ErrBeyondEstimation, "wrongly estimated execution time, updating predictor...", "metrics", metrics, "actualExecTime", podExec.actualExecTime, "pod", klog.KObj(pod))
		l.atlas.Add(metrics, podExec.actualExecTime)
	}
	return laxity, err
}

func (l *laxityManager) RemovePodExecution(pod *v1.Pod) {
	podExec := l.createPodExecutionIfNotExist(pod)
	podExec.pause()
	metrics := getPodMetrics(pod)
	klog.V(5).InfoS("pod execution finished, updating predictor...", "metrics", metrics, "actualExecTime", podExec.actualExecTime, "pod", klog.KObj(pod))
	l.atlas.Add(metrics, podExec.actualExecTime)
	l.podExecutions.Delete(toCacheKey(pod))
}

func toCacheKey(p *v1.Pod) string {
	return fmt.Sprintf("%s/%s", p.Namespace, p.Name)
}
