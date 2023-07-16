package deadline

import (
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/coscheduling/core"
)

// SimpleDDL implements the QueueSort plugin and schedules Pods in the order of their deadline
type SimpleDDL struct {
	frameworkHandler framework.Handle
}

var _ framework.QueueSortPlugin = &SimpleDDL{}

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name = "SimpleDDL"
	// AnnotationKeyPrefix is the prefix of the annotation key
	AnnotationKeyPrefix = "simpleddl.scheduling.x-k8s.io/"
	// AnnotationKeyDDL is the annotation key represents the relative deadline of a program
	AnnotationKeyDDL = AnnotationKeyPrefix + "ddl"
	// default relative deadline 10 minutes
	defaultDDLRelative = time.Minute * 10
)

// New initializes and returns a new SimpleDDL scheduling plugin.
func New(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return &SimpleDDL{
		frameworkHandler: handle,
	}, nil
}

// Name returns name of the plugin. It is used in logs, etc.
func (s *SimpleDDL) Name() string {
	return Name
}

// Less is used to sort pods in the scheduling queue in the following order.
//  1. Compare the priorities of Pods.
//  2. Compare the absolute deadline based on pod creation time and relative deadline defined in annotations.
//     If the creation time is not set, use current timestamp as fallback.
//     If the relative deadline is not defined via 'simpleddl.scheduling.x-k8s.io/ddl' annotations, use 10m as fallback
//  3. Compare the keys of Pods: <namespace>/<podname>.
func (s *SimpleDDL) Less(podInfo1, podInfo2 *framework.QueuedPodInfo) bool {
	prio1 := corev1helpers.PodPriority(podInfo1.Pod)
	prio2 := corev1helpers.PodPriority(podInfo2.Pod)
	if prio1 != prio2 {
		return prio1 > prio2
	}
	creationT1 := podInfo1.Pod.CreationTimestamp
	creationT2 := podInfo2.Pod.CreationTimestamp
	ddl1 := parseDDL(creationT1.Time, podInfo1.Pod)
	ddl2 := parseDDL(creationT2.Time, podInfo2.Pod)
	if ddl1.Equal(ddl2) {
		return core.GetNamespacedName(podInfo1.Pod) < core.GetNamespacedName(podInfo2.Pod)
	}
	return ddl1.Before(ddl2)
}

func parseDDL(creationTime time.Time, p *v1.Pod) time.Time {
	if creationTime.IsZero() {
		klog.Warningf("invalid pod creation time, using current timestamp and default deadline %s", defaultDDLRelative)
		return time.Now().Add(defaultDDLRelative)
	}
	defaultDDL := creationTime.Add(defaultDDLRelative)
	ddlStr, ok := p.Annotations[AnnotationKeyDDL]
	if !ok {
		klog.Warningf("deadline not defined in annotations, using default %v", defaultDDLRelative)
		return defaultDDL
	}
	ddl, err := time.ParseDuration(ddlStr)
	if err != nil {
		klog.Warningf("failed to parse deadline defined with key '%s', using default %v: %s", AnnotationKeyDDL, defaultDDLRelative, err.Error())
		return defaultDDL
	}
	if ddl < 0 {
		klog.Warningf("deadline defined with key '%s' is < 0, using default %v", AnnotationKeyDDL, defaultDDLRelative)
		return defaultDDL
	}
	return creationTime.Add(ddl)
}
