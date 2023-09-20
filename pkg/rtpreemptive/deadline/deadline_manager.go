package deadline

import (
	"time"

	gocache "github.com/patrickmn/go-cache"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

const (
	// AnnotationKeyPrefix is the prefix of the annotation key
	AnnotationKeyPrefix = "rt-preemptive.scheduling.x-k8s.io/"
	// AnnotationKeyDDL represents the relative deadline of a program
	AnnotationKeyDDL = AnnotationKeyPrefix + "ddl"
	// default relative deadline 1 month
	defaultDDLRelative = time.Hour * 24 * 30
)

type Manager interface {
	// ParsePodDeadline parse pod annotations and return the absolute deadline of a pod
	ParsePodDeadline(pod *v1.Pod) time.Time
	// AddPodDeadline adds a pod to absolute deadline pair to manager if no exist and returns the deadline to caller
	AddPodDeadline(pod *v1.Pod) time.Time
	// RemovePodDeadline deletes a pod to absolute deadline pair from manager
	RemovePodDeadline(pod *v1.Pod)
	// GetPodDeadline returns the absolute deadline of the given pod
	GetPodDeadline(pod *v1.Pod) time.Time
}

type deadlineManager struct {
	podDeadlines *gocache.Cache
}

func NewDeadlineManager() Manager {
	return &deadlineManager{podDeadlines: gocache.New(time.Hour*5, time.Second*5)}
}

func (m *deadlineManager) ParsePodDeadline(pod *v1.Pod) time.Time {
	creationTime := pod.CreationTimestamp
	if creationTime.IsZero() {
		klog.Warningf("invalid pod creation time, using current timestamp and default deadline %s", defaultDDLRelative)
		return time.Now().Add(defaultDDLRelative)
	}
	defaultDDL := creationTime.Add(defaultDDLRelative)
	ddlStr, ok := pod.Annotations[AnnotationKeyDDL]
	if !ok {
		klog.Warningf("deadline not defined in pod annotations, using default %v", defaultDDLRelative)
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

func (m deadlineManager) AddPodDeadline(pod *v1.Pod) time.Time {
	ddl := m.GetPodDeadline(pod)
	m.podDeadlines.Set(string(pod.UID), ddl, time.Hour)
	return ddl
}

func (m deadlineManager) RemovePodDeadline(pod *v1.Pod) {
	m.podDeadlines.Delete(string(pod.UID))
	m.podDeadlines.DeleteExpired()
}

func (m deadlineManager) GetPodDeadline(pod *v1.Pod) time.Time {
	ddl, ok := m.podDeadlines.Get(string(pod.UID))
	if !ok {
		return m.ParsePodDeadline(pod)
	}
	return ddl.(time.Time)
}
