package deadline

import (
	"testing"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testutil "sigs.k8s.io/scheduler-plugins/test/util"
)

func TestLess(t *testing.T) {
	now := time.Now()
	times := make([]v1.Time, 0)
	for _, d := range []time.Duration{0, 1, 2} {
		times = append(times, v1.Time{Time: now.Add(d * time.Second)})
	}
	ns1, ns2 := "namespace1", "namespace2"
	lowPriority, highPriority := int32(10), int32(100)
	shortDDL, longDDL := map[string]string{AnnotationKeyDDL: "10s"}, map[string]string{AnnotationKeyDDL: "20s"}
	for _, tt := range []struct {
		name     string
		p1       *framework.QueuedPodInfo
		p2       *framework.QueuedPodInfo
		expected bool
	}{
		{
			name: "p1.prio < p2 prio, p2 scheduled first",
			p1: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns1).Name("pod1").Priority(lowPriority).Obj()),
			},
			p2: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns2).Name("pod2").Priority(highPriority).Obj()),
			},
			expected: false,
		},
		{
			name: "p1.prio > p2 prio, p1 scheduled first",
			p1: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns1).Name("pod1").Priority(highPriority).Obj()),
			},
			p2: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns2).Name("pod2").Priority(lowPriority).Obj()),
			},
			expected: true,
		},
		{
			name: "p1.prio == p2 prio, p1 ddl earlier than p2 ddl, p1 scheduled first",
			p1: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns1).Name("pod1").Priority(lowPriority).CreationTimestamp(times[0]).Annotations(shortDDL).Obj()),
			},
			p2: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns2).Name("pod2").Priority(lowPriority).CreationTimestamp(times[1]).Annotations(longDDL).Obj()),
			},
			expected: true,
		},
		{
			name: "p1.prio == p2 prio, p1 ddl later than p2 ddl, p2 scheduled first",
			p1: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns1).Name("pod1").Priority(lowPriority).CreationTimestamp(times[0]).Annotations(longDDL).Obj()),
			},
			p2: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns2).Name("pod2").Priority(lowPriority).CreationTimestamp(times[1]).Annotations(shortDDL).Obj()),
			},
			expected: false,
		},
		{
			name: "p1.prio == p2 prio, no ddl defined, p1 created earlier than p2, p1 scheduled first",
			p1: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns1).Name("pod1").Priority(lowPriority).CreationTimestamp(times[0]).Obj()),
			},
			p2: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns2).Name("pod2").Priority(lowPriority).CreationTimestamp(times[1]).Obj()),
			},
			expected: true,
		},
		{
			name: "p1.prio == p2 prio, no ddl defined, p1 created later than p2, p2 scheduled first",
			p1: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns1).Name("pod1").Priority(lowPriority).CreationTimestamp(times[1]).Obj()),
			},
			p2: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns2).Name("pod2").Priority(lowPriority).CreationTimestamp(times[0]).Obj()),
			},
			expected: false,
		},
		{
			name: "p1.prio == p2 prio, equal creation time and ddl, sort by name string, p1 scheduled first",
			p1: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns1).Name("pod1").Priority(lowPriority).CreationTimestamp(times[0]).Obj()),
			},
			p2: &framework.QueuedPodInfo{
				PodInfo: testutil.MustNewPodInfo(t, st.MakePod().Namespace(ns2).Name("pod2").Priority(lowPriority).CreationTimestamp(times[0]).Obj()),
			},
			expected: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			simpleDDL := &SimpleDDL{}
			if got := simpleDDL.Less(tt.p1, tt.p2); got != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}
