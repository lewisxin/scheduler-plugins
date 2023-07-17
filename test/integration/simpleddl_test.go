package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/scheduler"
	schedapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"sigs.k8s.io/scheduler-plugins/pkg/deadline"
	"sigs.k8s.io/scheduler-plugins/test/util"
)

func TestSimpleDDLPlugin(t *testing.T) {
	testCtx := &testContext{}
	testCtx.Ctx, testCtx.CancelFn = context.WithCancel(context.Background())
	cs := kubernetes.NewForConfigOrDie(globalKubeConfig)
	testCtx.ClientSet = cs
	testCtx.KubeConfig = globalKubeConfig

	cfg, err := util.NewDefaultSchedulerComponentConfig()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Profiles[0].Plugins.QueueSort = schedapi.PluginSet{
		Enabled:  []schedapi.Plugin{{Name: deadline.Name}},
		Disabled: []schedapi.Plugin{{Name: "*"}},
	}

	testCtx = initTestSchedulerWithOptions(
		t,
		testCtx,
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(fwkruntime.Registry{deadline.Name: deadline.New}),
	)
	syncInformerFactory(testCtx)
	defer cleanupTest(t, testCtx)

	ns := fmt.Sprintf("integration-test-%v", string(uuid.NewUUID()))
	createNamespace(t, testCtx, ns)

	// create a node
	nodeName := "node1"
	node := st.MakeNode().Name(nodeName).Label("node", nodeName).Obj()
	node.Status.Capacity = v1.ResourceList{
		v1.ResourcePods:   *resource.NewQuantity(32, resource.DecimalSI),
		v1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
		v1.ResourceMemory: *resource.NewMilliQuantity(500, resource.DecimalSI),
	}
	node, err = cs.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create Node %q: %v", nodeName, err)
	}
	pause := imageutils.GetPauseImageName()

	for _, tt := range []struct {
		name              string
		pods              []*v1.Pod
		expectedPodsSched []string
	}{
		{
			name: "pod scheduled in order of the earliest deadline",
			pods: []*v1.Pod{
				st.MakePod().Namespace(ns).Name("p1-t1").Annotation(deadline.AnnotationKeyDDL, "1s").Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p2-t1").Annotation(deadline.AnnotationKeyDDL, "1ms").Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p3-t1").Annotation(deadline.AnnotationKeyDDL, "1h").Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p4-t1").Annotation(deadline.AnnotationKeyDDL, "1m").Container(pause).Obj(),
			},
			expectedPodsSched: []string{"p2-t1", "p1-t1", "p4-t1", "p3-t1"},
		},
		{
			name: "pod scheduled in order of the priority",
			pods: []*v1.Pod{
				st.MakePod().Namespace(ns).Name("p1-t2").Annotation(deadline.AnnotationKeyDDL, "1ms").Priority(lowPriority).Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p2-t2").Annotation(deadline.AnnotationKeyDDL, "1h").Priority(highPriority).Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p3-t2").Annotation(deadline.AnnotationKeyDDL, "1m").Priority(midPriority).Container(pause).Obj(),
			},
			expectedPodsSched: []string{"p2-t2", "p3-t2", "p1-t2"},
		},
		{
			name: "pod scheduled in order of creation time",
			pods: []*v1.Pod{
				st.MakePod().Namespace(ns).Name("p1-t3").Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p2-t3").Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p3-t3").Container(pause).Obj(),
			},
			expectedPodsSched: []string{"p1-t3", "p2-t3", "p3-t3"},
		},
		{
			name: "pod scheduled in order of mixed properties",
			pods: []*v1.Pod{
				st.MakePod().Namespace(ns).Name("p1-t4").Annotation(deadline.AnnotationKeyDDL, "1ms").Priority(lowPriority).Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p2-t4").Annotation(deadline.AnnotationKeyDDL, "2ms").Priority(lowPriority).Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p3-t4").Annotation(deadline.AnnotationKeyDDL, "3ms").Priority(highPriority).Container(pause).Obj(),
				st.MakePod().Namespace(ns).Name("p4-t4").Annotation(deadline.AnnotationKeyDDL, "4ms").Priority(highPriority).Container(pause).Obj(),
			},
			expectedPodsSched: []string{"p3-t4", "p4-t4", "p1-t4", "p2-t4"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer cleanupPods(t, testCtx, tt.pods)

			t.Logf("Start to create the pods.")
			for _, pod := range tt.pods {
				t.Logf("creating pod %q", pod.Name)
				_, err = cs.CoreV1().Pods(ns).Create(testCtx.Ctx, pod, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
				}
			}

			// wait for pods in sched queue
			if err = wait.Poll(time.Millisecond*200, wait.ForeverTestTimeout, func() (bool, error) {
				pendingPods, _ := testCtx.Scheduler.SchedulingQueue.PendingPods()
				if len(pendingPods) == len(tt.pods) {
					return true, nil
				}
				return false, nil
			}); err != nil {
				t.Fatal(err)
			}

			for _, expected := range tt.expectedPodsSched {
				actual := testCtx.Scheduler.NextPod().Pod.Name
				if actual != expected {
					t.Errorf("Expect Pod %q, but got %q", expected, actual)
				} else {
					t.Logf("Pod %q is popped out as expected.", actual)
				}
			}
		})
	}
}
