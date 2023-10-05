package rtpreemptive

import (
	"context"
	"errors"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corelisters "k8s.io/client-go/listers/core/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/utils/clock"
	"sigs.k8s.io/scheduler-plugins/pkg/coscheduling/core"
	"sigs.k8s.io/scheduler-plugins/pkg/rtpreemptive/deadline"
	"sigs.k8s.io/scheduler-plugins/pkg/rtpreemptive/preemption"
)

const (
	// Name of the plugin used in the plugin registry and configuration
	Name = "EDFPreemptiveScheduling"
	// used in cycle state
	PodDeadlinesSnapshotKey = Name + "/PodDeadlinesSnapshot"
	PreemptiblePodsKey      = Name + "/PreemptiblePods"
)

var (
	// sort pods based on priority and absolute deadline
	_ framework.QueueSortPlugin = &EDFPreemptiveScheduling{}
	// check if pod is marked as paused, set pod as pending
	_ framework.PreFilterPlugin = &EDFPreemptiveScheduling{}
	// check if pod is marked as paused, set pod as pending
	_ framework.FilterPlugin = &EDFPreemptiveScheduling{}
	// pause pod to be preempted or resume a paused pod and reject the current one
	_ framework.PostFilterPlugin = &EDFPreemptiveScheduling{}
)

// EDFPreemptiveScheduling implements several plugins to perform soft real-time
// (paused-/resume-based) preemptive scheduling
type EDFPreemptiveScheduling struct {
	fh                framework.Handle
	podLister         corelisters.PodLister
	deadlineManager   deadline.Manager
	preemptionManager preemption.Manager
	clock             clock.Clock
}

// Name returns name of the plugin, It is used in logs, etc.
func (rp *EDFPreemptiveScheduling) Name() string {
	return Name
}

// New initializes a new plugin and return it.
func New(_ runtime.Object, fh framework.Handle) (framework.Plugin, error) {
	podLister := fh.SharedInformerFactory().Core().V1().Pods().Lister()
	nodeLister := fh.SharedInformerFactory().Core().V1().Nodes().Lister()
	nodeInfoLister := fh.SnapshotSharedLister().NodeInfos()
	deadlineManager := deadline.NewDeadlineManager()
	priorityFuncEDF := func(pod *v1.Pod) int64 {
		ddl := deadlineManager.GetPodDeadline(pod)
		return -ddl.Unix()
	}
	return &EDFPreemptiveScheduling{
		fh:                fh,
		podLister:         podLister,
		deadlineManager:   deadlineManager,
		preemptionManager: preemption.NewPreemptionManager(podLister, nodeLister, nodeInfoLister, fh.ClientSet(), priorityFuncEDF),
		clock:             clock.RealClock{},
	}, nil
}

// QueueSort Plugin
// Less is used to sort pods in the scheduling queue in the following order.
//  1. Compare the priorities of Pods.
//  2. Compare the absolute deadline based on pod creation time and relative deadline defined in annotations.
//     If the creation time is not set, use current timestamp as fallback.
//     If the relative deadline is not defined via 'simpleddl.scheduling.x-k8s.io/ddl' annotations, use 10m as fallback
//  3. Compare the keys of Pods: <namespace>/<podname>.
func (rp *EDFPreemptiveScheduling) Less(podInfo1, podInfo2 *framework.QueuedPodInfo) bool {
	prio1 := corev1helpers.PodPriority(podInfo1.Pod)
	prio2 := corev1helpers.PodPriority(podInfo2.Pod)
	if prio1 != prio2 {
		return prio1 > prio2
	}
	ddl1 := rp.deadlineManager.AddPodDeadline(podInfo1.Pod)
	ddl2 := rp.deadlineManager.AddPodDeadline(podInfo2.Pod)
	if ddl1.Equal(ddl2) {
		return core.GetNamespacedName(podInfo1.Pod) < core.GetNamespacedName(podInfo2.Pod)
	}
	return ddl1.Before(ddl2)
}

// PreFilter Plugin
// handles the following cases
//  1. a pod is marked paused and should be resumed, then resume it and skip the scheduling cycle
//  2. a pod is marked paused but is already resumed, then skip the scheduling cycle
//  3. a pod is marked paused but cannot be resumed, then put it back to scheduling queue
//     - this happens when another paused pod on the same node as higher priority, or
//     - there is not enough resources on node to resume the pod, or
//     - some unexpected error happened
//  4. a pod is not marked paused, continue the scheduling cycle
func (rp *EDFPreemptiveScheduling) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	latestPod, _ := rp.podLister.Pods(pod.Namespace).Get(pod.Name)
	if latestPod != nil {
		pod = latestPod
	}
	if rp.preemptionManager.IsPodMarkedPaused(pod) {
		if rp.fh.GetWaitingPod(pod.UID) != nil || pod.Status.Phase == v1.PodPending {
			klog.InfoS("pod marked to be paused and is currently at waiting stage", "pod", klog.KObj(pod))
			return nil, framework.NewStatus(framework.Skip)
		}
		candidate := rp.preemptionManager.GetPausedCandidateOnNode(ctx, pod.Spec.NodeName)
		if candidate == nil {
			klog.InfoS("pod was marked to be paused but not found in preemption manager, attempt to resume", "pod", klog.KObj(pod))
			candidate = &preemption.Candidate{NodeName: pod.Spec.NodeName, Pod: pod}
		}
		if candidate.Pod.UID != latestPod.UID {
			klog.InfoS("another pod on the node has higher priority", "pod", klog.KObj(pod), "candidate", klog.KObj(candidate.Pod), "node", candidate.NodeName)
			return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, "rejected as another paused pod has higher priority")
		}
		c, err := rp.preemptionManager.ResumeCandidate(ctx, candidate)
		if err == preemption.ErrPodNotPaused {
			klog.InfoS("pod was marked to be paused but is not paused", "pod", klog.KObj(pod))
			return nil, framework.NewStatus(framework.Skip)
		}
		if err != nil {
			klog.ErrorS(err, "failed to resume pod", "pod", klog.KObj(pod))
			return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable)
		}
		klog.InfoS("successfully resumed pod", "pod", klog.KObj(c.Pod))
		return nil, framework.NewStatus(framework.Skip, "skipped because pod is resumed successfully")
	}
	rp.deadlineManager.AddPodDeadline(pod)
	return nil, nil
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (rp *EDFPreemptiveScheduling) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (rp *EDFPreemptiveScheduling) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if len(pod.Spec.NodeName) > 0 && pod.Spec.NodeName != nodeInfo.Node().Name {
		// pod is already assigned to a node and it's not the same as given node
		// this happens when a paused pod re-enters the scheduling queue
		return framework.NewStatus(framework.UnschedulableAndUnresolvable)
	}

	if candidate := rp.preemptionManager.GetPausedCandidateOnNode(ctx, nodeInfo.Node().Name); candidate != nil {
		if rp.deadlineManager.GetPodDeadline(candidate.Pod).Before(rp.deadlineManager.GetPodDeadline(pod)) {
			msg := "found a paused pod on node that need to be resumed"
			klog.InfoS(msg, "pausedPod", klog.KObj(candidate.Pod), "pod", klog.KObj(pod), "node", klog.KObj(nodeInfo.Node()))
			c, err := rp.preemptionManager.ResumeCandidate(ctx, candidate)
			if err == nil {
				klog.InfoS("resumed candidate successfully", "candidate", klog.KObj(c.Pod), "node", c.NodeName)
				return framework.NewStatus(framework.UnschedulableAndUnresolvable, msg)
			}
			klog.InfoS("failed to resume paused pod, continue to schedule pod", "error", err, "candidate", klog.KObj(candidate.Pod), "pod", klog.KObj(pod), "node", candidate.NodeName)
		}
	}

	var unpausedPods []*v1.Pod
	for _, p := range nodeInfo.Pods {
		if p.Pod.Status.Phase != v1.PodPaused {
			unpausedPods = append(unpausedPods, p.Pod)
		}
	}
	nodeExcludePausedPods := framework.NewNodeInfo(unpausedPods...)
	nodeExcludePausedPods.SetNode(nodeInfo.Node())
	insufficientResources := noderesources.Fits(pod, nodeExcludePausedPods)
	if len(insufficientResources) != 0 {
		// We will keep all failure reasons.
		failureReasons := make([]string, 0, len(insufficientResources))
		for i := range insufficientResources {
			failureReasons = append(failureReasons, insufficientResources[i].Reason)
		}
		return framework.NewStatus(framework.Unschedulable, failureReasons...)
	}
	return nil
}

// PostFilter plugin is called when no node is found schedulable at Filter stage
// here it attempt to preempt a pod on available nodes
// if preemption is successful, return with nominated node and add pod to paused map
func (rp *EDFPreemptiveScheduling) PostFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	allNode, err := rp.fh.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		klog.ErrorS(err, "failed to list all nodes", "pod", klog.KObj(pod))
		return nil, framework.AsStatus(err)
	}
	var candidates []*preemption.Candidate
	for _, nodeInfo := range allNode {
		// skip node where preemption is not helpful
		if filteredNodeStatusMap[nodeInfo.Node().Name].Code() == framework.UnschedulableAndUnresolvable {
			klog.InfoS("skipping unschedulable and unresolvable node", "pod", klog.KObj(pod), "node", klog.KObj(nodeInfo.Node()))
			continue
		}
		candidate := rp.findCandidateOnNode(pod, nodeInfo)
		if candidate != nil {
			candidates = append(candidates, &preemption.Candidate{NodeName: nodeInfo.Node().Name, Pod: candidate})
		}
	}
	candidate := rp.selectCandidate(candidates)
	if candidate == nil {
		klog.ErrorS(errors.New("no preemptible candidates"), "select candidate failed", "pod", klog.KObj(pod))
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, "no preemptible candidates found")
	}
	klog.InfoS("found candidate pod to pause on node", "candidate", klog.KObj(candidate.Pod), "pod", klog.KObj(pod), "node", candidate.NodeName)
	if _, err := rp.preemptionManager.PauseCandidate(ctx, candidate); err != nil {
		klog.ErrorS(err, "failed to pause pod on node", "candidate", klog.KObj(candidate.Pod), "pod", klog.KObj(pod), "node", candidate.NodeName)
		return nil, framework.AsStatus(err)
	}
	return framework.NewPostFilterResultWithNominatedNode(candidate.NodeName), framework.NewStatus(framework.Success)
}

func (rp *EDFPreemptiveScheduling) selectCandidate(candidates []*preemption.Candidate) *preemption.Candidate {
	if len(candidates) == 0 {
		return nil
	}
	maxDDL := rp.deadlineManager.GetPodDeadline(candidates[0].Pod)
	bestCandidate := candidates[0]
	for _, c := range candidates {
		if len(c.Pod.Spec.NodeName) <= 0 {
			klog.InfoS("selectCandidate: skipping candidate with no node assigned", "candidate", klog.KObj(c.Pod))
		}
		ddl := rp.deadlineManager.GetPodDeadline(c.Pod)
		if ddl.After(maxDDL) {
			maxDDL = ddl
			bestCandidate = c
		}
	}
	return bestCandidate
}

func (rp *EDFPreemptiveScheduling) findCandidateOnNode(pod *v1.Pod, nodeInfo *framework.NodeInfo) *v1.Pod {
	podDDL := rp.deadlineManager.GetPodDeadline(pod)
	maxDDL := podDDL

	var candidatePod *v1.Pod
	var unpausedPods []*v1.Pod
	// find latest deadline among all running pods and update candidate accordingly
	for _, podInfo := range append(nodeInfo.Pods, rp.fh.NominatedPodsForNode(nodeInfo.Node().Name)...) {
		p, err := rp.podLister.Pods(podInfo.Pod.Namespace).Get(podInfo.Pod.Name)
		if err != nil {
			klog.ErrorS(err, "Getting updated pod from node", "pod", klog.KRef(podInfo.Pod.Namespace, podInfo.Pod.Name), "node", nodeInfo.Node().Name)
			p = podInfo.Pod // fallback to pod from nodeInfo
		}
		if p.UID == pod.UID {
			klog.InfoS("skipping pod with the same uid", "p", klog.KObj(p), "pod", klog.KObj(pod), "uid", p.UID)
			continue
		}
		if p.Status.Phase != v1.PodPaused {
			unpausedPods = append(unpausedPods, p)
			ddl := rp.deadlineManager.GetPodDeadline(p)
			if ddl.After(maxDDL) {
				maxDDL = ddl
				candidatePod = p
			}
		}
	}
	if candidatePod == nil {
		klog.InfoS("found no candidate pods on node with deadline later than current pod deadline", "pod", klog.KObj(pod), "node", klog.KObj(nodeInfo.Node()))
		return nil
	}

	// check if the preemptible pod is excluded it would yield enough resource to run the current pod
	var podsExcludeCandidate []*v1.Pod
	for _, p := range unpausedPods {
		if p.UID != candidatePod.UID {
			podsExcludeCandidate = append(podsExcludeCandidate, p)
		}
	}
	nodeAfterPreemption := framework.NewNodeInfo(podsExcludeCandidate...)
	nodeAfterPreemption.SetNode(nodeInfo.Node())
	insufficientResources := noderesources.Fits(pod, nodeAfterPreemption)
	if len(insufficientResources) > 0 {
		klog.InfoS("there is not enough resource to run pod on node even after preemption", "candidate", klog.KObj(candidatePod), "pod", klog.KObj(pod), "node", klog.KObj(nodeInfo.Node()))
		return nil
	}
	return candidatePod
}
