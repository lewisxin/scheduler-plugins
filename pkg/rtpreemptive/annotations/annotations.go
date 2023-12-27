package annotations

const (
	// AnnotationKeyPrefix is the prefix of the annotation key
	AnnotationKeyPrefix = "rt-preemptive.scheduling.x-k8s.io/"
	// AnnotationKeyDDL represents the relative deadline of a program
	AnnotationKeyDDL = AnnotationKeyPrefix + "ddl"
	// AnnotationKeyExecTime represents the estimated execution time of a program
	AnnotationKeyExecTime = AnnotationKeyPrefix + "exec-time"
	// AnnotationKeyATLASEnabled represents additional metrics of the pod
	AnnotationKeyATLASMetrics = AnnotationKeyPrefix + "metrics"
	// AnnotationKeyPausePod represents whether or not a pod is marked to be paused
	AnnotationKeyPausePod = AnnotationKeyPrefix + "pause-pod"
	// AnnotationKeyPreemptible indicates a pod cannot be paused
	AnnotationKeyPreemptible = AnnotationKeyPrefix + "preemptible"
	// AnnotationKeyATLASEnabled indicates if execution time should be estimated using ATLAS predictor
	AnnotationKeyATLASEnabled = AnnotationKeyPrefix + "atlas-enabled"
)
