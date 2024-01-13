package annotations

const (
	// AnnotationKeyPrefix is the prefix of the annotation key
	AnnotationKeyPrefix = "rt-preemptive.scheduling.x-k8s.io/"
	// AnnotationKeyDDL represents the relative deadline of a program
	AnnotationKeyDDL = AnnotationKeyPrefix + "ddl"
	// AnnotationKeyExecTime represents the estimated execution time of a program
	AnnotationKeyExecTime = AnnotationKeyPrefix + "exec-time"
	// AnnotationKeyPausePod represents whether or not a pod is marked to be paused
	AnnotationKeyPausePod = AnnotationKeyPrefix + "pause-pod"
	// AnnotationKeyPreemptible indicates a pod cannot be paused
	AnnotationKeyPreemptible = AnnotationKeyPrefix + "preemptible"
	// AnnotationKeyATLASEnabled indicates if execution time should be estimated using ATLAS predictor
	AnnotationKeyATLASEnabled = AnnotationKeyPrefix + "atlas-enabled"
	// AnnotationKeyPredictor represents the name of the predictor to use for execution time prediction
	AnnotationKeyPredictor = AnnotationKeyPrefix + "atlas-predictor"
	// AnnotationKeyPredictorMetrics represents additional metrics of the pod
	AnnotationKeyPredictorMetrics = AnnotationKeyPrefix + "atlas-metrics"
	// AnnotationKeyPredictorUseJobIndex config if predictor should use job index for execution time prediction
	AnnotationKeyPredictorUseJobIndex = AnnotationKeyPrefix + "atlas-use-job-index"
)
