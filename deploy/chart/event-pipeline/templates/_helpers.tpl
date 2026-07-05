{{/* Common labels stamped on every object. */}}
{{- define "event-pipeline.labels" -}}
app.kubernetes.io/part-of: event-pipeline
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/* Pod-level security baseline (restricted profile). 65532 is the numeric
uid of distroless's `nonroot` user — runAsNonRoot can only be verified against
a numeric uid, so the image's symbolic USER alone is not enough. */}}
{{- define "event-pipeline.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{/* Container-level security baseline. */}}
{{- define "event-pipeline.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end }}
