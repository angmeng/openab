{{- define "openab-telegram.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "openab-telegram.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "openab-telegram.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "openab-telegram.agentImage" -}}
{{- $tag := .Values.image.tag -}}
{{- if not $tag -}}
  {{- $tag = .Values.channel | default "stable" -}}
{{- end -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}

{{- define "openab-telegram.gatewayImage" -}}
{{- printf "%s:%s" .Values.gateway.image .Values.gateway.tag -}}
{{- end }}

{{- define "openab-telegram.secretName" -}}
{{- .Values.existingSecret | default (include "openab-telegram.fullname" .) -}}
{{- end }}