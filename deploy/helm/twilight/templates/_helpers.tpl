{{- define "twilight.labels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/part-of: twilight
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "twilight.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/part-of: twilight
{{- end }}

{{- define "twilight.image" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end }}

{{- define "twilight.secretName" -}}
{{- required "secrets.existingSecret is required" .Values.secrets.existingSecret -}}
{{- end }}

{{/* Only pod identity is injected here; role is set by each binary. */}}
{{- define "twilight.identityEnv" -}}
- name: TWILIGHT_POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: TWILIGHT_POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: TWILIGHT_NODE_NAME
  valueFrom:
    fieldRef:
      fieldPath: spec.nodeName
{{- end }}

{{- define "twilight.metricsAnnotations" -}}
{{- $annotations := dict -}}
{{- if .Values.metrics.scrapeAnnotations -}}
{{- $_ := set $annotations "prometheus.io/scrape" "true" -}}
{{- $_ := set $annotations "prometheus.io/path" "/metrics" -}}
{{- $_ := set $annotations "prometheus.io/port" (toString .port) -}}
{{- end -}}
{{- range $key, $value := .extraAnnotations -}}
{{- $_ := set $annotations $key $value -}}
{{- end -}}
{{- if $annotations }}
annotations:
  {{- toYaml $annotations | nindent 2 }}
{{- end }}
{{- end }}
