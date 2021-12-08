{{/* vim: set filetype=mustache: */}}
{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hashi.fullname" -}}
{{- printf "%s" .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Environmental variables */}}
{{- define "hashi.env" -}}
- name: DD_AGENT_HOST
  valueFrom:
    fieldRef:
      fieldPath: status.hostIP
- name: DD_SERVICE
  valueFrom:
    configMapKeyRef:
      name: hashi-config
      key: DD_SERVICE
- name: DD_ENV
  valueFrom:
    configMapKeyRef:
      name: hashi-config
      key: K8S_ENV
- name: K8S_ENV
  valueFrom:
    configMapKeyRef:
      name: hashi-config
      key: K8S_ENV
- name: DATABASE_NAME
  valueFrom:
    configMapKeyRef:
      name: hashi-config
      key: DATABASE_NAME
- name: DATABASE_HOST
  valueFrom:
    configMapKeyRef:
      name: hashi-config
      key: DATABASE_HOST
- name: DATABASE_USER
  valueFrom:
    configMapKeyRef:
      name: hashi-config
      key: DATABASE_USER
- name: DATABASE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: hashi-secrets
      key: DATABASE_PASSWORD
{{- end -}}