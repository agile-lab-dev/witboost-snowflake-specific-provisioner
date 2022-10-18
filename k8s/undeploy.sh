#!/bin/bash

source config

NAMESPACE="${name}"

helm uninstall ${name} -n "${NAMESPACE}"
kubectl delete namespace "$NAMESPACE"