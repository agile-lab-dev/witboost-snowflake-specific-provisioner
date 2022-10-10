#!/bin/bash

source config

NAMESPACE="${name}"

helm uninstall mock-specific-provisioner -n "${NAMESPACE}"
kubectl delete namespace "$NAMESPACE"