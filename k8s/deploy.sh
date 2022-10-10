#!/bin/bash

set -ex

export VAULT_SKIP_VERIFY=true
export NAME=mock-specific-provisioner
export IMAGE_NAME=registry.gitlab.com/agilefactory/witboost.mesh/provisioning/witboost.mesh.provisioning.specificprovisioner
export NAMESPACE=${NAME}

# Check required vars
if [ -z ${VAULT_TOKEN+x} ]; then echo "VAULT_TOKEN is unset" && exit 1; fi
if [ -z ${VAULT_ADDR+x} ]; then echo "VAULT_ADDR is unset" && exit 1; fi
if [ -z ${ENV+x} ]; then echo "ENV is unset" && exit 1; fi
if [ -z ${VERSION+x} ]; then echo "VERSION is unset" && exit 1; fi
if [ -z ${COMMIT+x} ]; then echo "COMMIT is unset" && exit 1; fi

# Create namespace and import regcred, if needed
kubectl get namespace "${NAMESPACE}" > /dev/null || res=$?
if [ $res -eq 1 ]
then
   kubectl create namespace "${NAMESPACE}"
   kubectl get secret regcred --namespace=default -o yaml | grep -v '^\s*namespace:\s' | kubectl apply --namespace="${NAMESPACE}" -f -
fi

# Create config based on the environment, there might be different configs for different envs
export APPLICATION_CONF="application.$ENV.conf"
if [ ! -f "$APPLICATION_CONF" ]; then
  echo "$APPLICATION_CONF does not exist."; exit 1;
fi
kubectl create configmap application.conf --from-file=application.conf="$APPLICATION_CONF" -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# install/upgrade release
helm upgrade --atomic --install -n ${NAMESPACE} --wait --timeout 120s --description "${COMMIT}" -f values.yaml mock-specific-provisioner ./helm \
--set name="${NAME}" \
--set imageName="${IMAGE_NAME}" \
--set version="${VERSION}"

upgrade_result=$?

# show deploy history
helm history mock-specific-provisioner -n "${NAMESPACE}"

# return helm upgrade status code
exit $upgrade_result