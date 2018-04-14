#!/bin/bash

set -e -x

mvn clean install -N
(cd ./data-model; mvn clean install)
(cd ./workshop-main; mvn fabric8:deploy)
(cd ./positions-injector; mvn fabric8:deploy)
(cd ./stations-injector; mvn fabric8:deploy -Psolution)
(cd ./delayed-listener; mvn fabric8:deploy -Psolution)
(cd ./delayed-trains; mvn fabric8:deploy -Psolution)
(cd ./datagrid-visualizer; ./deploy.sh)
