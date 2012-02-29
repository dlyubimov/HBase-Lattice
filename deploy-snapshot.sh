#!/bin/bash

mvn -e -Dmaven.test.skip=true -PR -DaltDeploymentRepository=$MVN_GITHUB_SNAPSHOT_REPO clean install deploy

