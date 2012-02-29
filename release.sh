# 1 check all parameters in branch.sh
# 2 run this script off the $DEV_BRANCH branch.
# couldn't get release plugin working with remote git as needed at the first attempt otherwise.
# set MVN_GITHUB_RELEASE_REPO per deploy -DaltDeploymentREpository rules ( <id>::default::<url>)
# to push to during deployment. 


. ./branch.sh

git checkout $DEV_BRANCH && git pull $REMOTE && \
git checkout -b $REL_BRANCH-$REL_VERSION && git push -u $REMOTE $REL_BRANCH-$REL_VERSION  && \
mvn -e  release:clean org.apache.maven.plugins:maven-release-plugin:2.2.1:prepare -DlocalCheckout=true -DreleaseVersion=$REL_VERSION -Dtag=tag-$REL_VERSION \
-DpushChanges=false && \
mvn org.apache.maven.plugins:maven-release-plugin:2.2.1:perform -Darguments="-DaltDeploymentRepository=$MVN_GITHUB_RELEASE_REPO" -Prelease -PR -DlocalCheckout=true -Dmaven.test.skip=true \
-DpushChanges=false && \
git checkout $DEV_BRANCH && \
git merge $REL_BRANCH-$REL_VERSION && \
git push
git push --tags
