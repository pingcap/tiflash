catchError {
    def util = load('util.groovy')

    def label = "build-tics"

    def tiflashTag = ({
        def m = ghprbCommentBody =~ /tiflash\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        }
        return params.tiflashTag ?: ghprbTargetBranch ?: 'master'
    }).call()

    podTemplate(name: label, label: label, instanceCap: 5,
            workspaceVolume: emptyDirWorkspaceVolume(memory: true),
            nodeSelector: 'role_type=slave',
            containers: [
                    containerTemplate(name: 'dockerd', image: 'docker:18.09.6-dind', privileged: true),
                    containerTemplate(name: 'docker', image: 'hub.pingcap.net/zyguan/docker:build-essential',
                            alwaysPullImage: true, envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], ttyEnabled: true, command: 'cat'),
                    containerTemplate(name: 'builder', image: 'hub.pingcap.net/tiflash/tiflash-builder-ci',
                            alwaysPullImage: true, ttyEnabled: true, command: 'cat',
                            resourceRequestCpu: '4000m', resourceRequestMemory: '8Gi',
                            resourceLimitCpu: '10000m', resourceLimitMemory: '30Gi'),
            ]) {

        node(label) {

            dir("tics") {
                stage("Checkout") {
                    container("docker") {
                        sh """
                        archive_url=${FILE_SERVER_URL}/download/builds/pingcap/tics/cache/tics-repo_latest.tar.gz
                        if [ ! -d contrib ]; then curl -sL \$archive_url | tar -zx --strip-components=1 || true; fi
                        """
                        sh "chown -R 1000:1000 ./"
                        // sh "if ! grep -q hub.pingcap.net /etc/hosts ; then echo '172.16.10.5 hub.pingcap.net' >> /etc/hosts; fi"
                    }
                    util.checkoutTiCSFull("${ghprbActualCommit}", "${ghprbPullId}")
                }
                stage("Build & Upload") {
                    timeout(time: 70, unit: 'MINUTES') {
                        container("builder") {
                            sh "NPROC=5 BUILD_BRANCH=${ghprbTargetBranch} ENABLE_FORMAT_CHECK=true release-centos7/build/build-tiflash-ci.sh"
                            sh "PULL_ID=${ghprbPullId} COMMIT_HASH=${ghprbActualCommit} release-centos7/build/upload-ci-build.sh"
                        }
                    }
                }
            }
        }
    }
}


stage('Summary') {
    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
    def msg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" +
            "${ghprbPullLink}" + "\n" +
            "${ghprbPullDescription}" + "\n" +
            "Build Result: `${currentBuild.currentResult}`" + "\n" +
            "Elapsed Time: `${duration} mins` " + "\n" +
            "${env.RUN_DISPLAY_URL}"

    echo "${msg}"

    if (currentBuild.currentResult != "SUCCESS") {
        echo "Send slack here ..."
        slackSend channel: '#jenkins-ci', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${msg}"
    }
}
