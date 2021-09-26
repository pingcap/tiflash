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
            ],
            volumes: [
                    nfsVolume(mountPath: '/home/jenkins/agent/ci-cached-code-daily', serverAddress: '172.16.5.22',
                            serverPath: '/mnt/ci.pingcap.net-nfs/git', readOnly: false),
            ]
) {

        node(label) {

            dir("tics") {
                stage("Checkout") {
                    container("docker") {
                        def repoDailyCache = "/home/jenkins/agent/ci-cached-code-daily/src-tics.tar.gz"
                        if (fileExists(repoDailyCache)) {
                            println "get code from nfs to reduce clone time"
                            sh """
                            cp -R ${repoDailyCache}  ./
                            tar -xzf ${repoDailyCache} --strip-components=1
                            rm -f src-tics.tar.gz
                            """
                            sh "chown -R 1000:1000 ./"
                        }    
                    }
                    util.checkoutTiCSFull("${ghprbActualCommit}", "${ghprbPullId}")
                }
                stage("Build & Upload") {
                    timeout(time: 70, unit: 'MINUTES') {
                        container("builder") {
                            sh "NPROC=5 BUILD_BRANCH=${ghprbTargetBranch} release-centos7/build/build-tiflash-ci.sh"
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
