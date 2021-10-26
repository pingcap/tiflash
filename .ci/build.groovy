catchError {
    def util = load('util.groovy')

    def tiflashTag = ({
        def m = ghprbCommentBody =~ /tiflash\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        }
        return params.tiflashTag ?: ghprbTargetBranch ?: 'master'
    }).call()

    def CURWS = pwd()

    def NPROC = 5

    parallel (
        "build-tics": {
            util.runWithTiCSFull("build-tics", CURWS) {
                dir("${CURWS}/tics") {
                    stage("Build & Upload") {
                        timeout(time: 70, unit: 'MINUTES') {
                            container("builder") {
                                sh "NPROC=${NPROC} BUILD_BRANCH=${ghprbTargetBranch} ENABLE_FORMAT_CHECK=true ${CURWS}/tics/release-centos7/build/build-tiflash-ci.sh"
                                sh "PULL_ID=${ghprbPullId} COMMIT_HASH=${ghprbActualCommit} ${CURWS}/tics/release-centos7/build/upload-ci-build.sh"
                            }
                        }
                    }
                    stage("Static Analysis") {
                        timeout(time: 360, unit: 'MINUTES') {
                            container("builder") {
                                echo "NPROC=${NPROC} /build/tics/release-centos7/build/static-analysis.sh"
                            }
                        }
                    }
                }
            }
        },
        "ut-tics": {
            // util.runUTCoverTICS(CURWS, NPROC)
        },
    )
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
