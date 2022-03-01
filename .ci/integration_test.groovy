catchError {
    def util = load('util.groovy')

    def tidbBranch = ({
        def m = params.ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        }
        return params.ghprbTargetBranch ?: 'master'
    }).call()

    stage("Wait for ci build") {
        echo "ticsTag=${params.ghprbActualCommit} tidbBranch=${tidbBranch}"
    }

    node("${GO_BUILD_SLAVE}") {
        def curws = pwd()
        dir("/home/jenkins/agent/code-archive") {
            container("golang") {
                if(fileExists("/nfs/cache/git/src-tics.tar.gz")){
                    timeout(5) {
                        sh """
                        cp -R /nfs/cache/git/src-tics.tar.gz*  ./
                        mkdir -p ${curws}/tics
                        tar -xzf src-tics.tar.gz -C ${curws}/tics --strip-components=1
                        """
                    }
                }
            }
            dir("${curws}/tics") {
                util.checkoutTiCS("${params.ghprbActualCommit}", "${params.ghprbPullId}")
            }
            timeout(time: 60, unit: 'MINUTES') {
                container("golang") {
                    sh  """
                        rm -rf ${curws}/tics/contrib
                        rm -rf ${curws}/tics/.git

                        COMMIT_HASH=${params.ghprbActualCommit} PULL_ID=${params.ghprbPullId} bash -e ${curws}/tics/release-centos7-llvm/scripts/fetch-ci-build.sh
                        """
                }
            }
        }
        stash includes: "tics/**", name: "git-code-tics", useDefaultExcludes: false
    }

    def pod_label = "tics-integration-test-v1"

    parallel (
        "tidb ci test": {
            def name = "tidb-ci-test"
            util.runTest(pod_label, name, "tics/tests/tidb-ci", tidbBranch)
        },
        "delta merge test": {
            def name = "delta-merge-test"
            util.runTest(pod_label, name, "tics/tests/delta-merge-test", tidbBranch)
        },
        "fullstack test": {
            def name = "fullstack-test"
            util.runTest(pod_label, name, "tics/tests/fullstack-test", tidbBranch)
        },
        "fullstack test2": {
            def name = "fullstack-test2"
            util.runTest(pod_label, name, "tics/tests/fullstack-test2", tidbBranch)
        },
    )
}

stage('Summary') {
    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
    def msg = "Result: `${currentBuild.currentResult}`" + "\n" +
            "Elapsed Time: `${duration} mins`" + "\n" +
            "${env.RUN_DISPLAY_URL}"

    echo "${msg}"
}
