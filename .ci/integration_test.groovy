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
                        COMMIT_HASH=${params.ghprbActualCommit} PULL_ID=${params.ghprbPullId} TAR_PATH=${curws}/tics/tests/.build bash -e ${curws}/tics/release-centos7/build/fetch-ci-build.sh
                        """
                }
            }
        }
        stash includes: "tics/**", name: "git-code-tics", useDefaultExcludes: false
    }

    parallel (
        "tidb ci test": {
            def label = "tidb-ci-test"
            util.runTest(label, "tics/tests/tidb-ci", tidbBranch)
        },
        "gtest": {
            def label = "gtest"
            util.runTest(label, "tics/tests/gtest", tidbBranch)
        },
        "delta merge test": {
            def label = "delta-merge-test"
            util.runTest(label, "tics/tests/delta-merge-test", tidbBranch)
        },
        "fullstack test": {
            def label = "fullstack-test"
            util.runTest(label, "tics/tests/fullstack-test", tidbBranch)
        },
        "fullstack test2": {
            def label = "fullstack-test2"
            util.runTest(label, "tics/tests/fullstack-test2", tidbBranch)
        },
        "mutable test": {
            def label = "mutable-test"
            util.runTest(label, "tics/tests/mutable-test", tidbBranch)
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
