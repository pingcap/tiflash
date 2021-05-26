catchError {
    def util = load('util.groovy')

    def tidbBranch = ({
        def m = params.ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        }
        return params.ghprbTargetBranch ?: 'master'
    }).call()

    echo "ticsTag=${params.ghprbActualCommit} tidbBranch=${tidbBranch}"

    stage("Wait for images") {
        util.runClosure("wait-for-images") {
            timeout(time: 60, unit: 'MINUTES') {
                container("docker") {
                    sh  """
                        while ! docker pull hub.pingcap.net/tiflash/tics:${params.ghprbActualCommit}; do sleep 60; done
                        """
                }
            }
        }
    }

    stage("Stash tics code") {
        dir("tics") {
            util.checkoutTiCS("${params.ghprbActualCommit}", "${params.ghprbPullId}")
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
    def msg = "Build Result: `${currentBuild.currentResult}`" + "\n" +
            "Elapsed Time: `${duration} mins`" + "\n" +
            "${env.RUN_DISPLAY_URL}"

    echo "${msg}"

}
