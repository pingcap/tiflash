catchError {
    def util = load('util.groovy')

    def CURWS = pwd()

    def NPROC = 5

    util.runUnitTests("ut-tics-v2", CURWS, NPROC)

}

stage('Summary') {
    println("Coverage detail: ${CI_COVERAGE_BASE_URL}/${BUILD_NUMBER}/cobertura/")
    println("Coverage detail url is limited office network access\n")

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
