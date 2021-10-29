catchError {
    def util = load('util.groovy')

    def CURWS = pwd()

    def NPROC = 5

    util.runUTCoverTICS(CURWS, NPROC)

    cobertura autoUpdateHealth: false, autoUpdateStability: false, 
        coberturaReportFile: "/tmp/tiflash_gcovr_coverage.xml", 
        lineCoverageTargets: "${COVERAGE_RATE}, ${COVERAGE_RATE}, ${COVERAGE_RATE}", 
        maxNumberOfBuilds: 10, onlyStable: false, sourceEncoding: 'ASCII', zoomCoverageChart: false
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
