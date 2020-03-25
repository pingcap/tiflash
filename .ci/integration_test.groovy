def checkoutTiCS(commit, pullId) {
    def refspec = "+refs/heads/*:refs/remotes/origin/*"
    if (pullId) {
        refspec += " +refs/pull/${pullId}/*:refs/remotes/origin/pr/${pullId}/*"
    }
    checkout(changelog: false, poll: false, scm: [
            $class: "GitSCM",
            branches: [
                    [name: "${commit}"],
            ],
            userRemoteConfigs: [
                    [
                            url: "git@github.com:pingcap/tics.git",
                            refspec: refspec,
                            credentialsId: "github-sre-bot-ssh",
                    ]
            ],
            extensions: [
                    [$class: 'PruneStaleBranch'],
                    [$class: 'CleanBeforeCheckout'],
            ],
    ])
}

def checkoutTiFlash(branch) {
    checkout(changelog: false, poll: true, scm: [
            $class: "GitSCM",
            branches: [
                    [name: "${branch}"],
            ],
            userRemoteConfigs: [
                    [
                            url: "git@github.com:pingcap/tiflash.git",
                            refspec: "+refs/heads/*:refs/remotes/origin/*",
                            credentialsId: "github-sre-bot-ssh",
                    ]
            ],
            extensions: [
                    [$class: 'PruneStaleBranch'],
                    [$class: 'CleanBeforeCheckout'],
            ],
    ])
}

def run(label, Closure body) {
    podTemplate(name: label, label: label, instanceCap: 5, idleMinutes: 5, containers: [
            containerTemplate(name: 'dockerd', image: 'docker:18.09.6-dind', privileged: true,
                            resourceRequestCpu: '2000m', resourceRequestMemory: '8Gi',
                            resourceLimitCpu: '8000m', resourceLimitMemory: '32Gi'),
            containerTemplate(name: 'docker', image: 'hub.pingcap.net/zyguan/docker:build-essential-java',
                            alwaysPullImage: true, envVars: [
                                    envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                            ], ttyEnabled: true, command: 'cat'),
    ]) { node(label) { body() } }
}

catchError {

    def label = "test-tics"

    def tiflashTag = ({
        def m = params.ghprbCommentBody =~ /tiflash\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        }
        return params.ghprbTargetBranch
    }).call()

    def tidbBranch = ({
        def m = params.ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
        if (m) {
            return "${m.group(1)}"
        }
        return params.ghprbTargetBranch
    }).call()

    println "[debug] tiflashTag: $tiflashTag"
    println "[debug] tidbBranch: $tidbBranch"

    parallelsAlwaysFailFast true
    parallel(
        "TiCS Test": {
            run(label+"-self") {
                dir("tics") {
                    stage("Checkout") {
                        container("docker") {
                            sh """
                            archive_url=${FILE_SERVER_URL}/download/builds/pingcap/tics/cache/tics-repo_latest.tar.gz
                            if [ ! -d contrib ]; then curl -sL \$archive_url | tar -zx --strip-components=1 || true; fi
                            """
                            sh "chown -R 1000:1000 ./"
                        }
                        checkoutTiCS("${params.ghprbActualCommit}", "${params.ghprbPullId}")
                    }
                    stage("Test") {
                        timeout(120) {
                            container("docker") {
                                sh """
                                while ! docker pull hub.pingcap.net/tiflash/tics:${params.ghprbActualCommit}; do sleep 60; done
                                """
                                dir("tests/docker") {
                                    try {
                                        sh "TAG=${params.ghprbActualCommit} BRANCH=${tidbBranch} sh -xe run.sh"
                                    } catch(e) {
                                        archiveArtifacts(artifacts: "log/**/*.log", allowEmptyArchive: true)
                                        sh "find log -name '*.log' | xargs tail -n 50"
                                        sh "docker ps -a"
                                        throw e
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "TiFlash Test": {
            run(label+"-tiflash")  {
                stage("Test via TiFlash") {
                    timeout(120) {
                        dir("tispark") {
                            container("docker") {
                                sh """
                                archive_url=${FILE_SERVER_URL}/download/builds/pingcap/tiflash/cache/tiflash-m2-cache_latest.tar.gz
                                if [ ! -d /root/.m2 ]; then curl -sL \$archive_url | tar -zx -C /root || true; fi
                                """
                                sh "chown -R 1000:1000 ./"
                            }
                            git url: "https://github.com/pingcap/tispark.git", branch: "tiflash-ci-test"
                            container("docker") {
                                sh "mvn install -Dmaven.test.skip=true"
                            }
                        }
                        dir("tiflash") {
                            container("docker") {
                                sh "chown -R 1000:1000 ./"
                            }
                            checkoutTiFlash(tiflashTag)
                            container("docker") {
                                dir("tests/maven") {
                                    sh """
                                    while ! docker pull hub.pingcap.net/tiflash/tics:${params.ghprbActualCommit}; do sleep 60; done
                                    """
                                    try {
                                        sh "TAG=${params.ghprbActualCommit} BRANCH=${tidbBranch} sh -xe run.sh"
                                    } catch(e) {
                                        archiveArtifacts(artifacts: "log/**/*.log", allowEmptyArchive: true)
                                        sh "for f in \$(find log -name '*.log'); do echo \"LOG: \$f\"; tail -500 \$f; done"
                                        throw e
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    )

}

stage('Summary') {
    echo "Send slack here ..."
    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
    def slackmsg = "[#${params.ghprbPullId}: ${params.ghprbPullTitle}]" + "\n" +
    "${params.ghprbPullLink}" + "\n" +
    "${params.ghprbPullDescription}" + "\n" +
    "Build Result: `${currentBuild.currentResult}`" + "\n" +
    "Elapsed Time: `${duration} mins` " + "\n" +
    "${env.RUN_DISPLAY_URL}"

    if (currentBuild.currentResult != "SUCCESS") {
        slackSend channel: '#jenkins-ci', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
    }
    node("master") {
        echo "Set status for commit(${params.ghprbActualCommit}) according to build result(${currentBuild.currentResult})"
        currentBuild.result = currentBuild.currentResult
        try {
            step([
                $class: "GitHubCommitStatusSetter",
                reposSource: [$class: "ManuallyEnteredRepositorySource", url: "https://github.com/pingcap/tics"],
                contextSource: [$class: "ManuallyEnteredCommitContextSource", context: "idc-jenkins-ci-tics/test"],
                commitShaSource: [$class: "ManuallyEnteredShaSource", sha: params.ghprbActualCommit],
                statusResultSource: [
                    $class: 'ConditionalStatusResultSource',
                    results: [
                        [$class: 'BetterThanOrEqualBuildResult', result: 'SUCCESS', state: 'SUCCESS', message: "Jenkins job succeeded."],
                        [$class: 'BetterThanOrEqualBuildResult', result: 'FAILURE', state: 'FAILURE', message: "Jenkins job failed."],
                        [$class: 'AnyBuildResult', state: 'ERROR', message: 'Jenkins job meets something wrong.'],
                    ]
                ],
            ]);
        } catch (e) {
            echo "Failed to set commit status: $e"
        }
    }
}
