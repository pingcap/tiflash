def checkoutTiCS(commit, pullId) {
    def refspec = "+refs/heads/*:refs/remotes/origin/*"
    if (pullId) {
        refspec += " +refs/pull/${pullId}/*:refs/remotes/origin/pr/${pullId}/*"
    }
    if (sh(returnStatus: true, script: '[ -d .git ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
        echo ".git already exist or not a valid git dir. Delete dir..."
        deleteDir()
    }
    try {
        checkout(changelog: false, poll: false, scm: [
                $class           : "GitSCM",
                branches         : [
                        [name: "${commit}"],
                ],
                userRemoteConfigs: [
                        [
                                url          : "git@github.com:pingcap/tics.git",
                                refspec      : refspec,
                                credentialsId: "github-sre-bot-ssh",
                        ]
                ],
                extensions       : [
                        [$class: 'PruneStaleBranch'],
                        [$class: 'CleanBeforeCheckout'],
                ],
        ])
    } catch (info) {
        retry(2) {
            echo "checkout failed, retry.."
            sleep 5
            if (sh(returnStatus: true, script: '[ -d .git ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                echo ".git already exist or not a valid git dir. Delete dir..."
                deleteDir()
            }
            checkout(changelog: false, poll: false, scm: [
                    $class           : "GitSCM",
                    branches         : [
                            [name: "${commit}"],
                    ],
                    userRemoteConfigs: [
                            [
                                    url          : "git@github.com:pingcap/tics.git",
                                    refspec      : refspec,
                                    credentialsId: "github-sre-bot-ssh",
                            ]
                    ],
                    extensions       : [
                            [$class: 'PruneStaleBranch'],
                            [$class: 'CleanBeforeCheckout'],
                    ],
            ])
        }
    }
}

def runClosure(label, Closure body) {
    podTemplate(name: label, label: label, instanceCap: 15, containers: [
            containerTemplate(name: 'dockerd', image: 'docker:18.09.6-dind', privileged: true,
                    resourceRequestCpu: '5000m', resourceRequestMemory: '10Gi',
                    resourceLimitCpu: '16000m', resourceLimitMemory: '32Gi'),
            containerTemplate(name: 'docker', image: 'hub.pingcap.net/zyguan/docker:build-essential-java',
                    alwaysPullImage: true, envVars: [
                    envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
            ], ttyEnabled: true, command: 'cat'),
    ]) {
        node(label) {
            body()
        }
    }
}

def runTest(label, testPath, tidbBranch) {
    runClosure(label) {
        stage("Unstash") {
            dir("tics") {
                unstash 'git-code-tics'
            }
        }
        dir(testPath) {
            stage("Test") {
                timeout(time: 60, unit: 'MINUTES') {
                    container("docker") {
                        try {
                            sh "pwd"
                            sh "TAG=${params.ghprbActualCommit} BRANCH=${tidbBranch} bash -xe ./run.sh"
                        } catch (e) {
                            archiveArtifacts(artifacts: "log/**/*.log", allowEmptyArchive: true)
                            sh "find log -name '*.log' | xargs tail -n 500"
                            sh "docker ps -a"
                            throw e
                        }
                    }
                }
            }
        }
    }
}

return this
