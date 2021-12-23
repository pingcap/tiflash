def doCheckout(commit, refspec) {
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

def checkoutTiCS(commit, pullId) {
    def refspec = "+refs/heads/*:refs/remotes/origin/*"
    if (pullId) {
        refspec = " +refs/pull/${pullId}/*:refs/remotes/origin/pr/${pullId}/*"
    }
    try {
        doCheckout(commit, refspec)
    } catch (info) {
        retry(2) {
            echo "checkout failed, retry.."
            sleep 5
            if (sh(returnStatus: true, script: '[ -d .git ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                echo ".git already exist or not a valid git dir. Delete dir..."
                deleteDir()
            }
            doCheckout(commit, refspec)
        }
    }
}

def checkoutTiCSFull(commit, pullId) {
    def refspec = "+refs/heads/*:refs/remotes/origin/*"
    if (pullId) {
        refspec += " +refs/pull/${pullId}/*:refs/remotes/origin/pr/${pullId}/*"
    }
    checkout(changelog: false, poll: false, scm: [
            $class                           : "GitSCM",
            branches                         : [
                    [name: "${commit}"],
            ],
            userRemoteConfigs                : [
                    [
                            url          : "git@github.com:pingcap/tics.git",
                            refspec      : refspec,
                            credentialsId: "github-sre-bot-ssh",
                    ]
            ],
            extensions                       : [
                    [$class             : 'SubmoduleOption',
                     disableSubmodules  : false,
                     parentCredentials  : true,
                     recursiveSubmodules: true,
                     trackingSubmodules : false,
                     reference          : ''],
                    [$class: 'PruneStaleBranch'],
                    [$class: 'CleanBeforeCheckout'],
            ],
            doGenerateSubmoduleConfigurations: false,
    ])
}

def runClosure(label, Closure body) {
    podTemplate(name: label, label: label, instanceCap: 15, containers: [
            containerTemplate(name: 'dockerd', image: 'docker:18.09.6-dind', privileged: true,
                    resourceRequestCpu: '5000m', resourceRequestMemory: '10Gi',
                    resourceLimitCpu: '16000m', resourceLimitMemory: '32Gi'),
            containerTemplate(name: 'docker', image: 'hub.pingcap.net/jenkins/docker:build-essential-java',
                    alwaysPullImage: true, envVars: [
                    envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
            ], ttyEnabled: true, command: 'cat'),
            containerTemplate(name: 'builder', image: 'hub.pingcap.net/tiflash/tiflash-builder-ci',
                    alwaysPullImage: true, ttyEnabled: true, command: 'cat',
                    resourceRequestCpu: '5000m', resourceRequestMemory: '10Gi',
                    resourceLimitCpu: '10000m', resourceLimitMemory: '30Gi'),
            containerTemplate(name: 'builder-llvm', image: 'hub.pingcap.net/tiflash/tiflash-llvm-base:amd64',
                    alwaysPullImage: true, ttyEnabled: true, command: 'cat',
                    resourceRequestCpu: '5000m', resourceRequestMemory: '10Gi',
                    resourceLimitCpu: '10000m', resourceLimitMemory: '30Gi'),
    ],
    volumes: [
            nfsVolume(mountPath: '/home/jenkins/agent/ci-cached-code-daily', serverAddress: '172.16.5.22',
                    serverPath: '/mnt/ci.pingcap.net-nfs/git', readOnly: false),
    ]
    ) {
        node(label) {
            body()
        }
    }
}

def runWithTiCSFull(label, curws, Closure body) {
    runClosure(label) {
        dir("${curws}/tics") {
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
                    } else {
                        sh "exit -1"
                    }
                }
                checkoutTiCSFull("${ghprbActualCommit}", "${ghprbPullId}")
            }
        }
        body()
    }
}

def runTest(label, testPath, tidbBranch) {
    runClosure(label) {
        stage("Unstash") {
            unstash 'git-code-tics'
            dir("tics") {
                timeout(time: 5, unit: 'MINUTES') {
                    container("docker") {
                        sh """
                        pwd
                        DOWNLOAD_TAR=true COMMIT_HASH=${params.ghprbActualCommit} PULL_ID=${params.ghprbPullId} TAR_PATH=./tests/.build bash -e release-centos7/build/fetch-ci-build.sh
                        """
                    }
                }
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
                            sh "mv log ${label}-log"
                            archiveArtifacts(artifacts: "${label}-log/**/*.log", allowEmptyArchive: true)
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

def runUTCoverTICS(CURWS, NPROC) {
    def NPROC_UT = NPROC * 2
    runWithTiCSFull("ut-tics-llvm", CURWS) {
        dir("${CURWS}/tics") {
            stage("Build") {
                timeout(time: 70, unit: 'MINUTES') {
                    container("builder-llvm") {
                        sh "NPROC=${NPROC} BUILD_BRANCH=${ghprbTargetBranch} UPDATE_CCACHE=false ${CURWS}/tics/release-centos7-llvm/scripts/build-tiflash-ut-coverage.sh"
                    }
                }
            }
            stage("Tests") {
                timeout(time: 50, unit: 'MINUTES') {
                    container("builder-llvm") {
                        sh "NPROC=${NPROC_UT} /build/tics/release-centos7-llvm/scripts/run-ut.sh"
                    }
                }
            }
            stage("Show UT Coverage") {
                timeout(time: 5, unit: 'MINUTES') {
                    container("builder-llvm") {
                        sh "NPROC=${NPROC} /build/tics/release-centos7-llvm/scripts/upload-ut-coverage.sh"
                        sh """
                        cp /tmp/tiflash_gcovr_coverage.xml ./
                        cp /tmp/tiflash_gcovr_coverage.res ./
                        chown -R 1000:1000 tiflash_gcovr_coverage.xml tiflash_gcovr_coverage.res
                        """
                        ut_coverage_result = sh(script: "cat tiflash_gcovr_coverage.res", returnStdout: true).trim()
                        sh """
                        rm -f comment-pr
                        curl -O http://fileserver.pingcap.net/download/comment-pr
                        chmod +x comment-pr
                        set +x
                        ./comment-pr --token=$TOKEN --owner=pingcap --repo=tics --number=${ghprbPullId} --comment="Coverage detail: ${CI_COVERAGE_BASE_URL}/${BUILD_NUMBER}/cobertura/  \n(Coverage detail url is limited office network access)   \n\n ${ut_coverage_result}"
                        set -x
                        """
                    }
                }

                cobertura autoUpdateHealth: false, autoUpdateStability: false, 
                    coberturaReportFile: "tiflash_gcovr_coverage.xml", 
                    lineCoverageTargets: "${COVERAGE_RATE}, ${COVERAGE_RATE}, ${COVERAGE_RATE}", 
                    maxNumberOfBuilds: 10, onlyStable: false, sourceEncoding: 'ASCII', zoomCoverageChart: false
            }
        }
    }
}

return this
