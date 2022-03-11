def coverage() {
    if (ghprbTargetBranch.contains("release-5.2") 
     || ghprbTargetBranch.contains("release-5.1")
     || ghprbTargetBranch.contains("release-5.0") 
     || ghprbTargetBranch.contains("release-4.0") 
     || ghprbTargetBranch.contains("release-3.0")) {
        return false
    }
    return true
}
def page_tools() {
    if (ghprbTargetBranch.contains("release-5.1") 
     || ghprbTargetBranch.contains("release-5.0") 
     || ghprbTargetBranch.contains("release-4.0") 
     || ghprbTargetBranch.contains("release-3.0")) {
        return false
    }
    return true
}
def IDENTIFIER = "tiflash-ut-${ghprbTargetBranch}-${ghprbPullId}"
def parameters = [
        string(name: "ARCH", value: "amd64"),
        string(name: "OS", value: "linux"),
        string(name: "CMAKE_BUILD_TYPE", value: "Debug"),
        string(name: "TARGET_BRANCH", value: ghprbTargetBranch),
        string(name: "TARGET_PULL_REQUEST", value: ghprbPullId),
        string(name: "TARGET_COMMIT_HASH", value: ghprbActualCommit),
        [$class: 'BooleanParameterValue', name: 'BUILD_TIFLASH', value: false],
        [$class: 'BooleanParameterValue', name: 'BUILD_PAGE_TOOLS', value: page_tools()],
        [$class: 'BooleanParameterValue', name: 'BUILD_TESTS', value: true],
        [$class: 'BooleanParameterValue', name: 'ENABLE_CCACHE', value: true],
        [$class: 'BooleanParameterValue', name: 'ENABLE_PROXY_CACHE', value: true],
        [$class: 'BooleanParameterValue', name: 'UPDATE_CCACHE', value: false],
        [$class: 'BooleanParameterValue', name: 'UPDATE_PROXY_CACHE', value: false],
        [$class: 'BooleanParameterValue', name: 'ENABLE_STATIC_ANALYSIS', value: false],
        [$class: 'BooleanParameterValue', name: 'ENABLE_FORMAT_CHECK', value: false],
        [$class: 'BooleanParameterValue', name: 'ENABLE_COVERAGE', value: coverage()],
        [$class: 'BooleanParameterValue', name: 'PUSH_MESSAGE', value: false],
        [$class: 'BooleanParameterValue', name: 'DEBUG_WITHOUT_DEBUG_INFO', value: true],
        [$class: 'BooleanParameterValue', name: 'ARCHIVE_ARTIFACTS', value: true],
        [$class: 'BooleanParameterValue', name: 'ARCHIVE_BUILD_DATA', value: true],
        [$class: 'BooleanParameterValue', name: 'ENABLE_FAILPOINTS', value: true],
    ]


def runBuilderClosure(label, image, Closure body) {
    podTemplate(name: label, label: label, instanceCap: 15, containers: [
            containerTemplate(name: 'runner', image: image,
                    alwaysPullImage: true, ttyEnabled: true, command: 'cat',
                    resourceRequestCpu: '10000m', resourceRequestMemory: '32Gi',
                    resourceLimitCpu: '20000m', resourceLimitMemory: '64Gi'),
    ],
    volumes: [
            nfsVolume(mountPath: '/home/jenkins/agent/dependency', serverAddress: '172.16.5.22',
                    serverPath: '/mnt/ci.pingcap.net-nfs/tiflash/dependency', readOnly: true),
            nfsVolume(mountPath: '/home/jenkins/agent/ci-cached-code-daily', serverAddress: '172.16.5.22',
                    serverPath: '/mnt/ci.pingcap.net-nfs/git', readOnly: true),
    ],
    hostNetwork: false
    ) {
        node(label) {
            container('runner') {
                body()
            }
        }
    }
}

def dispatchRunEnv(toolchain, identifier, Closure body) {
    if (toolchain == 'llvm') {
        runBuilderClosure(identifier, "hub.pingcap.net/tiflash/tiflash-llvm-base:amd64", body)
    } else {
        runBuilderClosure(identifier, "hub.pingcap.net/tiflash/tiflash-builder-ci", body)
    }
}


def existsBuildCache() {
    def status = true
    try {
        def api = "https://ci.pingcap.net/job/tiflash-build-common/api/xml?tree=allBuilds[result,building,actions[parameters[name,value]]]&xpath=(//allBuild[action[parameter[name=%22TARGET_COMMIT_HASH%22%20and%20value=%22${ghprbActualCommit}%22]%20and%20parameter[name=%22BUILD_TESTS%22%20and%20value=%22true%22]]])[1]"
        def response = httpRequest api
        def content = response.getContent()
        if (!content.contains('<building>false</building>') || !content.contains('<result>SUCCESS</result>')) {
            status = false
        }
    } catch (Exception e) {
        println "error: ${e}"
        status = false
    }
    return status
}

def prepareArtifacts(built, get_toolchain) {
    def filter = "*"
    if (get_toolchain) {
        filter = "toolchain"
    }
    if (built != null) {
        copyArtifacts(
            projectName: 'tiflash-build-common',
            selector: specific("${built.number}"),
            filter: filter,
            optional: false
        )
    } else {
        copyArtifacts(
            projectName: 'tiflash-build-common',
            parameters: "TARGET_BRANCH=${ghprbTargetBranch},TARGET_PULL_REQUEST=${ghprbPullId},TARGET_COMMIT_HASH=${ghprbActualCommit},BUILD_TESTS=true",
            filter: filter,
            selector: lastSuccessful(),
            optional: false
        )
    }
}


node(GO_TEST_SLAVE) {
    def toolchain = null
    def built = null
    stage('Build') {
        if (!existsBuildCache()) {
            built = build(
                job: "tiflash-build-common",
                wait: true,
                propagate: true,
                parameters: parameters
            )
        } else {
            echo "Using cached build"
        }
        prepareArtifacts(built, true)
        toolchain = readFile(file: 'toolchain').trim()
        echo "Built with ${toolchain}"
    }
    dispatchRunEnv(toolchain, IDENTIFIER) {
        def cwd = pwd()
        def repo_path = "/home/jenkins/agent/workspace/tiflash-build-common/tics"
        def build_path = "/home/jenkins/agent/workspace/tiflash-build-common/build"
        def binary_path = "/tiflash"
        def prallelism = 8
        stage('Checkout') {
            dir(repo_path) {
                def cache_path = "/home/jenkins/agent/ci-cached-code-daily/src-tics.tar.gz"
                if (fileExists(cache_path)) {
                    println "get code from nfs to reduce clone time"
                    sh """
                    set +x
                    cp -R ${cache_path}  ./
                    tar -xzf ${cache_path} --strip-components=1
                    rm -f src-tics.tar.gz
                    chown -R 1000:1000 ./
                    set -x
                    """
                }
                checkout(changelog: false, poll: false, scm: [
                    $class                           : "GitSCM",
                    branches                         : [
                            [name: ghprbActualCommit],
                    ],
                    userRemoteConfigs                : [
                            [
                                    url          : "git@github.com:pingcap/tics.git",
                                    refspec      : "+refs/pull/${ghprbPullId}/*:refs/remotes/origin/pr/${ghprbPullId}/*",
                                    credentialsId: "github-sre-bot-ssh",
                            ]
                    ],
                    extensions                       : [
                            [$class             : 'SubmoduleOption',
                            disableSubmodules  : toolchain == 'llvm',
                            parentCredentials  : true,
                            recursiveSubmodules: true,
                            trackingSubmodules : false,
                            reference          : ''],
                            [$class: 'PruneStaleBranch'],
                            [$class: 'CleanBeforeCheckout'],
                    ],
                    doGenerateSubmoduleConfigurations: false,
                ])
                sh """
                ln -sf \$(realpath tests) /tests
                """
            }
        }
        stage('Get Artifacts') {
            prepareArtifacts(built, false)
            sh """
            tar -xvaf tiflash.tar.gz
            ln -sf \$(realpath tiflash) /tiflash
            """
            dir(repo_path) {
                sh """
                cp '${cwd}/source-patch.tar.xz' ./source-patch.tar.xz
                tar -xaf ./source-patch.tar.xz
                """
            }
            dir(build_path) {
                sh """
                cp '${cwd}/build-data.tar.xz' ./build-data.tar.xz
                tar -xaf ./build-data.tar.xz
                """
                if (toolchain == 'legacy' && coverage()) {
                    sh "touch -a -m \$(find . -name '*.gcno')"
                }
            }
        }
        stage('Run Tests') {
            sh """
            source /tests/docker/util.sh
            export LLVM_PROFILE_FILE="/tiflash/profile/unit-test-%${prallelism}m.profraw"
            show_env
            ENV_VARS_PATH=/tests/docker/_env.sh OUTPUT_XML=true NPROC=${prallelism} /tests/run-gtest.sh
            """
        }

        stage('Prepare Coverage Report') {
            if (!coverage()) {
                echo "skipped"
            }
            else if(toolchain == 'llvm') {
                dir(repo_path){
                    if (sh(returnStatus: true, script: "which lcov") != 0) {
                        echo "try to install lcov"
                        sh "rpm -i /home/jenkins/agent/dependency/lcov-1.15-1.noarch.rpm"
                    }
                    sh """
                    llvm-profdata merge -sparse /tiflash/profile/*.profraw -o /tiflash/profile/merged.profdata 

                    llvm-cov export \\
                        /tiflash/gtests_dbms /tiflash/gtests_libcommon /tiflash/gtests_libdaemon \\
                        --format=lcov \\
                        --instr-profile /tiflash/profile/merged.profdata \\
                        --ignore-filename-regex "/usr/include/.*" \\
                        --ignore-filename-regex "/usr/local/.*" \\
                        --ignore-filename-regex "/usr/lib/.*" \\
                        --ignore-filename-regex ".*/contrib/.*" \\
                        --ignore-filename-regex ".*/dbms/src/Debug/.*" \\
                        --ignore-filename-regex ".*/dbms/src/Client/.*" \\
                        > /tiflash/profile/lcov.info

                    mkdir -p /tiflash/report
                    genhtml /tiflash/profile/lcov.info -o /tiflash/report/ --ignore-errors source

                    pushd /tiflash
                        tar -czf coverage-report.tar.gz report
                        mv coverage-report.tar.gz ${cwd}
                    popd

                    COMMIT_HASH_BASE=\$(git merge-base origin/${ghprbTargetBranch} HEAD)
                    SOURCE_DELTA=\$(git diff --name-only '\$COMMIT_HASH_BASE' | { grep -E '.*\\.(cpp|h|hpp|cc|c)\$' || true; })
                    echo '### Coverage for changed files' > ${cwd}/diff-coverage
                    echo '```' >> ${cwd}/diff-coverage

                    if [[ -z \$SOURCE_DELTA ]]; then
                        echo 'no c/c++ source change detected' >> ${cwd}/diff-coverage
                    else
                        llvm-cov report /tiflash/gtests_dbms /tiflash/gtests_libcommon /tiflash/gtests_libdaemon -instr-profile /tiflash/profile/merged.profdata \$SOURCE_DELTA > "/tiflash/profile/diff-for-delta"
                        if [[ \$(wc -l "/tiflash/profile/diff-for-delta" | awk -e '{printf \$1;}') -gt 32 ]]; then
                            echo 'too many lines from llvm-cov, please refer to full report instead' >> ${cwd}/diff-coverage
                        else
                            cat /tiflash/profile/diff-for-delta >> ${cwd}/diff-coverage
                        fi
                    fi

                    echo '```' >> ${cwd}/diff-coverage
                    echo '' >> ${cwd}/diff-coverage
                    echo '### Coverage summary' >> ${cwd}/diff-coverage
                    echo '```' >> ${cwd}/diff-coverage
                    llvm-cov report \\
                        --summary-only \\
                        --show-region-summary=false \\
                        --show-branch-summary=false \\
                        --ignore-filename-regex "/usr/include/.*" \\
                        --ignore-filename-regex "/usr/local/.*" \\
                        --ignore-filename-regex "/usr/lib/.*" \\
                        --ignore-filename-regex ".*/contrib/.*" \\
                        --ignore-filename-regex ".*/dbms/src/Debug/.*" \\
                        --ignore-filename-regex ".*/dbms/src/Client/.*" \\
                        /tiflash/gtests_dbms /tiflash/gtests_libcommon /tiflash/gtests_libdaemon -instr-profile /tiflash/profile/merged.profdata | \\
                        grep -E "^(TOTAL|Filename)" | \\
                        cut -d" " -f2- | sed -e 's/^[[:space:]]*//' | sed -e 's/Missed\\ /Missed/g' | column -t >> ${cwd}/diff-coverage
                    echo '```' >> ${cwd}/diff-coverage
                    echo '' >> ${cwd}/diff-coverage
                    echo "[full coverage report](https://ci-internal.pingcap.net/job/tics_ghpr_unit_test/${BUILD_NUMBER}/artifact/coverage-report.tar.gz) (for internal network access only)" >> ${cwd}/diff-coverage
                    """
                }
            } else {
                if (sh(returnStatus: true, script: "which gcovr") != 0) {
                    echo "try to install gcovr"
                    sh """
                    cp '/home/jenkins/agent/dependency/gcovr.tar' '/tmp/'
                    cd /tmp
                    tar xvf gcovr.tar && rm -rf gcovr.tar
                    ln -sf /tmp/gcovr/gcovr /usr/bin/gcovr
                    """
                }
                sh """
                mkdir -p /tiflash/profile/
                gcovr --xml -r ${repo_path} \\
                    --gcov-ignore-parse-errors \\
                    -e "/usr/include/*" \\
                    -e "/usr/local/*" \\
                    -e "/usr/lib/*" \\
                    -e "${repo_path}/contrib/*" \\
                    -e "${repo_path}/dbms/src/Debug/*" \\
                    -e "${repo_path}/dbms/src/Client/*" \\
                    --object-directory=${build_path} -o ${cwd}/tiflash_gcovr_coverage.xml -j ${prallelism} -s >${cwd}/diff-coverage
                """
            }
        }

        stage("Report Coverage") {
            if (coverage()) {
                ut_coverage_result = readFile(file: "diff-coverage").trim()
                withCredentials([string(credentialsId: 'sre-bot-token', variable: 'TOKEN')]) {
                    sh """
                    set +x
                    /home/jenkins/agent/dependency/comment-pr \\
                        --token="\$TOKEN" \\
                        --owner=pingcap \\
                        --repo=tics \\
                        --number=${ghprbPullId} \\
                        --comment='${ut_coverage_result}'
                    set -x
                    """
                }
                if (toolchain == 'llvm') {
                    archiveArtifacts artifacts: "coverage-report.tar.gz"
                } else {
                    cobertura autoUpdateHealth: false, autoUpdateStability: false, 
                        coberturaReportFile: "tiflash_gcovr_coverage.xml", 
                        lineCoverageTargets: "${COVERAGE_RATE}, ${COVERAGE_RATE}, ${COVERAGE_RATE}", 
                        maxNumberOfBuilds: 10, onlyStable: false, sourceEncoding: 'ASCII', zoomCoverageChart: false
                }
            } else {
                echo "skipped"
            }
        }
    }
}
