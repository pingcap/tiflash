def parameters = [
        string(name: "ARCH", value: "amd64"),
        string(name: "OS", value: "linux"),
        string(name: "CMAKE_BUILD_TYPE", value: "Debug"),
        string(name: "TARGET_BRANCH", value: ghprbTargetBranch),
        string(name: "TARGET_PULL_REQUEST", value: ghprbPullId),
        string(name: "TARGET_COMMIT_HASH", value: ghprbActualCommit),
        [$class: 'BooleanParameterValue', name: 'BUILD_TIFLASH', value: true],
        [$class: 'BooleanParameterValue', name: 'BUILD_PAGE_TOOLS', value: false],
        [$class: 'BooleanParameterValue', name: 'BUILD_TESTS', value: false],
        [$class: 'BooleanParameterValue', name: 'ENABLE_CCACHE', value: true],
        [$class: 'BooleanParameterValue', name: 'ENABLE_PROXY_CACHE', value: true],
        [$class: 'BooleanParameterValue', name: 'UPDATE_CCACHE', value: false],
        [$class: 'BooleanParameterValue', name: 'UPDATE_PROXY_CACHE', value: false],
        [$class: 'BooleanParameterValue', name: 'ENABLE_STATIC_ANALYSIS', value: true],
        [$class: 'BooleanParameterValue', name: 'ENABLE_FORMAT_CHECK', value: true],
        [$class: 'BooleanParameterValue', name: 'ENABLE_COVERAGE', value: false],
        [$class: 'BooleanParameterValue', name: 'PUSH_MESSAGE', value: false],
        [$class: 'BooleanParameterValue', name: 'DEBUG_WITHOUT_DEBUG_INFO', value: true],
        [$class: 'BooleanParameterValue', name: 'ARCHIVE_ARTIFACTS', value: true],
        [$class: 'BooleanParameterValue', name: 'ENABLE_FAILPOINTS', value: true],
    ]

stage('Build') {
    build job: "tiflash-build-common",
        wait: true,
        propagate: true,
        parameters: parameters
}
