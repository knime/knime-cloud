#!groovy
def BN = BRANCH_NAME == "master" || BRANCH_NAME.startsWith("releases/") ? BRANCH_NAME : "master"

library "knime-pipeline@$BN"

properties([
    pipelineTriggers([
        upstream('knime-database/' + env.BRANCH_NAME.replaceAll('/', '%2F')),
        upstream('knime-textprocessing/' + env.BRANCH_NAME.replaceAll('/', '%2F'))
    ]),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    // provide the name of the update site project
    knimetools.defaultTychoBuild('org.knime.update.cloud')

    // Specifying configurations is optional. If omitted, the default configurations will be used
    // (see jenkins-pipeline-libraries/vars/workflowTests.groovy)
    // def testConfigurations = [
    //     "ubuntu18.04 && python-3",
    //     "windows && python-3"
    // ]

    // workflowTests.runTests(
    //     dependencies: [
    //         // A list of repositories required for running workflow tests. All repositories that are required for a minimal
    //         // KNIME AP installation are added by default and don't need to be specified here. Currently these are:
    //         //
    //         // 'knime-tp', 'knime-shared', 'knime-core', 'knime-base', 'knime-workbench', 'knime-expressions',
    //         // 'knime-js-core','knime-svg', 'knime-product'
    //         //
    //         // All features (not plug-ins!) in the specified repositories will be installed.
    //         repositories: ['knime-ap-repository-template', 'knime-json', 'knime-python'],
    //         // an optional list of additional bundles/plug-ins from the repositories above that must be installed
    //         ius: ['org.knime.json.tests']
    //     ],
    //     // this is optional and defaults to false
    //     withAssertions: true,
    //     // this is optional and only needs to be provided if non-default configurations are used, see above
    //     // configurations: testConfigurations
    // )

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        // TODO: remove empty test configuration once test are enabled
        workflowTests.runSonar([])
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}

/* vim: set shiftwidth=4 expandtab smarttab: */
