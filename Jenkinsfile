#!groovy
def BN = BRANCH_NAME == "master" || BRANCH_NAME.startsWith("releases/") ? BRANCH_NAME : "master"

library "knime-pipeline@$BN"

properties([
    pipelineTriggers([
        upstream("knime-database/${env.BRANCH_NAME.replaceAll('/', '%2F')}" +
            ", knime-textprocessing/${env.BRANCH_NAME.replaceAll('/', '%2F')}")
    ]),
	parameters(workflowTests.getConfigurationsAsParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    knimetools.defaultTychoBuild('org.knime.update.cloud')

    workflowTests.runTests(
        dependencies: [
            repositories: ['knime-cloud', 'knime-js-base', 'knime-filehandling','knime-textprocessing','knime-database','knime-kerberos','knime-streaming'],
        ]
    )

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar()
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}

/* vim: set shiftwidth=4 expandtab smarttab: */
