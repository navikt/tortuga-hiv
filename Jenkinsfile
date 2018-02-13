node {
    cleanWs()

    def version
    stage("checkout") {
        sh "git clone https://github.com/navikt/tortuga.git ."

        version = sh(script: 'cat VERSION', returnStdout: true).trim()
    }

    stage("upload manifest") {
        withCredentials([usernamePassword(credentialsId: 'nexusUploader', usernameVariable: 'NEXUS_USERNAME', passwordVariable: 'NEXUS_PASSWORD')]) {
            sh "make manifest"
        }
    }

    stage("deploy") {
        def commitHash = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()

        build([
            job: 'nais-deploy-pipeline',
            wait: false,
            parameters: [
                string(name: 'APP', value: "tortuga-testapi"),
                string(name: 'REPO', value: "navikt/tortuga"),
                string(name: 'VERSION', value: version),
                string(name: 'COMMIT_HASH', value: commitHash),
                string(name: 'DEPLOY_ENV', value: 'q0')
            ]
        ])

        build([
            job: 'nais-deploy-pipeline',
            wait: false,
            parameters: [
                string(name: 'APP', value: "tortuga-hiv"),
                string(name: 'REPO', value: "navikt/tortuga"),
                string(name: 'VERSION', value: version),
                string(name: 'COMMIT_HASH', value: commitHash),
                string(name: 'DEPLOY_ENV', value: 'q0')
            ]
        ])

        build([
            job: 'nais-deploy-pipeline',
            propagate: false,
            parameters: [
                string(name: 'APP', value: "tortuga-hoi"),
                string(name: 'REPO', value: "navikt/tortuga"),
                string(name: 'VERSION', value: version),
                string(name: 'COMMIT_HASH', value: commitHash),
                string(name: 'DEPLOY_ENV', value: 'q0')
            ]
        ])
    }
}
