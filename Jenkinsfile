node {
    cleanWs()

    def version
    stage("checkout") {
        withEnv(['HTTPS_PROXY=http://webproxy-utvikler.nav.no:8088']) {
            sh(script: "git clone https://github.com/navikt/tortuga.git .")
        }

        version = sh(script: 'cat VERSION', returnStdout: true).trim()
    }

    stage("upload manifest") {
        withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'nexusUser', usernameVariable: 'NEXUS_USERNAME', passwordVariable: 'NEXUS_PASSWORD']]) {
            sh "/usr/local/bin/nais upload --app tortuga-hiv -v ${version} -f hiv/nais.yaml"
            sh "/usr/local/bin/nais upload --app tortuga-hoi -v ${version} -f hoi/nais.yaml"
            sh "/usr/local/bin/nais upload --app tortuga-testapi -v ${version} -f testapi/nais.yaml"
        }
    }
}
