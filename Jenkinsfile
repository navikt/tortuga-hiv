#!/usr/bin/env groovy
@Library('peon-pipeline') _

node {
    def appToken
    def commitHash
    try {
        cleanWs()

        def version
        stage("checkout") {
            appToken = github.generateAppToken()

            sh "git init"
            sh "git pull https://x-access-token:$appToken@github.com/navikt/tortuga-hiv.git"

            sh "make bump-version"

            version = sh(script: 'cat VERSION', returnStdout: true).trim()
            commitHash = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()

            github.commitStatus("pending", "navikt/tortuga-hiv", appToken, commitHash)
        }

        stage("build") {
            sh "make"
        }

        stage("release") {
            withCredentials([usernamePassword(credentialsId: 'nexusUploader', usernameVariable: 'NEXUS_USERNAME', passwordVariable: 'NEXUS_PASSWORD')]) {
                sh "docker login -u ${env.NEXUS_USERNAME} -p ${env.NEXUS_PASSWORD} repo.adeo.no:5443"
            }

            sh "make release"

            sh "git push --tags https://x-access-token:$appToken@github.com/navikt/tortuga-hiv HEAD:master"
        }

        stage("upload manifest") {
            withCredentials([usernamePassword(credentialsId: 'nexusUploader', usernameVariable: 'NEXUS_USERNAME', passwordVariable: 'NEXUS_PASSWORD')]) {
                sh "make manifest"
            }
        }

        stage("deploy preprod") {
            build([
                    job       : 'nais-deploy-pipeline',
                    propagate : true,
                    parameters: [
                            string(name: 'APP', value: "tortuga-hiv"),
                            string(name: 'REPO', value: "navikt/tortuga-hiv"),
                            string(name: 'VERSION', value: version),
                            string(name: 'COMMIT_HASH', value: commitHash),
                            string(name: 'DEPLOY_ENV', value: 'q0')
                    ]
            ])
        }

        stage("deploy prod") {
            build([
                    job       : 'nais-deploy-pipeline',
                    propagate : true,
                    parameters: [
                            string(name: 'APP', value: "tortuga-hiv"),
                            string(name: 'REPO', value: "navikt/tortuga-hiv"),
                            string(name: 'VERSION', value: version),
                            string(name: 'COMMIT_HASH', value: commitHash),
                            string(name: 'DEPLOY_ENV', value: 'p')
                    ]
            ])
        }

        github.commitStatus("success", "navikt/tortuga-hiv", appToken, commitHash)
    } catch (err) {
        github.commitStatus("failure", "navikt/tortuga-hiv", appToken, commitHash)
        throw err
    }
}
