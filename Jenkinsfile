#!/usr/bin/env groovy
node {
    def commitHash
    try {
        cleanWs()

        def version
        stage("checkout") {
            sh "git clone https://github.com/navikt/tortuga.git ."

            sh "make bump-version"

            version = sh(script: 'cat VERSION', returnStdout: true).trim()

            commitHash = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
            notifyGithub("navikt", "tortuga", 'continuous-integration/jenkins', commitHash, 'pending', "Build #${env.BUILD_NUMBER} has started")
        }

        stage("build") {
            sh "make"
        }

        stage("release") {
            withCredentials([usernamePassword(credentialsId: 'nexusUploader', usernameVariable: 'NEXUS_USERNAME', passwordVariable: 'NEXUS_PASSWORD')]) {
                sh "docker login -u ${env.NEXUS_USERNAME} -p ${env.NEXUS_PASSWORD} repo.adeo.no:5443"
            }

            sh "make release"

            withCredentials([string(credentialsId: 'navikt-ci-oauthtoken', variable: 'GITHUB_OAUTH_TOKEN')]) {
                sh "git push --tags https://$GITHUB_OAUTH_TOKEN@github.com/navikt/tortuga HEAD:master"
            }
        }

        stage("upload manifest") {
            withCredentials([usernamePassword(credentialsId: 'nexusUploader', usernameVariable: 'NEXUS_USERNAME', passwordVariable: 'NEXUS_PASSWORD')]) {
                sh "make manifest"
            }
        }

        stage("deploy") {
            build([
                    job       : 'nais-deploy-pipeline',
                    wait      : false,
                    parameters: [
                            string(name: 'APP', value: "tortuga-testapi"),
                            string(name: 'REPO', value: "navikt/tortuga"),
                            string(name: 'VERSION', value: version),
                            string(name: 'COMMIT_HASH', value: commitHash),
                            string(name: 'DEPLOY_ENV', value: 'q0')
                    ]
            ])

            build([
                    job       : 'nais-deploy-pipeline',
                    wait      : false,
                    parameters: [
                            string(name: 'APP', value: "tortuga-hiv"),
                            string(name: 'REPO', value: "navikt/tortuga"),
                            string(name: 'VERSION', value: version),
                            string(name: 'COMMIT_HASH', value: commitHash),
                            string(name: 'DEPLOY_ENV', value: 'q0')
                    ]
            ])

            build([
                    job       : 'nais-deploy-pipeline',
                    propagate : false,
                    parameters: [
                            string(name: 'APP', value: "tortuga-hoi"),
                            string(name: 'REPO', value: "navikt/tortuga"),
                            string(name: 'VERSION', value: version),
                            string(name: 'COMMIT_HASH', value: commitHash),
                            string(name: 'DEPLOY_ENV', value: 'q0')
                    ]
            ])
        }

        notifyGithub("navikt", "tortuga", 'continuous-integration/jenkins', commitHash, 'success', "Build #${env.BUILD_NUMBER} has finished")
    } catch (err) {
        notifyGithub("navikt", "tortuga", 'continuous-integration/jenkins', commitHash, 'failure', "Build #${env.BUILD_NUMBER} has failed")

        throw err
    }
}

def notifyGithub(owner, repo, context, sha, state, description) {
    def postBody = [
            state: "${state}",
            context: "${context}",
            description: "${description}",
            target_url: "${env.BUILD_URL}"
    ]
    def postBodyString = groovy.json.JsonOutput.toJson(postBody)

    withEnv(['HTTPS_PROXY=http://webproxy-utvikler.nav.no:8088']) {
        withCredentials([string(credentialsId: 'navikt-ci-oauthtoken', variable: 'GITHUB_OAUTH_TOKEN')]) {
            sh """
                curl -H 'Authorization: token ${GITHUB_OAUTH_TOKEN}' \
                    -H 'Content-Type: application/json' \
                    -X POST \
                    -d '${postBodyString}' \
                    'https://api.github.com/repos/${owner}/${repo}/statuses/${sha}'
            """
        }
    }
}
