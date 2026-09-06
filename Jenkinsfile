// Two tiers, in order, each a plain make target. Nothing lives here that
// `make ci` does not also run, so Jenkins is a visible history of the same
// checks the pre-push hook runs, never a different set. docs/DECISIONS.md D11.
pipeline {
  agent any
  options {
    timestamps()
    disableConcurrentBuilds()
    timeout(time: 20, unit: 'MINUTES')
  }
  environment {
    PYTHON = '/opt/ci-venv/bin/python'
    // The platform tier diffs each contract against main. In a Jenkins
    // checkout that ref is origin/main.
    CI_BASE_REF = 'origin/main'
  }
  stages {
    stage('Service tier') {
      steps {
        sh 'make ci-service'
      }
    }
    stage('Platform tier') {
      steps {
        sh 'make ci-platform'
      }
    }
  }
  post {
    failure {
      echo 'A red platform tier lists every contract that no longer holds. That list is the blast radius, shown before merge.'
    }
  }
}
