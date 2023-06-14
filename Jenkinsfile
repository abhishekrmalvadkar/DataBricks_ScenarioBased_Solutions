pipeline {
    agent any



    stages {
        stage('Clone PythonAssignments Repository') {
            steps {
                script {
                    git(
                        credentialsId: 'b3b3ed96-9893-460b-8a8c-d83a22445137',
                        url: 'https://github.com/abhishekrmalvadkar/DataBricks/blob/main/Casting%20columns.py',
                        branches: [[name: '*/**']],
                        directory: 'PythonAssignments' // Specify the directory to clone into
                    )
                }
            }
        }
       }
     }