pipeline {
    agent any



    stages {
        stage('Clone PythonAssignments Repository') {
            steps {
                script {
                    git(
                        credentialsId: 'b32a65a1-452b-453d-aa16-0e01dec14992',
                        url: 'https://github.com/abhishekrmalvadkar/DataBricks/blob/main/Casting%20columns.py',
                        branches: [[name: '*/**']],
                        directory: 'PythonAssignments' // Specify the directory to clone into
                    )
                }
            }
        }
       }
     }