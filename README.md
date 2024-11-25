## gwvolman

Girder Worker Volume Manager Plugin for WholeTale

### Contents

The `gwvolman` plugin provides methods for handling the docker container
and publishing jobs.

`gwvolman/`: Base plugin directory
 
`lib/`: Contains publishing logic 
  

### Running Unit Tests

To run the unit tests, navigate to the gwvolman project in the terminal.
Then, run the tests in a virtual environment with the following

```
pip install pipenv --user
pipenv --python 3.8
pipenv install
pipenv run pip install -e .
pipenv run pytest --cov=gwvolman .
```

### Notes

1. For K8s deployment it is necessary to use Docker Hub credentials:
    1. Obtain a personal access token from Docker Hub (read access to public repos is enough)
    1. Create a json file with the following content:
       ```
       {
         "auths": {
           "https://index.docker.io/v1/": {
             "auth": "<base64 encoded username:password>"
           }
         }
       }
       ```
    1. Encode the username and password in base64:
       ```
       echo -n "username:password" | base64
       ```
    1. Replace `<base64 encoded username:password>` with the output from the previous command
    1. Create a secret in the K8s cluster:
       ```
       kubectl create secret generic dockerhub-creds --from-file=.dockerconfigjson=<path to the json file>
       ```
    1. Set `DOCKER_PULL_SECRET` env var to `dockerhub-creds`
