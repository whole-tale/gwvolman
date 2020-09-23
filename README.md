## gwvolman

Girder Worker Volume Manager Plugin for WholeTale

### Contents

The `gwvolman` plugin provides methods for handling the docker container
and publishing jobs.

`gwvolman/`: Base plugin directory
 
`lib/`: Contains publishing logic 
  
`lib/dataone`: DataONE related publishing code


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
