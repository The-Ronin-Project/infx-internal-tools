# infx-internal-tools
![Generic badge](https://img.shields.io/badge/python-3.9-blue)
![Generic badge](https://img.shields.io/badge/code%20style-black-000000.svg)
A .gitignore is here as well. any subdirs or files not to be checked should have their relative paths set there.

## Team Conventions
### Branch naming

Our basic convention is INFX-{ticket-number}-lower-case-identifier

### Install Python

Download and install Python.

### Python Versions and Virtualenvs

To support automation tools provided by the Platform team, we use [pipenv](https://github.com/pypa/pipenv) 
to ensure we all run the same Python version locally in a virtual environment. 

The Python version on your local machine may be the same or later than the version in the virtual environment.

The version is controlled in the [Pipfile](Pipfile) `python_version` setting for this project.
That setting value changes rarely and only when the Platform team announces an update.

### Install pyenv

`brew install pyenv`

Load pyenv automatically by appending
the following to ~/.zshrc:

open .zshrc using `open ~/.zshrc`

If the file does not already exist, create it using `touch ~/.zshrc`

Then append the following line to the end of the file.

`eval "$(pyenv init -)"`

To list the all available versions of Python, including Anaconda, Jython, pypy, and stackless, use:
`$ pyenv install --list`

Then install the desired versions:
```
$ pyenv install 3.9.10
$ pyenv versions
```

The [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) plugin can be used to manage virtualenvs.

`brew install pyenv-virtualenv`

Load pyenv virtualenv automatically by appending
the following to the end of  ~/.zshrc:

`eval "$(pyenv virtualenv-init -)"`

To create a virtualenv for the Python version used with pyenv, run pyenv virtualenv, specifying the Python version you are using and the name of the virtualenv directory. For example,

`$ pyenv virtualenv 3.9.10 infx-internal-tools`

If eval "$(pyenv virtualenv-init -)" is configured in your shell, pyenv-virtualenv will automatically activate/deactivate virtualenvs on entering/leaving directories which contain a .python-version file that contains the name of a valid virtual environment as shown in the output of pyenv virtualenvs (e.g., venv34 or 3.4.3/envs/venv34 in example above) . .python-version files are used by pyenv to denote local Python versions and can be created and deleted with the pyenv local command.

You can also activate and deactivate a pyenv virtualenv manually:

`pyenv activate <name>`
`pyenv deactivate`

virtualenv should work fine with pipenv

### Install Pipenv

pipenv can be installed via pip, pipx or via

`brew install pipenv`

`pipenv install --dev` will install all dependencies and set the `python_version` to the value in the [Pipfile](Pipfile) 

A `Pipfile.lock` file is in the working directory along with Pipfile. Please check this in on any changes. We use caching keyed 
off of the lock file, so the cache will be invalidated on mods to it. If you have challenges installing your dependencies
you may need to check your [Pipfile](Pipfile) and possibly
recreate it.

Contact a team member for:
- pgAdmin access: you need to ask the IT team for a username and password  
- details about creating your own .env file from our .env.template
- get a PEM file that contains your key for OCI access, create a folder `.oci` at your top level folder, and put the PEM file there
- ask your team to be added to the database access group `clinical-intelligence`


### Setting up a dev environment

First, follow these directions: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account

Tool set:
- Docker Desktop
- Git (can be installed via xcode command line tools, or use GitHub Desktop)
- PyCharm IDE
- CodeWithMe
- Postman 
- Homebrew (https://brew.sh)
- DataDog

In PyCharm:
1. Settings > Project: Python Interpreter > Virtualenv Environment > in the dropdown, navigate to your local python > Apply > OK
2. You should now see a number of plugins listed, including Black for code style
3. CodeWithMe person icon at top right in PyCharm window > Permissions > enable Full Access, disable Start Call
4. Settings > Tools > Plugins > Database Tools and SQL > click Disabled > Apply > OK
5. Restart PyCharm


### Running a dev server

1. In PyCharm, working in the infx-internal-tools repo, open a terminal session.
2. In the terminal session, activate your environment by name: `pyenv activate infx-internal-tools`
3. Environment is active when you see that name at the start of the terminal prompt: `(infx-internal-tools) `
4. If dependencies and/or the [Pipfile](Pipfile) have changed recently, also run: `pipenv install --dev`
5. Start the local dev server with: `python -m app.app`
6. See messages that the Flask app is serving, Debug mode is on, and the Debugger is active.
7. When viewing a Python file in PyCharm, run and debug controls are available at top right in the PyCharm window.
8. In Postman, in the Informatics workspace, choose your working environment:
    - `Dev-Local` to run the code in your local environment:
    - `Prod` to run the code currently deployed in Prod
    - `Dev` to run the code currently deployed in Dev
    - `Dev-Docker` to run the Docker image in your local machine environment
9. Then run test calls in Postman.
10. Use PyCharm to debug and step through the code running in your virtual environment. 
10. Postman will display messages when there are issues. In Dev and Prod, DataDog also displays messages. 
11. To debug Dev or Prod, first synchronize your local virtual environment with the deployed code version, then debug.

When using Postman, if the API request is calling to RxNav-in-a-box, make sure that container is running in Docker first.


### Testing

We use [Pytest](https://docs.pytest.org/en/6.2.x/) for automated testing, Postman for manual testing.


### Testing and Project Structure

We recommend splitting integration tests and unit tests into separate directories. Code coverage should be calculated using integration tests. 

Our current structure calls for tests to be outside the app at the top level of the directory. Pytest will discover them.