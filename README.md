# infx-internal-tools
![Generic badge](https://img.shields.io/badge/python-3.9-blue)
![Generic badge](https://img.shields.io/badge/code%20style-black-000000.svg)
A .gitignore is here as well. any subdirs or files not to be checked should have their relative paths set there.

## Team Conventions
### Branch naming

Our basic convention is INFX-{ticket-number}-lower-case-identifier

### Python Versions and Virtualenvs

If you do want to manage multiple Python versions on your system, you can use **pyenv** to install and switch between
different Python versions and then use **pipenv** to create virtual environments and manage dependencies for each project.

We recommend [pyenv](https://github.com/pyenv/pyenv) for managing multiple Python versions for a project. Pyenv 
works with all versions of Python and allows you to easily switch between different Python versions on the same
system. Due to automation needs we are now [pipenv](https://github.com/pypa/pipenv), it is a tool for managing dependencies and virtual environments for Python projects. It provides a way to create 
isolated Python environments for each project with its own dependencies.

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
$ pyenv install 3.11.4
$ pyenv versions
```

The [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) plugin can be used to manage virtualenvs.

`brew install pyenv-virtualenv`

Load pyenv virtualenv automatically by appending
the following to the end of  ~/.zshrc:

`eval "$(pyenv virtualenv-init -)"`

To create a virtualenv for the Python version used with pyenv, run pyenv virtualenv, specifying the Python version you want and the name of the virtualenv directory. For example,

`$ pyenv virtualenv 3.11.4 infx-internal-tools`

If eval "$(pyenv virtualenv-init -)" is configured in your shell, pyenv-virtualenv will automatically activate/deactivate virtualenvs on entering/leaving directories which contain a .python-version file that contains the name of a valid virtual environment as shown in the output of pyenv virtualenvs (e.g., venv34 or 3.4.3/envs/venv34 in example above) . .python-version files are used by pyenv to denote local Python versions and can be created and deleted with the pyenv local command.

You can also activate and deactivate a pyenv virtualenv manually:

`pyenv activate <name>`
`pyenv deactivate`

virtualenv should work fine with pipenv

### Install Pipenv

pipenv can be installed via pip, pipx or via

`brew install pipenv`

`pipenv install --dev` will install all deps and ensure the python version is correct 3.11.1

`pipenv shell` will give you a virtual environment

In addition, according to the docs your current virtual environment should also work. But you will still need to install the deps via pipenv.

A Pipfile.lock is in the working directory along with Pipfile. Please check this in on any changes. We use caching keyed 
off of the lock file, so the cache will be invalidated on mods to it. If you have challenges installing your dependencies
you may need to check your [Pipfile](https://github.com/pypa/pipfile#pipfile-the-replacement-for-requirementstxt) and possibly
recreate it.


### Testing

We use [Pytest](https://docs.pytest.org/en/6.2.x/) for automated testing, Postman for automated testing.


### Testing and Project Structure

We recommend splitting integration tests and unit tests into separate directories. Code coverage should be calculated using integration tests. 

Our current structure calls for tests to be outside of the app at the top level of the directory. Pytest will discover them.

### Setting up a dev environment

Prerequisites:
- Docker Desktop
- Git (can be installed via xcode command line tools) or GitHub Desktop
- IDE (PyCharm)
- Postman 
- Homebrew (https://brew.sh)

First, follow these directions: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account


### Running a dev server
In the code editor terminal, run command "python -m app.app".
If the API request is calling to RxNav-in-a-box, make sure that container is running in Docker first.
