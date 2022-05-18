# infx-internal-tools

A .gitignore is here as well. any subdirs or files not to be checked should have their relative paths set there.

## Team Conventions
### Branch naming

Our basic convention is CLIN-{ticket-number}-lower-case-identifier

### Python Versions and Virtualenvs

You can still use pyenv to manage different versions of python and you can stil use virtualenv. However, do to automation needs we are now using the preferred [pipenv](https://github.com/pypa/pipenv)

pipenv can be installed via pip, pipx or via

`brew install pipenv`

`pipenv install --dev` will install all deps and ensure the python version is correct 3.9

`pipenv shell` will give you a virtual environment

In addition, according to the docs your current virtual environment should also work. But you will still need to install the deps via pipenv.

A Pipefile.lock is in the working directory along with Pipfile. Please check this in on any changes. We use caching keyed off of the lock file, so the cache will be invalidated on mods to it.

We still recommend [pyenv](https://github.com/pyenv/pyenv) for managing particular versions of Python per project as pyenv works with all versions of Python 2 and 3. if you want to manage different versions of python for various tasks. Or you can use pipenv as well. Your choice.

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
$ pyenv install 2.7.6
$ pyenv install 2.6.8
$ pyenv versions
  system
  2.6.8
* 2.7.6 (set by /home/yyuu/.pyenv/version)
```
`$ pyenv local`

Sets a local application-specific Python version by writing the version name to a .python-version file in the current directory. This version overrides the global version, and can be overridden itself by setting the PYENV_VERSION environment variable or with the pyenv shell command.

`$ pyenv local 2.7.6`

When you switch to a project directory with a .python-version, pyenv will switch to the proper version

The [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) plugin can be used to manage virtualenvs.

`brew install pyenv-virtualenv`

Load pyenv virtualenv automatically by appending
the following to the end of  ~/.zshrc:

`eval "$(pyenv virtualenv-init -)"`

To create a virtualenv for the Python version used with pyenv, run pyenv virtualenv, specifying the Python version you want and the name of the virtualenv directory. For example,

`$ pyenv virtualenv 2.7.10 my-virtual-env-2.7.10`

If eval "$(pyenv virtualenv-init -)" is configured in your shell, pyenv-virtualenv will automatically activate/deactivate virtualenvs on entering/leaving directories which contain a .python-version file that contains the name of a valid virtual environment as shown in the output of pyenv virtualenvs (e.g., venv34 or 3.4.3/envs/venv34 in example above) . .python-version files are used by pyenv to denote local Python versions and can be created and deleted with the pyenv local command.

You can also activate and deactivate a pyenv virtualenv manually:

`pyenv activate <name>`
`pyenv deactivate`

virtualenv should work fine with pipenv

### Testing

[Pytest](https://docs.pytest.org/en/6.2.x/) is recommended for testing.


### Testing and Project Structure

We recommend splitting integration tests and unit tests into separate directories. Code coverage should be calculated using unit tests. We can guide to this platonic ideal.

Our current structure calls for tests to be outside of the app at the top level of the directory. Pytest will discover them.

### Setting up a dev environment

Prerequisites:
- Docker Desktop
- Git (can be installed via xcode command line tools)
- IDE (VSCode, PyCharm, etc.)
- Postman
- Homebrew (https://brew.sh)
- Postgres (install via Homebrew with `brew install postgresql`)

First, follow these directions: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account


### Running a dev server
In the code editor terminal, run command "python -m app.app".
If the API request is calling to RxNav-in-a-box, make sure that container is running in Docker first.
