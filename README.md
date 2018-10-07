# Sentimento

Stock sentiment analysis by team UVAA

## Setup

> Make sure for each module, use the specific Python version noted in `.python-version`

Follow the instructions on [this site](https://packaging.python.org/guides/installing-using-pip-and-virtualenv/) to install `pip` and `virtualenv`

Then, to start a new module development:

- `cd` to the module directory

- run `virtualenv venv` to create a new isolated environment

- activate your `venv` by `source ./bin/activate`, install any dependencies by `pip install <your dependency>`

- The directory of `venv` contains all libraries and binaries you will use under your module and it is not check into the source.

- before deactivation, run `pip freeze --local > requirements.txt` to dump module dependencies to `requirements.txt`

- deactivate your `venv` by `deactivate`, specify a `.python-version` file with your module's Python version

then you are done.

To work with existing modules:

- `cd` to the module directory

- install a Python version specified in `.python-version`

- run `virtualenv venv` to create a `venv`

- activate your `venv` and run `pip install -r requirements.txt` to install dependencies for that module

- deactivate as above