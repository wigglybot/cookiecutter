from subprocess import run
import os

PROJECT_DIR = os.path.join(os.getcwd())
APP_DIR = os.path.join(PROJECT_DIR, "app")
VENV = os.path.join(APP_DIR, ".venv")
os.mkdir(VENV)

run("cd %s && pipenv run pipenv install --dev" % APP_DIR, shell=True, check=True)
