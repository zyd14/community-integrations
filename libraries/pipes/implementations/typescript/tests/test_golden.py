from dagster_pipes_tests import PipesTestSuite
import pytest
import subprocess


@pytest.fixture(scope="session", autouse=True)
def built_binary():
    subprocess.run(["npm", "run", "build"], check=True)


class TestTypescriptPipes(PipesTestSuite):
    BASE_ARGS = ["node", "dist/test/golden_test.js"]
