import pytest
import shutil
import tempfile

from fakeredis import FakeStrictRedis

from asu import create_app


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture
def redis():
    r = FakeStrictRedis()
    r.sadd("packages-snapshot-testtarget/testsubtarget", "test1", "test2", "test3")
    r.hset(
        "profiles-snapshot-snapshot",
        mapping={"testprofile": "testtarget/testsubtarget"},
    )
    r.hset("mapping-snapshot", mapping={"testvendor,testprofile": "testprofile"})
    r.sadd("targets-snapshot", "testtarget/testsubtarget")
    yield r


@pytest.fixture
def app(redis):
    test_path = tempfile.mkdtemp()
    app = create_app(
        {
            "CACHE_PATH": test_path + "/cache",
            "JSON_PATH": test_path + "/json",
            "REDIS_CONN": redis,
            "STORE_PATH": test_path + "/store",
            "TESTING": True,
            "UPSTREAM_URL": "http://localhost:8001",
            "VERSIONS": {
                "metadata_version": 1,
                "branches": [
                    {
                        "name": "snapshot",
                        "enabled": True,
                        "versions": ["snapshot"],
                        "git_branch": "master",
                        "path": "snapshots",
                        "pubkey": "RWS1BD5w+adc3j2Hqg9+b66CvLR7NlHbsj7wjNVj0XGt/othDgIAOJS+",
                        "updates": "dev",
                    },
                    {
                        "name": "19.07",
                        "enabled": True,
                        "versions": ["19.07.6", "19.07.5"],
                        "git_branch": "openwrt-19.07",
                        "path": "releases/{version}",
                        "pubkey": "RWS1BD5w+adc3j2Hqg9+b66CvLR7NlHbsj7wjNVj0XGt/othDgIAOJS+",
                        "updates": "stable",
                    },
                ],
            },
        }
    )

    yield app

    shutil.rmtree(test_path)


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def runner(app):
    return app.test_cli_runner()


@pytest.fixture
def httpserver_listen_address():
    return ("127.0.0.1", 8001)
