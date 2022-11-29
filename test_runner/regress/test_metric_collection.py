import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("localhost", 9091)


#
# verify that metrics look minilally sane
#
def metrics_handler(request: Request) -> Response:
    if request.json is None:
        return Response(status=400)

    events = request.json["events"]
    log.info(events)

    checks = {
        "written_size": lambda value: value > 0,
        "physical_size": lambda value: value >= 0,
        "s3_storage_size": lambda value: value == 0,
    }

    for event in events:
        assert checks.pop(event["metric"])(event["value"]), f"{event['metric']} isn't valid"

    assert not checks, f"{' '.join(checks.keys())} wasn't/weren't received"

    return Response(status=200)


def test_metric_collection(httpserver: HTTPServer, neon_env_builder: NeonEnvBuilder):

    # mock http server that returns OK for the metrics
    httpserver.expect_request("/billing/api/v1/usage_events", method="POST").respond_with_handler(
        metrics_handler
    )

    # spin up neon after http server is ready
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("test_metric_collection")
    pg = env.postgres.create_start("test_metric_collection")

    with pg.cursor() as cur:
        cur.execute("CREATE TABLE t as SELECT g FROM generate_series(1, 1000000) g")

    # check that all requests are served
    httpserver.check()
