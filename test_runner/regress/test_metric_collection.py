import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PortDistributor
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


@pytest.fixture(scope="session")
def httpserver_listen_address(port_distributor: PortDistributor):
    port = port_distributor.get_port()
    return ("localhost", port)


num_metrics_received = 0


#
# verify that metrics look minilally sane
#
def metrics_handler(request: Request) -> Response:
    if request.json is None:
        return Response(status=400)

    events = request.json["events"]
    log.info("received events:")
    log.info(events)

    checks = {
        "written_size": lambda value: value > 0,
        "physical_size": lambda value: value >= 0,
        "s3_storage_size": lambda value: value == 0,
    }

    for event in events:
        assert checks.pop(event["metric"])(event["value"]), f"{event['metric']} isn't valid"

    assert not checks, f"{' '.join(checks.keys())} wasn't/weren't received"

    global num_metrics_received
    num_metrics_received += 1
    return Response(status=200)


def test_metric_collection(
    httpserver: HTTPServer, neon_env_builder: NeonEnvBuilder, httpserver_listen_address
):
    (host, port) = httpserver_listen_address
    metric_collection_endpoint = f"http://{host}:{port}/billing/api/v1/usage_events"

    neon_env_builder.pageserver_config_override = f"""
    metric_collection_endpoint="{metric_collection_endpoint}"
    """

    log.info(f"test_metric_collection endpoint is {metric_collection_endpoint}")

    # mock http server that returns OK for the metrics
    httpserver.expect_oneshot_request(
        "/billing/api/v1/usage_events", method="POST"
    ).respond_with_handler(metrics_handler)

    # spin up neon,  after http server is ready
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("test_metric_collection")
    pg = env.postgres.create_start("test_metric_collection")

    with pg.cursor() as cur:
        cur.execute("CREATE TABLE t as SELECT g FROM generate_series(1, 1000000) g")

    # check that all requests are served
    httpserver.check()
    global num_metrics_received
    assert num_metrics_received > 0, "no metrics were received"
