import json
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from undertow.server import make_server


@pytest.fixture
def app(tmp_path):
    server = make_server(0, tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    thread.join()


def test_import_persist_read_and_host_origin_guards(app):
    config = json.load(urlopen(app + "/api/config"))
    body = {
        "title": "Integration",
        "sources": [
            {
                "name": "a.log",
                "content": json.dumps(
                    {
                        "timestamp": "2026-10-15T12:00:00Z",
                        "consumer": "a",
                        "message": "Starting",
                    }
                ),
            }
        ],
    }

    def request(token=None, origin=None):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Undertow-Token"] = token
        if origin:
            headers["Origin"] = origin
        return Request(app + "/api/analyze", json.dumps(body).encode(), headers)

    with pytest.raises(HTTPError) as exc:
        urlopen(request())
    assert exc.value.code == 403
    with pytest.raises(HTTPError) as exc:
        urlopen(request(config["token"], "https://evil.example"))
    assert exc.value.code == 403
    result = json.load(urlopen(request(config["token"])))
    saved = json.load(urlopen(app + "/api/cases/" + result["id"]))
    assert saved["title"] == "Integration"
    assert json.load(urlopen(app + "/api/cases"))[0]["id"] == result["id"]
    with pytest.raises(HTTPError) as exc:
        urlopen(Request(app + "/api/config", headers={"Host": "evil.example"}))
    assert exc.value.code == 403
    with pytest.raises(HTTPError) as exc:
        urlopen(app + "/api/cases/..%2Fsecret")
    assert exc.value.code == 400
