def test_failure_state_transition():
    node = {
        "status": "ALIVE"
    }

    node["status"] = "SUSPECT"
    assert node["status"] == "SUSPECT"

    node["status"] = "DEAD"
    assert node["status"] == "DEAD"
