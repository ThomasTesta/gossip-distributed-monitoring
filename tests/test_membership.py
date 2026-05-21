def test_membership_entry_creation():
    membership = {}

    membership["node-1"] = {
        "heartbeat": 1,
        "status": "ALIVE"
    }

    assert "node-1" in membership
    assert membership["node-1"]["status"] == "ALIVE"
    assert membership["node-1"]["heartbeat"] == 1
