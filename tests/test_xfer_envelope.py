from typing import Optional

from common.xfer import envelope


class DummyMessage:
    def __init__(self, payload: bytes, props: Optional[dict] = None):
        self._payload = payload
        self._props = props or {}

    def get_payload_as_bytes(self):
        return self._payload

    def get_properties(self):
        return self._props


def test_parse_meta_ok():
    meta = envelope.MetaPayload(
        kind=envelope.KIND_META,
        transfer_id="abc",
        file_name="a.bin",
        file_size=10,
        chunk_size=5,
        total_chunks=2,
        sha256="deadbeef",
    )
    msg = DummyMessage(
        meta.to_json().encode("utf-8"),
        {envelope.PROP_KIND: "meta", envelope.PROP_ID: "abc"},
    )
    parsed, err = envelope.try_parse_meta(msg)
    assert err == ""
    assert parsed is not None
    assert parsed.transfer_id == "abc"
    assert parsed.file_name == "a.bin"
    assert parsed.total_chunks == 2


def test_parse_meta_wrong_kind():
    msg = DummyMessage(b"{}", {envelope.PROP_KIND: "data"})
    parsed, err = envelope.try_parse_meta(msg)
    assert parsed is None
    assert "expected 'meta'" in err


def test_parse_end_ok():
    end = envelope.EndPayload(kind=envelope.KIND_END, transfer_id="abc", sha256="ff")
    msg = DummyMessage(
        end.to_json().encode("utf-8"),
        {envelope.PROP_KIND: "end", envelope.PROP_ID: "abc"},
    )
    parsed, err = envelope.try_parse_end(msg)
    assert err == ""
    assert parsed is not None
    assert parsed.sha256 == "ff"
