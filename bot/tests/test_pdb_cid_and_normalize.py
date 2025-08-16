from bot.pdb_cid import cid_from_object, is_valid_cid, canonical_json_bytes
from bot.pdb_normalize import normalize_profile


def test_cid_determinism_and_validity():
    obj1 = {"b": 2, "a": 1}
    obj2 = {"a": 1, "b": 2}  # different order, same content
    cid1 = cid_from_object(obj1)
    cid2 = cid_from_object(obj2)
    assert cid1 == cid2
    assert is_valid_cid(cid1)
    # canonical bytes must be stable and sorted
    b1 = canonical_json_bytes(obj1)
    b2 = canonical_json_bytes(obj2)
    assert b1 == b2


def test_normalize_profile_fields():
    src = {
        "title": "Dr. Watson",
        "bio": "Loyal companion",
        "mbti_type": "ISFJ",
        "attributes": {"socionics_type": "ESI", "big_five": "SCOEI"},
    }
    norm = normalize_profile(src)
    assert norm["name"] == "Dr. Watson"
    assert norm["description"] == "Loyal companion"
    assert norm["mbti"] == "ISFJ"
    assert norm["socionics"] == "ESI"
    assert norm["big5"] == "SCOEI"
