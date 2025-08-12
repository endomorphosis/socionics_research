import time

from bot.utils import RateLimiter, parse_time_range

def test_rate_limiter_allows_within_limit():
    rl = RateLimiter(3)
    uid = 123
    assert rl.allow(uid)
    assert rl.allow(uid)
    assert rl.allow(uid)
    assert rl.allow(uid) is False  # 4th exceeds limit


def test_rate_limiter_recovers_after_time():
    rl = RateLimiter(2)
    uid = 5
    rl.allow(uid)
    rl.allow(uid)
    assert rl.allow(uid) is False
    # simulate passage
    for k in rl.calls:
        rl.calls[k].clear()
    assert rl.allow(uid)


def test_parse_time_range():
    start, end = parse_time_range("10m")
    assert end - start - 600 < 2  # allow small drift
