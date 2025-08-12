from types import SimpleNamespace

from bot.utils import has_admin_access

class _Perms(SimpleNamespace):
    pass

class _Role(SimpleNamespace):
    pass

class _Member(SimpleNamespace):
    pass

def _member(role_ids=None, manage=True):
    role_objs = [ _Role(id=r) for r in (role_ids or []) ]
    return _Member(roles=role_objs, guild_permissions=_Perms(manage_messages=manage))


def test_has_admin_access_with_roles():
    admin_roles = {10, 20}
    m = _member(role_ids=[5, 20])
    assert has_admin_access(m, admin_roles)


def test_has_admin_access_without_required_roles():
    admin_roles = {10, 20}
    m = _member(role_ids=[5])
    assert not has_admin_access(m, admin_roles)


def test_has_admin_access_fallback_manage_messages():
    admin_roles = set()
    m = _member(role_ids=[5], manage=True)
    assert has_admin_access(m, admin_roles)
    m2 = _member(role_ids=[5], manage=False)
    assert not has_admin_access(m2, admin_roles)
