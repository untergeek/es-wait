"""Unit tests for Task"""

import pytest
from es_wait._base import Waiter


def test_raise_on_empty(client):
    """Should raise a ValueError if you leave 'task_id' as None"""
    name = 'action'
    msg = f'Keyword arg {name} cannot be None'
    w = Waiter(client)
    w.action = None  # Setting this deliberately
    with pytest.raises(ValueError, match=msg):
        w.empty_check(name=name)


def test_check_is_false(client):
    """Should return False"""
    w = Waiter(client)
    assert not w.check
