"""Unit tests for Exists"""

import pytest
from es_wait import Exists


class TestExists:
    """Test Exists class"""

    @pytest.fixture(scope='class')
    def kinds(self):
        """What kinds of items"""
        kinds = ['index', 'data_stream', 'template', 'component']
        yield kinds

    def test_exists(self, client, existschk, kinds):
        """Should return ``True`` if exists"""
        for kind in kinds:
            existschk(kind, True)
            ec = Exists(client, kind=kind, name='arbitrary')
            assert ec.check

    def test_not_exists(self, client, existschk, kinds):
        """Should return ``False`` if not exists"""
        for kind in kinds:
            existschk(kind, False)
            ec = Exists(client, kind=kind, name='arbitrary')
            assert not ec.check

    def test_not_kind(self, client):
        """kindmap should return 'FAIL' if kind doesn't match"""
        ec = Exists(client, kind='NOPE', name='arbitrary')
        assert ec.kindmap == 'FAIL'
