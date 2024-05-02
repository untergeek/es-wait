"""Unit tests for IndexLifecycle"""

import pytest
from elasticsearch8.exceptions import NotFoundError
from es_wait import IlmPhase, IlmStep
from es_wait.exceptions import IlmWaitError


class TestIlmPhase:
    """Test IlmPhase class"""

    def test_ilm_explain_idx_notfound(self, client, fake_notfound):
        """Should re-raise ``NotFoundError`` when NotFoundError is encountered"""
        client.ilm.explain_lifecycle.side_effect = fake_notfound
        ic = IlmPhase(client, name='arbitrary', phase='warm')
        with pytest.raises(NotFoundError):
            # pylint: disable=W0104
            ic.check

    def test_ilm_explain_exception(self, client, fake_fail):
        """Should raise ``IlmWaitError`` when Exception is encountered"""
        client.ilm.explain_lifecycle.side_effect = fake_fail
        ic = IlmPhase(client, name='arbitrary', phase='warm')
        with pytest.raises(IlmWaitError):
            # pylint: disable=W0104
            ic.check

    def test_ilm_phase_matches(self, ilmresponse, ilm_test):
        """Should result in True if phases match"""
        ilmresponse(phase='warm')
        assert bool(ilm_test(phase='warm', result=True))

    def test_ilm_phase_no_match(self, ilmresponse, ilm_test):
        """Should result in False if phases match"""
        ilmresponse(phase='warm')
        assert bool(ilm_test(phase='cold', result=False))


class TestIlmStep:
    """Test IlmStep class"""

    def test_ilm_explain_idx_notfound(self, client, fake_notfound):
        """
        Should re-raise ``NotFoundError`` when NotFoundError is encountered and index
        does not exist
        """
        client.ilm.explain_lifecycle.side_effect = fake_notfound
        client.indices.exists.return_value = False
        ic = IlmStep(client, name='arbitrary')
        with pytest.raises(NotFoundError):
            # pylint: disable=W0104
            ic.check

    def test_ilm_explain_idx_notfound_exists(self, client, fake_notfound):
        """
        Should return False when NotFoundError is encountered but index exists
        """
        client.ilm.explain_lifecycle.side_effect = fake_notfound
        client.indices.exists.return_value = True
        ic = IlmStep(client, name='arbitrary')
        assert not ic.check

    def test_ilm_step_complete(self, ilmresponse, ilm_test):
        """Should result in True if action and step are complete"""
        ilmresponse(action='complete', step='complete')
        assert bool(ilm_test(result=True))

    def test_ilm_step_incomplete(self, ilmresponse, ilm_test):
        """Should result in False if either action or step are not complete"""
        ilmresponse(action='complete', step='nope')
        assert bool(ilm_test(result=False))
