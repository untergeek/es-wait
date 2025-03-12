"""Unit tests for Snapshot"""

from es_wait.snapshot import Snapshot


class TestSnapshot:
    """TestSnapshot

    Test Snapshot class
    """

    def test_fail_to_get_snapshot(self, fake_fail, snapbundle):
        """test_fail_to_get_snapshot

        Should raise ``ValueError`` when another upstream Exception occurs.
        """
        client, kwargs = snapbundle
        client.snapshot.get.side_effect = fake_fail
        sc = Snapshot(client, **kwargs)
        assert not sc.check()

    def test_in_progress(self, snap_resp, snapbundle, snapchk):
        """test_in_progress

        Should return ``False`` when state is ``IN_PROGRESS``.
        """
        client, kwargs = snapbundle
        snapchk(snap_resp(state='IN_PROGRESS'))
        sc = Snapshot(client, **kwargs)
        assert not sc.check()

    def test_success(self, snap_resp, snapbundle, snapchk):
        """test_success

        Should return ``True`` when state is ``SUCCESS``.
        """
        client, kwargs = snapbundle
        snapchk(snap_resp(state='SUCCESS'))
        sc = Snapshot(client, **kwargs)
        assert sc.check()

    def test_partial(self, snap_resp, snapbundle, snapchk):
        """test_partial

        Should return ``True`` when state is ``PARTIAL``.
        """
        client, kwargs = snapbundle
        snapchk(snap_resp(state='PARTIAL'))
        sc = Snapshot(client, **kwargs)
        assert sc.check()

    def test_failed(self, snap_resp, snapbundle, snapchk):
        """test_failed

        Should return ``True`` when state is ``FAILED``.
        """
        client, kwargs = snapbundle
        snapchk(snap_resp(state='FAILED'))
        sc = Snapshot(client, **kwargs)
        assert sc.check()

    def test_other(self, snap_resp, snapbundle, snapchk):
        """test_other

        Should return ``True`` when state is anything other than ``IN_PROGRESS`` or the
        above.
        """
        client, kwargs = snapbundle
        snapchk(snap_resp(state='OTHER'))
        sc = Snapshot(client, **kwargs)
        assert sc.check()
