import pytest

from app import store


def test_prior_statuses_for_in_progress():
    assert store.prior_task_statuses("in_progress") == ["accepted"]


@pytest.mark.parametrize("terminal", ["done", "failed"])
def test_prior_statuses_for_terminal(terminal):
    assert store.prior_task_statuses(terminal) == ["accepted", "in_progress"]


def test_accepted_has_no_prior_statuses():
    assert store.prior_task_statuses("accepted") == []


def test_unknown_status_rejected():
    with pytest.raises(ValueError):
        store.prior_task_statuses("exploded")


def test_every_task_type_and_outcome_moves_the_tenant():
    for task_type in store.TASK_TYPES:
        for outcome in store.TASK_TERMINAL:
            assert (task_type, outcome) in store.TENANT_STATUS_AFTER_TASK


def test_tenant_transitions_match_spec():
    assert store.TENANT_STATUS_AFTER_TASK[("deploy", "done")] == "active"
    assert store.TENANT_STATUS_AFTER_TASK[("update", "done")] == "active"
    assert store.TENANT_STATUS_AFTER_TASK[("destroy", "done")] == "destroyed"
    for task_type in store.TASK_TYPES:
        assert store.TENANT_STATUS_AFTER_TASK[(task_type, "failed")] == "failed"
    assert {"active"} == store.PATCH_ALLOWED_FROM
    assert {"active", "failed"} == store.DELETE_ALLOWED_FROM
