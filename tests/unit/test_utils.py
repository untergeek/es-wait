"""Unit tests for es_wait.utils module."""

# pylint: disable=W0621
import logging
import pytest
from elastic_transport import ObjectApiResponse, ApiResponseMeta
from es_wait.debug import debug
from es_wait.utils import (
    diagnosis_generator,
    impact_generator,
    indicator_generator,
    healthchk_result,
    health_report,
    loop_health_indicators,
)

META = ApiResponseMeta(200, '1.1', {}, 0.01, None)

# Sample data for testing generators
SAMPLE_DIAGNOSIS = [
    {"cause": "cause1", "action": "action1", "affected_resources": ["res1"]},
    {"cause": "cause2", "action": "action2", "affected_resources": ["res2"]},
]

SAMPLE_IMPACT = [
    {"severity": "high", "description": "desc1", "impact_areas": ["area1"]},
    {"severity": "low", "description": "desc2", "impact_areas": ["area2"]},
]

SAMPLE_INDICATOR = {
    "symptom": "symptom1",
    "details": "details1",
    "impacts": SAMPLE_IMPACT,
    "diagnosis": SAMPLE_DIAGNOSIS,
}


# Fixture to capture log output
@pytest.fixture
def caplog(caplog):
    """Capture log output."""
    caplog.set_level(logging.INFO)
    return caplog


# Test diagnosis_generator
def test_diagnosis_generator():
    """Test that diagnosis_generator yields correct strings."""
    ind = "test_ind"
    data = SAMPLE_DIAGNOSIS
    expected = [
        "INDICATOR: test_ind: DIAGNOSIS #0: CAUSE: cause1",
        "INDICATOR: test_ind: DIAGNOSIS #0: ACTION: action1",
        "INDICATOR: test_ind: DIAGNOSIS #0: AFFECTED_RESOURCES: ['res1']",
        "INDICATOR: test_ind: DIAGNOSIS #1: CAUSE: cause2",
        "INDICATOR: test_ind: DIAGNOSIS #1: ACTION: action2",
        "INDICATOR: test_ind: DIAGNOSIS #1: AFFECTED_RESOURCES: ['res2']",
    ]
    result = list(diagnosis_generator(ind, data))
    assert result == expected


# Test impact_generator
def test_impact_generator():
    """Test that impact_generator yields correct strings."""
    ind = "test_ind"
    data = SAMPLE_IMPACT
    expected = [
        "INDICATOR: test_ind: IMPACT AREA #0: SEVERITY: high",
        "INDICATOR: test_ind: IMPACT AREA #0: DESCRIPTION: desc1",
        "INDICATOR: test_ind: IMPACT AREA #0: IMPACT_AREAS: ['area1']",
        "INDICATOR: test_ind: IMPACT AREA #1: SEVERITY: low",
        "INDICATOR: test_ind: IMPACT AREA #1: DESCRIPTION: desc2",
        "INDICATOR: test_ind: IMPACT AREA #1: IMPACT_AREAS: ['area2']",
    ]
    result = list(impact_generator(ind, data))
    assert result == expected


# Test indicator_generator
def test_indicator_generator():
    """Test that indicator_generator yields correct strings from all components."""
    ind = "test_ind"
    data = SAMPLE_INDICATOR
    expected = [
        "INDICATOR: test_ind: SYMPTOM: symptom1",
        "INDICATOR: test_ind: DETAILS: details1",
        "INDICATOR: test_ind: IMPACT AREA #0: SEVERITY: high",
        "INDICATOR: test_ind: IMPACT AREA #0: DESCRIPTION: desc1",
        "INDICATOR: test_ind: IMPACT AREA #0: IMPACT_AREAS: ['area1']",
        "INDICATOR: test_ind: IMPACT AREA #1: SEVERITY: low",
        "INDICATOR: test_ind: IMPACT AREA #1: DESCRIPTION: desc2",
        "INDICATOR: test_ind: IMPACT AREA #1: IMPACT_AREAS: ['area2']",
        "INDICATOR: test_ind: DIAGNOSIS #0: CAUSE: cause1",
        "INDICATOR: test_ind: DIAGNOSIS #0: ACTION: action1",
        "INDICATOR: test_ind: DIAGNOSIS #0: AFFECTED_RESOURCES: ['res1']",
        "INDICATOR: test_ind: DIAGNOSIS #1: CAUSE: cause2",
        "INDICATOR: test_ind: DIAGNOSIS #1: ACTION: action2",
        "INDICATOR: test_ind: DIAGNOSIS #1: AFFECTED_RESOURCES: ['res2']",
    ]
    result = list(indicator_generator(ind, data))
    assert result == expected


# Test healthchk_result with matching data
def test_healthchk_result_match():
    """Test healthchk_result returns True when all conditions match."""
    body = {"status": "green", "relocating_shards": 0}
    data = ObjectApiResponse(body, META)
    check_for = {"status": "green", "relocating_shards": 0}
    assert healthchk_result(data, check_for) is True


# Test healthchk_result with non-matching data
def test_healthchk_result_no_match():
    """Test healthchk_result returns False when conditions do not match."""
    body = {"status": "yellow", "relocating_shards": 1}
    data = ObjectApiResponse(body, META)
    check_for = {"status": "green", "relocating_shards": 0}
    assert healthchk_result(data, check_for) is False


# Test healthchk_result with missing key raises KeyError
def test_healthchk_result_missing_key():
    """Test healthchk_result raises KeyError when a key is missing."""
    body = {"status": "green"}
    data = ObjectApiResponse(body, META)
    check_for = {"status": "green", "relocating_shards": 0}
    with pytest.raises(
        KeyError, match='Key "relocating_shards" not in cluster health output'
    ):
        healthchk_result(data, check_for)


# Test health_report with green status (no logging)
def test_health_report_green(caplog):
    """Test health_report does not log when status is green."""
    body = {"status": "green", "indicators": {}}
    data = ObjectApiResponse(body, META)
    with debug.change_level(1):
        # Set debug level to 1 to avoid logging in the test
        health_report(data)
    assert len(caplog.records) == 0


# Test health_report with non-green status
def test_health_report_non_green(caplog):
    """Test health_report logs indicators when status is not green."""
    body = {
        "status": "red",
        "indicators": {
            "test_ind": {
                "status": "red",
                "symptom": "symptom1",
                "details": "details1",
                "impacts": SAMPLE_IMPACT,
                "diagnosis": SAMPLE_DIAGNOSIS,
            }
        },
    }
    data = ObjectApiResponse(body, META)
    health_report(data)
    assert "HEALTH REPORT: STATUS: RED" in caplog.text
    assert "INDICATOR: test_ind: SYMPTOM: symptom1" in caplog.text


# Test loop_health_indicators
def test_loop_health_indicators(caplog):
    """Test loop_health_indicators logs indicators correctly."""
    inds = {
        "test_ind": {
            "status": "red",
            "symptom": "symptom1",
            "details": "details1",
            "impacts": SAMPLE_IMPACT,
            "diagnosis": SAMPLE_DIAGNOSIS,
        }
    }
    loop_health_indicators(inds)
    assert "INDICATOR: test_ind: SYMPTOM: symptom1" in caplog.text


# Test health_report with missing keys
def test_health_report_missing_keys(caplog):
    """Test health_report handles missing keys gracefully."""
    body = {"status": "red"}
    data = ObjectApiResponse(body, META)
    health_report(data)
    assert "KeyError('indicators')" in caplog.text
