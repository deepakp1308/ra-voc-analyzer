"""Unit tests for Channel 3 parser (#mc-hvc-escalations)."""

import pytest

from voc_agent.ingestion.parser_escalation import parse_escalation_message


SAMPLE_FEEDBACK_INTAKE = (
    ":postal_horn: *New HVC Product Feedback Received* :postal_horn:\n\n"
    "*Customer Name*\nWorld Central Kitchen\n"
    "*Source*\nCustomer Success - Strategic\n"
    "*Submitter*\n<@U039NUU82EL|Rachel Benner>\n"
    "*Impacted Product*\nAnalytics\n"
    "*Goal: what is the user trying to accomplish?* \n"
    "Number discrepancy between Custom Reports & Campaign View\n\n"
    "Goal: Have a single, reliable source of truth for campaign performance metrics.\n"
    "*Constraints: what constraints is the user facing?*\n"
    "The numbers in C1's custom reports don't match what she sees when she clicks "
    "into the individual campaign and views the report there.\n"
    "*Workaround details*\n"
    "C1 has to manually compare both views and try to reconcile the numbers herself.\n"
    "*Supportive materials*\n\n"
    "*Criticality (if specific customer request)*\n"
    "P0 (Immediate) \u2013 High churn risk due to this item\n"
    "*Customer UID*\n7165809\n"
    "*MRR*\n6664"
)

SAMPLE_PRODUCT_HELP = (
    ":successtse:\n"
    "*<https://us20.admin.mailchimp.com/peaches2/users/account?user_id=77161842|Customer UID>*: 77161842\n"
    "*SF Case*: NA\n"
    "*Customer Name*: HC Brands\n"
    "*MRR*: 3488\n"
    "*Requestor\u2019s Team*: Customer Success - Strategic\n"
    "*Topic*: Analytics & Reporting\n"
    "*Criticality*: P1 (High) - Significant pain, potential churn\n"
    "Custom Reports is currently pulling all-time revenue for certain automations, "
    "causing a spike in reported revenue. For HC Brands, this means their automation "
    "revenue is being inaccurately aggregated on March 3."
)


class TestParseFeedbackIntake:
    def test_customer_name(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert result["customer_name"] == "World Central Kitchen"

    def test_source_team(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert result["source_team"] == "Customer Success - Strategic"

    def test_submitter(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert result["submitter"] == "Rachel Benner"

    def test_impacted_product(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert result["impacted_product"] == "Analytics"

    def test_goal(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert "discrepancy" in result["goal"]

    def test_constraints(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert "don't match" in result["constraints"]

    def test_feedback_text_combined(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert result["feedback_text"] is not None
        assert len(result["feedback_text"]) > 10

    def test_criticality(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert "P0" in result["criticality"]

    def test_customer_uid(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert result["user_id"] == "7165809"

    def test_mrr(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert result["mrr"] == 6664.0

    def test_survey_type(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert result["survey_type"] == "Escalation"


class TestParseProductHelp:
    def test_customer_name(self) -> None:
        result = parse_escalation_message(SAMPLE_PRODUCT_HELP)
        assert result["customer_name"] == "HC Brands"

    def test_mrr(self) -> None:
        result = parse_escalation_message(SAMPLE_PRODUCT_HELP)
        assert result["mrr"] == 3488.0

    def test_source_team(self) -> None:
        result = parse_escalation_message(SAMPLE_PRODUCT_HELP)
        assert "Customer Success" in result["source_team"]

    def test_impacted_product(self) -> None:
        result = parse_escalation_message(SAMPLE_PRODUCT_HELP)
        assert "Analytics" in result["impacted_product"]

    def test_criticality(self) -> None:
        result = parse_escalation_message(SAMPLE_PRODUCT_HELP)
        assert "P1" in result["criticality"]

    def test_feedback_text(self) -> None:
        result = parse_escalation_message(SAMPLE_PRODUCT_HELP)
        assert result["feedback_text"] is not None
        assert "revenue" in result["feedback_text"]


class TestParseEdgeCases:
    def test_empty_text(self) -> None:
        result = parse_escalation_message("")
        assert result["customer_name"] is None
        assert result["mrr"] is None

    def test_survey_type_always_escalation(self) -> None:
        result = parse_escalation_message(SAMPLE_FEEDBACK_INTAKE)
        assert result["survey_type"] == "Escalation"
