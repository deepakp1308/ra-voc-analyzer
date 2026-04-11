"""Unit tests for Channel 2 parser (#hvc_feedback)."""

import pytest

from voc_agent.ingestion.parser_hvc import parse_hvc_message


SAMPLE_FEEDBACK_BADGE = (
    "*Response from Feedback Badge*\n\n"
    "*User ID:* 149472166  | *Premium plan* | *MRR:*  1440\n"
    "*Page URL:* <https://us7.admin.mailchimp.com/analytics/reports/custom-reports/builder?id=dcd7f097>\n\n"
    "*CSAT:* Terrible\n"
    "*Feedback:* Your new reporting sections is awful. It requires so many clicks and selections. \n\n"
    "Not to mention that I now can't filter by a campaign name. We have to click and click. "
    "This is just not a good experience. \n\n"
    "*<https://app.fullstory.com/ui/ZHBMT/client-session/test|FS Session Replay>*\n"
    "_______________________________________"
)

SAMPLE_CSAT_SURVEY = (
    "*Response from CSAT Survey*\n\n"
    "*User ID:* 4043598  | *Paid* | *MRR:* 361\n\n"
    "*CSAT:* Average\n"
    "*Feedback:* Formatting, bolding, and other style types can be cumbersome or impossible.\n\n"
    "<|*FS Session Replay*>\n"
    "____________________________________"
)

SAMPLE_PRS_SURVEY = (
    "*Response from the PRS Survey*\n\n"
    "*User ID:* 175352541  | *Premium plan* | *MRR:* 818\n\n"
    "*PRS:* 0\n"
    "*Reason:* Price\n"
    "*Feedback:* This sucks\n\n"
    "*<https://app.fullstory.com/ui/ZHBMT/client-session/test|FS Session Replay>*"
)

SAMPLE_PRS_WITH_HTML_REASON = (
    "*Response from the PRS Survey*\n\n"
    "*User ID:* 30872411  | *Standard plan* | *MRR:* 310\n\n"
    "*PRS:* 6\n"
    "*Reason:* <style type=\"text/css\"><!--td {border: 1px solid #cccccc;}--></style>\n"
    "Su facilidad de uso\n"
    "*Feedback:* Que haga mas economico el servircio\n\n"
    "*<https://app.fullstory.com/test|FS Session Replay>*"
)


class TestParseFeedbackBadge:
    def test_survey_type(self) -> None:
        result = parse_hvc_message(SAMPLE_FEEDBACK_BADGE)
        assert result["survey_type"] == "Feedback Badge"

    def test_compound_user_plan_mrr(self) -> None:
        result = parse_hvc_message(SAMPLE_FEEDBACK_BADGE)
        assert result["user_id"] == "149472166"
        assert result["plan"] == "Premium plan"
        assert result["mrr"] == 1440.0

    def test_csat(self) -> None:
        result = parse_hvc_message(SAMPLE_FEEDBACK_BADGE)
        assert result["csat_raw"] == "Terrible"

    def test_page_url(self) -> None:
        result = parse_hvc_message(SAMPLE_FEEDBACK_BADGE)
        assert "custom-reports/builder" in result["page_url"]

    def test_feedback_text(self) -> None:
        result = parse_hvc_message(SAMPLE_FEEDBACK_BADGE)
        assert "reporting sections is awful" in result["feedback_text"]

    def test_fullstory_url(self) -> None:
        result = parse_hvc_message(SAMPLE_FEEDBACK_BADGE)
        assert "fullstory.com" in result["fullstory_url"]


class TestParseCsatSurvey:
    def test_survey_type(self) -> None:
        result = parse_hvc_message(SAMPLE_CSAT_SURVEY)
        assert result["survey_type"] == "CSAT Survey"

    def test_paid_plan(self) -> None:
        result = parse_hvc_message(SAMPLE_CSAT_SURVEY)
        assert result["plan"] == "Paid"
        assert result["mrr"] == 361.0

    def test_average_csat(self) -> None:
        result = parse_hvc_message(SAMPLE_CSAT_SURVEY)
        assert result["csat_raw"] == "Average"

    def test_no_prs(self) -> None:
        result = parse_hvc_message(SAMPLE_CSAT_SURVEY)
        assert result["prs_score"] is None
        assert result["prs_reason"] is None


class TestParsePrsSurvey:
    def test_survey_type(self) -> None:
        result = parse_hvc_message(SAMPLE_PRS_SURVEY)
        assert result["survey_type"] == "PRS Survey"

    def test_prs_score(self) -> None:
        result = parse_hvc_message(SAMPLE_PRS_SURVEY)
        assert result["prs_score"] == 0

    def test_prs_reason(self) -> None:
        result = parse_hvc_message(SAMPLE_PRS_SURVEY)
        assert result["prs_reason"] == "Price"

    def test_feedback(self) -> None:
        result = parse_hvc_message(SAMPLE_PRS_SURVEY)
        assert result["feedback_text"] == "This sucks"

    def test_no_csat_in_prs(self) -> None:
        result = parse_hvc_message(SAMPLE_PRS_SURVEY)
        assert result["csat_raw"] is None

    def test_html_reason_filtered(self) -> None:
        """PRS reason sometimes has HTML style tags — should be filtered."""
        result = parse_hvc_message(SAMPLE_PRS_WITH_HTML_REASON)
        assert result["prs_reason"] is None or "<style" not in result["prs_reason"]


class TestParseEdgeCases:
    def test_empty_text(self) -> None:
        result = parse_hvc_message("")
        assert all(v is None for v in result.values())

    def test_prs_score_boundary(self) -> None:
        result = parse_hvc_message(SAMPLE_PRS_SURVEY)
        assert 0 <= result["prs_score"] <= 10
