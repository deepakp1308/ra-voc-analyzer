"""Unit tests for Channel 1 parser (#mc-reporting-analytics-feedback)."""

import pytest

from voc_agent.ingestion.parser_feedback import parse_feedback_message


# Real message samples from live Slack exploration (2026-04-10)

SAMPLE_FEEDBACK_BADGE = (
    "*New Survey Response from the In-App Feedback Badge*\n\n"
    "*MRR:*  1440\n"
    "*Plan:* Premium plan \n"
    "*User ID:* 149472166\n"
    "*CSAT* Terrible\n"
    "*Current Page* <https://us7.admin.mailchimp.com/analytics/reports/custom-reports/builder?id=dcd7f097>\n"
    "*Feedback:* Your new reporting sections is awful. It requires so many clicks and selections. \n\n"
    "Not to mention that I now can't filter by a campaign name. We have to click and click. "
    "This is just not a good experience. \n\n"
    "*Fullstory:* <https://app.fullstory.com/ui/ZHBMT/client-session/test>\n"
    "_______________________________________"
)

SAMPLE_IN_APP_SURVEY = (
    "*New Survey Response from the In-App Survey*\n\n"
    "*MRR:* null\n"
    "*Plan:* Free\n"
    "*User ID:* 6193946\n"
    "*CSAT:* Good\n"
    "*Current Page* <https://us2.admin.mailchimp.com/email/editor?id=8611458&neaNuniMigrated=false"
    "|https://us2.admin.mailchimp.com/email/editor?id=8611458&neaNuniMigrated=false>\n"
    "*Feedback:* There were some loading issues yesterday and I needed to clean my data to get it to work.\n\n"
    "*Fullstory URL:* <https://app.fullstory.com/ui/ZHBMT/client-session/test2>\n"
    "---------------------------------------------------------------------"
)

SAMPLE_NO_CSAT = (
    "*New Survey Response from the In-App Feedback Badge*\n\n"
    "*MRR:*  null\n"
    "*Plan:* Pay As You Go plan \n"
    "*User ID:* 14432227\n"
    "*CSAT* \n"
    "*Current Page* <https://us6.admin.mailchimp.com/>\n"
    "*Feedback:* As the 'top locations by opens' functionality is being discontinued, "
    "please could you add a reporting feature to replace this?\n\n"
    "*Fullstory:* <https://app.fullstory.com/ui/ZHBMT/client-session/test3>\n"
    "_______________________________________"
)

SAMPLE_SPANISH_CSAT = (
    "*New Survey Response from the In-App Survey*\n\n"
    "*MRR:* null\n"
    "*Plan:* Free plan\n"
    "*User ID:* 221160682\n"
    "*CSAT:* Medianamente satisfecho\n"
    "*Current Page* <https://us10.admin.mailchimp.com/campaigns/>\n"
    "*Feedback:* reporte del mail confuso\n\n"
    "*Fullstory URL:* <https://app.fullstory.com/ui/ZHBMT/client-session/test4>\n"
    "---------------------------------------------------------------------"
)


class TestParseFeedbackBadge:
    def test_survey_type(self) -> None:
        result = parse_feedback_message(SAMPLE_FEEDBACK_BADGE)
        assert result["survey_type"] == "In-App Feedback Badge"

    def test_mrr(self) -> None:
        result = parse_feedback_message(SAMPLE_FEEDBACK_BADGE)
        assert result["mrr"] == 1440.0

    def test_plan(self) -> None:
        result = parse_feedback_message(SAMPLE_FEEDBACK_BADGE)
        assert result["plan"] == "Premium plan"

    def test_user_id(self) -> None:
        result = parse_feedback_message(SAMPLE_FEEDBACK_BADGE)
        assert result["user_id"] == "149472166"

    def test_csat_no_colon(self) -> None:
        result = parse_feedback_message(SAMPLE_FEEDBACK_BADGE)
        assert result["csat_raw"] == "Terrible"

    def test_page_url(self) -> None:
        result = parse_feedback_message(SAMPLE_FEEDBACK_BADGE)
        assert "custom-reports/builder" in result["page_url"]

    def test_feedback_text(self) -> None:
        result = parse_feedback_message(SAMPLE_FEEDBACK_BADGE)
        assert "reporting sections is awful" in result["feedback_text"]
        assert "campaign name" in result["feedback_text"]

    def test_fullstory_url(self) -> None:
        result = parse_feedback_message(SAMPLE_FEEDBACK_BADGE)
        assert result["fullstory_url"] is not None
        assert "fullstory.com" in result["fullstory_url"]


class TestParseInAppSurvey:
    def test_survey_type(self) -> None:
        result = parse_feedback_message(SAMPLE_IN_APP_SURVEY)
        assert result["survey_type"] == "In-App Survey"

    def test_null_mrr(self) -> None:
        result = parse_feedback_message(SAMPLE_IN_APP_SURVEY)
        assert result["mrr"] is None

    def test_free_plan(self) -> None:
        result = parse_feedback_message(SAMPLE_IN_APP_SURVEY)
        assert result["plan"] == "Free"

    def test_csat_with_colon(self) -> None:
        result = parse_feedback_message(SAMPLE_IN_APP_SURVEY)
        assert result["csat_raw"] == "Good"

    def test_fullstory_url_variant(self) -> None:
        """Tests the '*Fullstory URL:*' variant (note: 'URL' in the key)."""
        result = parse_feedback_message(SAMPLE_IN_APP_SURVEY)
        assert result["fullstory_url"] is not None


class TestParseEdgeCases:
    def test_empty_csat(self) -> None:
        result = parse_feedback_message(SAMPLE_NO_CSAT)
        assert result["csat_raw"] is None

    def test_null_mrr_pay_as_you_go(self) -> None:
        result = parse_feedback_message(SAMPLE_NO_CSAT)
        assert result["mrr"] is None
        assert result["plan"] == "Pay As You Go plan"

    def test_spanish_csat(self) -> None:
        result = parse_feedback_message(SAMPLE_SPANISH_CSAT)
        assert result["csat_raw"] == "Medianamente satisfecho"

    def test_empty_text(self) -> None:
        result = parse_feedback_message("")
        assert all(v is None for v in result.values())

    def test_none_text(self) -> None:
        result = parse_feedback_message("")
        assert result["survey_type"] is None

    def test_multiline_feedback(self) -> None:
        result = parse_feedback_message(SAMPLE_FEEDBACK_BADGE)
        # Should capture both paragraphs
        assert "clicks and selections" in result["feedback_text"]
        assert "good experience" in result["feedback_text"]
