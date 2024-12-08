# # Import

# +
import json
from typing import Dict, List

import dataclass.data_structures as ds

# -

# # Message Creation Center


class MessageCreator:
    """Handles creation and formatting of GPT messages for Bitcoin article analysis."""

    SYSTEM_PROMPT = (
        "You are a financial sentiment analyst. Your role is to analyze crypto-related "
        "articles and provide a structured assessment of their potential impact on crypto prices "
        "with emphasis on shorter term."
    )

    @staticmethod
    def _format_enum_options(enum_class: type) -> str:
        """Formats enum options for prompt display."""
        return ", ".join(item.value for item in enum_class)

    @staticmethod
    def create_analysis_requirements() -> str:
        """Creates the analysis requirements section of the prompt."""

        sentiment_range = f"{-1.0} (very negative) and {1.0} (very positive)"
        emotions = MessageCreator._format_enum_options(ds.EmotionCategory)
        bull_bear = MessageCreator._format_enum_options(ds.BullishBearish)
        likelihood = MessageCreator._format_enum_options(ds.ImpactLikelihood)
        timeframe = MessageCreator._format_enum_options(ds.Timeframe)

        return f"""
        1. Sentiment Analysis:
            - Sentiment Score: A float value between {sentiment_range}.
            - Emotion Category: One of the following: {emotions}.
            - Bullish or Bearish: Indicate whether the text suggests: {bull_bear}.
        2. Impact Assessment:
            - Impact Likelihood: {likelihood} likelihood of this article affecting Bitcoin prices.
            - Timeframe of Impact: {timeframe}.
        3. Reasoning: No more than 3 sentence summary of explanation of your reasoning.
        """

    @staticmethod
    def _create_json_template() -> str:
        """Creates a JSON template based on the dataclass structure."""
        template_dict = {
            "sentiment_score": "<float>",
            "emotion_category": f"<{' | '.join(item.value for item in ds.EmotionCategory)}>",
            "bullish_bearish": f"<{' | '.join(item.value for item in ds.BullishBearish)}>",
            "impact_likelihood": f"<{' | '.join(item.value for item in ds.ImpactLikelihood)}>",
            "timeframe_of_impact": f"<{' | '.join(item.value for item in ds.Timeframe)}>",
            "llm_reasoning": "<str>",
        }
        return json.dumps(template_dict, indent=2)

    @classmethod
    def create_message(cls, title: str, summary: str, subjects: List[str]) -> List[Dict[str, str]]:
        """
        Creates a message for GPT to analyze Bitcoin-related articles.
        """

        user_content = f"""Analyze the following articles about {', '.join(subjects)}:
        Title: {title}
        Summary: {summary}
        
        Provide the following details:{cls._create_analysis_requirements()}
        
        Provide the response in the following structured JSON format,
        without any Markdown or code block formatting:

        {cls._create_json_template()}
        """

        return [
            {"role": "system", "content": cls.SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ]

    @staticmethod
    def parse_response(response_json: str) -> ds.ArticleAnalysis:
        """
        Parses the GPT response JSON into strongly-typed dataclasses.
        """
        try:
            data = json.loads(response_json)

            sentiment = ds.SentimentAnalysis(
                sentiment_score=float(data["sentiment_score"]),
                emotion_category=ds.EmotionCategory(data["emotion_category"]),
                bullish_bearish=ds.BullishBearish(data["bullish_bearish"]),
            )

            impact = ds.ImpactAssessment(
                impact_likelihood=ds.ImpactLikelihood(data["impact_likelihood"]),
                timeframe_of_impact=ds.Timeframe(data["timeframe_of_impact"]),
            )

            free_text = ds.FreeText(llm_reasoning=data["llm_reasoning"])

            return ds.ArticleAnalysis(sentiment=sentiment, impact=impact, free_text=free_text)

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise ValueError(f"Invalid response format: {str(e)}")
