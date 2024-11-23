# # Import

# +
from typing import List, Dict
import json

import model.data_structures as ds


# -

# # Message Creation Center

class MessageCreator:
    """Handles creation and formatting of GPT messages for Bitcoin article analysis."""
    
    SYSTEM_PROMPT = "You are a financial sentiment analyst. Your role is to analyze crypto-related " \
    "articles and provide a structured assessment of their potential impact on crypto prices with emphasis on shorter term."

    @staticmethod
    def _format_enum_options(enum_class: type) -> str:
        """Formats enum options for prompt display."""
        return ", ".join(item.value for item in enum_class)

    @staticmethod
    def _create_analysis_requirements() -> str:
        """Creates the analysis requirements section of the prompt."""

        return f"""
        1. Sentiment Analysis:
           - Sentiment Score: A float value between {-1.0} (very negative) and {1.0} (very positive).
           - Emotion Category: One of the following: {MessageCreator._format_enum_options(ds.EmotionCategory)}.
           - Bullish or Bearish: Indicate whether the text suggests: {MessageCreator._format_enum_options(ds.BullishBearish)}.
        2. Impact Assessment:
           - Impact Likelihood: {MessageCreator._format_enum_options(ds.ImpactLikelihood)} likelihood of this article affecting Bitcoin prices.
           - Timeframe of Impact: {MessageCreator._format_enum_options(ds.Timeframe)}."""

    @staticmethod
    def _create_json_template() -> str:
        """Creates a JSON template based on the dataclass structure."""
        template_dict = {
            "sentiment_score": "<float>",
            "emotion_category": f"<{' | '.join(item.value for item in ds.EmotionCategory)}>",
            "bullish_bearish": f"<{' | '.join(item.value for item in ds.BullishBearish)}>",
            "impact_likelihood": f"<{' | '.join(item.value for item in ds.ImpactLikelihood)}>",
            "timeframe_of_impact": f"<{' | '.join(item.value for item in ds.Timeframe)}>"
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
        
        Provide the response in the following structured JSON format:
        {cls._create_json_template()}
        """

        return [
            {"role": "system", "content": cls.SYSTEM_PROMPT},
            {"role": "user", "content": user_content}
        ]

    @staticmethod
    def parse_response(response_json: str) -> ds.ArticleAnalysis:
        """
        Parses the GPT response JSON into strongly-typed dataclasses.
        """
        try:
            data = json.loads(response_json)
            
            sentiment = ds.SentimentAnalysis(
                sentiment_score=float(data['sentiment_score']),
                emotion_category=ds.EmotionCategory(data['emotion_category']),
                bullish_bearish=ds.BullishBearish(data['bullish_bearish'])
            )
            
            impact = ds.ImpactAssessment(
                impact_likelihood=ds.ImpactLikelihood(data['impact_likelihood']),
                timeframe_of_impact=ds.Timeframe(data['timeframe_of_impact'])
            )
            
            return ds.ArticleAnalysis(sentiment=sentiment, impact=impact)
            
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise ValueError(f"Invalid response format: {str(e)}")
