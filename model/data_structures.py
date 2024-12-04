from enum import Enum
from dataclasses import dataclass
from typing import List, Optional
from pydantic import BaseModel, Field, ValidationError, model_validator
from textblob import TextBlob


# # News Data Structures

# ## Article Validator

class Article(BaseModel):
    title: str = Field(..., description="Title of the article")
    summary: str = Field(..., description="Summary of the article")

    @model_validator(mode="before")
    @classmethod
    def validate_word_counts(cls, values):
        """Validates word counts for title and summary."""
        title = values.get("title", "")
        summary = values.get("summary", "")

        def word_count(text):
            return len(TextBlob(text).words)

        if word_count(title) < 3:
            raise ValueError(f"Title must have at least 2 words. Given: '{title}'")

        if word_count(summary) < 20:
            raise ValueError(f"Summary must have at least 15 words. Given: '{summary}'")

        return values


# ## Article Output Categories

# +
class EmotionCategory(Enum):
    OPTIMISTIC = "optimistic"
    FEARFUL = "fearful"
    CONFIDENT = "confident"
    UNCERTAIN = "uncertain"
    NEUTRAL = "neutral"

class BullishBearish(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"

class ImpactLikelihood(Enum):
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"

class Timeframe(Enum):
    IMMEDIATE = "immediate"
    WITHIN_A_WEEK = "within a week"
    LONG_TERM = "long-term"


# +
@dataclass
class SentimentAnalysis:
    sentiment_score: float 
    emotion_category: EmotionCategory 
    bullish_bearish: BullishBearish 

@dataclass
class ImpactAssessment:
    impact_likelihood: ImpactLikelihood 
    timeframe_of_impact: Timeframe

@dataclass
class FreeText:
    llm_reasoning: str 

@dataclass
class ArticleAnalysis:
    sentiment: SentimentAnalysis
    impact: ImpactAssessment
    free_text: FreeText


# -
# # LLM Data Structure

@dataclass
class LLMConfig:
    model_name: str
    temperature: float
    max_tokens: Optional[int]
    timeout_seconds: int

