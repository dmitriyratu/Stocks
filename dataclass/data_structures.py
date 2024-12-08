from dataclasses import dataclass
from enum import Enum
from typing import Optional

# # News Data Structures

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