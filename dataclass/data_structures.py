from dataclasses import dataclass
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field, constr, confloat

# # NEWS DATA STRUCTURES

# ## Enums for categorical features


# +
class EmotionCategory(Enum):
    OPTIMISTIC = "Optimistic"
    FEARFUL = "Fearful"
    CONFIDENT = "Confident"
    UNCERTAIN = "Uncertain"
    NEUTRAL = "Neutral"


class EventCategory(Enum):
    REGULATION = "Regulation"
    ADOPTION = "Adoption"
    SECURITY = "Security"
    TECHNOLOGY = "Technology"
    MACRO = "Macro"
    OTHER = "Other"

class PriceDirection(Enum):
    UP = "Upward"
    DOWN = "Downward"
    NEUTRAL = "Neutral"

class TimeFrame(Enum):
    IMMEDIATE = "within 24 hours"
    WITHIN_A_WEEK = "within a week"
    LONG_TERM = "long-term"


# -

class CategoricalFeatures(BaseModel):
    emotion_category: EmotionCategory = Field(
        description="The dominant emotional tone in the article (e.g., Optimistic for positive outlook, Fearful for concern about risks)"
    )
    event_category: List[EventCategory] = Field(
        description="Categories of events discussed (e.g., Regulation for policy changes, Adoption for market acceptance)"
    )
    timeframe_category: TimeFrame = Field(
        description="Expected timeframe for impact (24h, week, or long-term)"
    )
    price_direction_category: PriceDirection = Field(
        description="Expected direction of price movement (Upward, Downward, or Neutral)"
    )


# ## Continuous Features

class ContinuousFeatures(BaseModel):
    positive: confloat(ge=0.0, le=1.0) = Field(description="Positive sentiment score (positive + negative + neutral = 1)")
    negative: confloat(ge=0.0, le=1.0) = Field(description="Negative sentiment score (positive + negative + neutral = 1)")
    neutral: confloat(ge=0.0, le=1.0) = Field(description="Neutral sentiment score (positive + negative + neutral = 1)")
    emotion_intensity: confloat(ge=0.0, le=1.0) = Field(description="Intensity of the emotion (0: no emotion, 1: extremely emotional)")
    market_alignment: confloat(ge=0.0, le=1.0) = Field(description="How aligned with current market trends (0: opposed, 1: fully aligned)")
    impact_magnitude: confloat(ge=0.0, le=1.0) = Field(description="Magnitude of potential impact (0: no impact, 1: maximum impact)")
    trend_alignment_score: confloat(ge=0.0, le=1.0) = Field(description="Alignment with existing trends (0: misaligned, 1: fully aligned)")
    credibility_score: confloat(ge=0.0, le=1.0) = Field(description="Credibility of the information (0: not credible, 1: highly credible)")
    virality_score: confloat(ge=0.0, le=1.0) = Field(description="Likelihood of viral spread (0: unlikely, 1: highly viral)")
    event_relevance: confloat(ge=0.0, le=1.0) = Field(description="Relevance to Bitcoin price movement (0: irrelevant, 1: highly relevant)")
    confidence_score: confloat(ge=0.0, le=1.0) = Field(description="Model's confidence in analysis (0: not confident, 1: highly confident)")


# ## Free Text Features

class ArticleTextFields(BaseModel):
    key_topics: List[str] = Field(description="Main topics/entities discussed in the article")
    free_text_summary: str = Field(description="Summarize the main points and implications of the article")
    explain_reasoning_summary: str = Field(description="Explain the reasoning behind the analysis and predictions")
    historical_analogy: Optional[str] = Field(description="Provide a historical analogy where similar events influenced the market")



# ## Full Article Analysis

@dataclass
class ArticleAnalysis:
    categorical: CategoricalFeatures
    continuous: ContinuousFeatures
    text_content: ArticleTextFields


# # News Processing

@dataclass
class TimeoutConfig:
    """Centralized timeout configuration for scraping operations."""
    # General timeouts
    DEFAULT: int = 10
    CONNECTION: int = 10 
    READ: int = 12  
    
    # Selenium specific
    PAGE_LOAD: int = 30
    SCRIPT: int = 20 
    IMPLICIT_WAIT: int = 7  
    COMMAND: int = 15 
    
    # Cloudflare/verification specific
    VERIFICATION_SLEEP: int = 8
    VERIFICATION_WAIT: int = 15  
