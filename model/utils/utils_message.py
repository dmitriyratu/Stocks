# # Import

import json
from typing import List, Dict, Tuple
from transformers import GPT2TokenizerFast
from config import parameters

import dataclass.data_structures as ds

tokenizer = GPT2TokenizerFast.from_pretrained("gpt2")

# # Message Creation Center


class BatchMessageCreator:
    """Handles batch creation and formatting of GPT messages for Bitcoin article analysis."""

    SYSTEM_PROMPT = (
        "You are a financial sentiment analyst. Your role is to analyze crypto-related articles "
        "and provide structured assessments of their potential impact on Bitcoin prices. "
        "Analyze each article separately and provide structured results."
    )

        
    @staticmethod
    def create_analysis_requirements() -> str:
       """Creates the analysis requirements section of the prompt."""
       MAX_SENTENCES = 5
       
       return f"""
       1. Categorical Analysis:
           - Emotion Category: One of: {', '.join(item.value for item in ds.EmotionCategory)}
           - Event Categories: A list of: {', '.join(item.value for item in ds.EventCategory)}
           - Expected Price Direction: One of: {', '.join(item.value for item in ds.PriceDirection)}
           - Timeframe of Impact: One of: {', '.join(item.value for item in ds.TimeFrame)}
       
       2. Continuous Analysis - all values between 0.0 (minimum) and 1.0 (maximum):
            - Positive, Negative and Neutral Sentiment Scores: in total they should add up to 1
            - Emotion Intensity: How strong is the emotional content
            - Market Alignment: How aligned is this with current market trends
            - Impact Magnitude: How significant is the potential impact
            - Trend Alignment Score: How well does this align with existing trends
            - Credibility Score: How credible is the information
            - Virality Score: How likely is this to spread virally
            - Event Relevance: How relevant is this event to Bitcoin
            - Confidence Score: How confident are you in this analysis
       
       3. Additional Information:
           - Key Topics: Main topics/entities discussed in the article
           - Free Text Summary: Summarize the main points and implications of the article ({MAX_SENTENCES} sentences max)
           - Explain Reasoning: Explain the reasoning behind the analysis and predictions ({MAX_SENTENCES} sentences max)
           - Historical Analogy: Provide a historical analogy where similar events influenced the market (if applicable)
       """

    @staticmethod
    def create_single_article_messages(article: Dict[str, str]) -> List[Dict[str, str]]:
        """Creates messages for GPT to process a single article."""
        user_content = f"""Analyze the following article: 
        
        News ID: {article['news_id']}
        Title: {article['title_text']}
        Article: {article['llm_ready_text']}
        
        Provide the following details:
        {BatchMessageCreator.create_analysis_requirements()}
        
        Respond in structured JSON format, without Markdown or code block formatting:
        {{
            "news_id": "{article['news_id']}",
            "emotion_category": "<{' | '.join(item.value for item in ds.EmotionCategory)}>",
            "event_category": ["<{' | '.join(item.value for item in ds.EventCategory)}>"],
            "price_direction_category": "<{' | '.join(item.value for item in ds.PriceDirection)}>",
            "timeframe_category": "<{' | '.join(item.value for item in ds.TimeFrame)}>",
            "positive": "<float between 0.0 and 1.0>",
            "negative": "<float between 0.0 and 1.0>",
            "neutral": "<float between 0.0 and 1.0>",
            "emotion_intensity": "<float between 0.0 and 1.0>",
            "market_alignment": "<float between 0.0 and 1.0>",
            "impact_magnitude": "<float between 0.0 and 1.0>",
            "trend_alignment_score": "<float between 0.0 and 1.0>",
            "credibility_score": "<float between 0.0 and 1.0>",
            "virality_score": "<float between 0.0 and 1.0>",
            "event_relevance": "<float between 0.0 and 1.0>",
            "confidence_score": "<float between 0.0 and 1.0>",
            "key_topics": ["<str>", "..."],
            "free_text_summary": "<str>",
            "historical_analogy": "<str | null>"
        }}
        """
        
        return [
            {"role": "system", "content": BatchMessageCreator.SYSTEM_PROMPT},
            {"role": "user", "content": user_content}
        ]
    


    @staticmethod
    def create_batch_requests(articles: List[Dict[str, str]]) -> Tuple[List[Dict], List[int]]:
        """Creates batch requests from multiple articles."""
        
        llm_params = parameters.LLM_PARAMS
        
        batch_requests, token_count_requests = [], []
        
        for article in articles:
            
            message = BatchMessageCreator.create_single_article_messages(article)
            message_token_count = len(tokenizer.encode(message, add_special_tokens=False, truncation = False, verbose=False))
            
            batch_request = {
                "custom_id": f"article_{article['news_id']}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": llm_params['model_name'],
                    "temperature": llm_params['temperature'],
                    "max_tokens": llm_params['max_tokens'],
                    "messages": message
                }
            }
            
            batch_requests.append(batch_request)
            token_count_requests.append(token_count_requests)
        
        return batch_requests, token_count_requests
        
    @staticmethod
    def parse_batch_response(response_json: str) -> List[Dict[str, ds.ArticleAnalysis]]:
        """
        Parses GPT response JSON into a list of ArticleAnalysis objects mapped to their IDs.
        """
        try:
            data = json.loads(response_json)

            results = []
            for item in data:
                categorical = ds.CategoricalFeatures(
                    emotion_category=ds.EmotionCategory(item["emotion_category"]),
                    event_category=[ds.EventCategory(event) for event in item["event_category"]],
                    timeframe_category=ds.TimeFrame(item["timeframe_category"]),
                    price_direction_category=ds.PriceDirection(item["price_direction_category"]),
                )

                continuous = ds.ContinuousFeatures(
                    positive=item["positive"],
                    negative=item["negative"],
                    neutral=item["neutral"],
                    emotion_intensity=item["emotion_intensity"],
                    market_alignment=item["market_alignment"],
                    impact_magnitude=item["impact_magnitude"],
                    trend_alignment_score=item["trend_alignment_score"],
                    credibility_score=item["credibility_score"],
                    virality_score=item["virality_score"],
                    event_relevance=item["event_relevance"],
                    confidence_score=item["confidence_score"],
                )

                results.append({
                    "news_id": item["news_id"],
                    "analysis": ds.ArticleAnalysis(
                        categorical=categorical,
                        continuous=continuous,
                        key_topics=item["key_topics"],
                        free_text_summary=item["summary"],
                        free_text_reasoning=item["reasoning"],
                        historical_analogy=item.get("historical_analogy"),
                    ),
                })

            return results

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise ValueError(f"Invalid response format: {str(e)}")
