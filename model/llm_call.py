# +
import json
import os
from typing import List

import dataclass.data_structures as ds
from dotenv import load_dotenv
from model.utils.utils_gpt import MessageCreator
from openai import OpenAI

# -

# # Setup

# +
load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
PROJECT_ID = os.getenv("STOCKS_PROJECT_ID")


# -


class LLMClient:

    def __init__(self, config: ds.LLMConfig):

        self.client = OpenAI(api_key=API_KEY, project=PROJECT_ID)
        self.config = config

    def _call_gpt(self, message: list) -> dict:
        """Sends a message to ChatGPT and retrieves the JSON response."""
        try:

            completion = self.client.chat.completions.create(
                model=self.config.model_name,
                messages=message,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                timeout=self.config.timeout_seconds,
            )

            choice = completion.choices[0]
            response_content = choice.message.content

            if not response_content:
                raise ValueError("Empty response from API")

            return response_content

        except (KeyError, json.JSONDecodeError) as e:
            raise KeyError(f"Invalid response format: {str(e)}")

    def analyze_article(self, title: str, summary: str, subjects: List[str]) -> ds.ArticleAnalysis:

        message = MessageCreator.create_message(
            title=title,
            summary=summary,
            subjects=subjects,
        )

        response = self._call_gpt(message)

        analysis = MessageCreator.parse_response(response)

        print(f"Successfully analyzed article: {title[:50]}...")

        return analysis


# +
config = ds.LLMConfig(
    model_name="gpt-4o",
    temperature=0.7,
    max_tokens=2000,
    timeout_seconds=30,
)

client_cls = LLMClient(config)
# -
