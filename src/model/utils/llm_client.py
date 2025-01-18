# +
import json
import os
from typing import List

from dotenv import load_dotenv
import openai
from config import parameters
# -

from src.core.schemas import data_structures as ds
from src.core.config import settings

# # Setup

# +
load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
PROJECT_ID = os.getenv("STOCKS_PROJECT_ID")


# -


class LLMClient:

    def __init__(self):

        self.client = openai.OpenAI(api_key=API_KEY, project=PROJECT_ID)
        self.config = parameters.LLM_PARAMS

    def send_message_to_gpt(self, message: list) -> dict:
        """Sends a message batch to ChatGPT and retrieves the JSON response."""
        try:

            completion = self.client.chat.completions.create(
                model=self.config['model_name'],
                messages=message,
                temperature=self.config['temperature'],
                max_tokens=self.config['max_tokens'],
                timeout=self.config['timeout_seconds'],
            )

            choice = completion.choices[0]
            response_content = choice.message.content

            if not response_content:
                raise ValueError("Empty response from API")

            return json.loads(response_content)

        except json.JSONDecodeError:
            raise ValueError("API response is not valid JSON")
        except openai.OpenAIError as e:
            raise RuntimeError(f"OpenAI API Error: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Unexpected Error: {str(e)}")
