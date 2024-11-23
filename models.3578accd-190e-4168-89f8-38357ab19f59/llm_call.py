# +
from dotenv import load_dotenv
import os
from openai import OpenAI

from model.utils.utils_gpt import MessageCreator, call_gpt
# -

# # Setup

load_dotenv() 


def call_gpt(client: OpenAI, message: list) -> dict:
    """Sends a message to ChatGPT and retrieves the JSON response."""
    try:
        
        # Make the API call
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=message,
        )
        
        # Extract the assistant's response
        response_content = completion['choices'][0]['message']['content']
        return json.loads(response_content)
    
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Error parsing GPT response: {e}")
        return {}


# +
class api_call

API_KEY = os.getenv("OPENAI_API_KEY")
STOCKS_PROJECT_ID = os.getenv("STOCKS_PROJECT_ID")

client = OpenAI(api_key = API_KEY, project = STOCKS_PROJECT_ID)
# -
# # Call API

# +
message = MessageCreator.create_message(
    title = title,
    summary = summary,
    subjects = subjects,
)

response = call_gpt(client, message)

analysis = MessageCreator.parse_response(gpt_response_json)
# -

"Bitcoin Hits New High"
"Major institutions and businesses are increasingly adopting Bitcoin for payments and investment."












