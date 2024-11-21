from dotenv import load_dotenv
import os
from openai import OpenAI

# ## Setup

load_dotenv() 

# ## Establish Connection

api_key = os.getenv("OPENAI_API_KEY")
stocks_project_id = 'proj_ojHPJZi9l17f1ejCfJkmoIBK'
client = OpenAI(api_key = api_key, project = stocks_project_id)

messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "How can I predict Bitcoin prices?"},
    ]

completion = client.chat.completions.create(
    model="gpt-4o",
    frequency_penalty = 1.0, # Penalizes repetitive text
    # function_call = , # Useful in applications where the model needs to perform tasks via API calls or defined functions,
    # function = , # Define functions that the model can use during its response generation
    # max_completion_tokens = , #max tokens in the response
    # presence_penalty = #Penalizes the model for introducing new topics. A value between -2.0 and 2.0.
    temperature = 0.3, # Controls the randomness of the output. Values range from 0.0 (deterministic) to 1.0 (highly random)
    top_p=0.9, # Use this for more focused or varied responses.
    messages = messages, 
)




print(completion.choices[0].message.content)


