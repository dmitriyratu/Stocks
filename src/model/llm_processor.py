from transformers import GPT2TokenizerFast
from clean import clean_news_data

from src.core.storage.delta_lake import DeltaLakeManager, TableNames
from src.model.utils.llm_client import LLMClient
from src.model.utils.message_creator import BatchMessageCreator

for _, article in clean_news_data.news_data.iterrows():
    break

message = utils_message.BatchMessageCreator.create_single_article_messages(article)

print(message[-1]['content'])

llm_client = utils_call_llm.LLMClient()

llm_client.send_message_to_gpt(message)




