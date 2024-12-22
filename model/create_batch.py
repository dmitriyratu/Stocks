from transformers import GPT2TokenizerFast
from clean import clean_news_data

from model.utils import utils_call_llm, utils_message

for _, article in clean_news_data.news_data.iterrows():
    break

message = utils_message.BatchMessageCreator.create_single_article_messages(article)

print(message[-1]['content'])

llm_client = utils_call_llm.LLMClient()

llm_client.send_message_to_gpt(message)




