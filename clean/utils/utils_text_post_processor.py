from cleantext import clean
from transformers import GPT2TokenizerFast
from textblob import TextBlob
import pandas as pd

from config import constants
from clean.utils.utils_spam_detector import SpamDetector


class TextProcessor:

    def __init__(self):
        
        self.tokenizer = GPT2TokenizerFast.from_pretrained("gpt2")
        self.spam_scorer = SpamDetector()

    def clean_text(self, text):

        if pd.isna(text):
            return None
            
        return clean(
            text,
            lower = False,
            fix_unicode=True, 
            to_ascii=True,
            normalize_whitespace=True,
            no_line_breaks=True,
            no_urls = True,
            no_phone_numbers = True,
        )


    def measure_text(self, text):
        
        if pd.isna(text):
            return None, None
                
        word_count = len(TextBlob(text).words)
        
        token_count = len(
            self.tokenizer.encode(
                text, 
                add_special_tokens=False, 
                truncation = False, 
                verbose=False
            )
        )

        return word_count, token_count

    def generate_curated_text(self, text, error):

        if pd.isna(text):
            return text, error, None

        spam_score = self.spam_scorer.get_score(text)
        word_count = len(TextBlob(text).words)
        
        if word_count < constants.MINIMUM_ARTICLE_WORDS:
            return None, 'text too short', None
        
        if spam_score > 0.25:
            return None, 'high spam score', spam_score

        return text, error, spam_score
