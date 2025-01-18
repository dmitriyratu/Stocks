import emoji
import re
import pandas as pd
from nltk.corpus import stopwords
from functools import lru_cache
import string


class SpamDetector:
    """Thread-safe utility for calculating spam scores from text."""
    
    PROMO_WORDS = frozenset({
        'bonus', 'free', 'offer', 'deposit', 'win', 'prize', 'reward',
        'exclusive', 'limited', 'special', 'click', 'subscribe',
        'guaranteed', 'instant', 'sale', 'game', '100%', 'code',
        'new', 'lucky', 'winner'
    })
    EMOJI_SET = frozenset(emoji.EMOJI_DATA.keys())
    STOP_WORDS = frozenset(stopwords.words('english'))

    WORD_PATTERN = re.compile(r'\b\w+\b')
    EXCLAMATION_PATTERN = re.compile(r'!{2,}')

    TRANS_TABLE = str.maketrans('', '', string.punctuation.replace('!', ''))

    def __init__(self):
        """Initialize the detector with cache size."""
        self.get_score = lru_cache(maxsize=1024)(self._get_score)

    def _get_score(self, text: str) -> float:

        if pd.isna(text):
            return None

        # Convert to lowercase once
        text_lower = text.lower()

        # Word tokenization and stop word removal
        words = [
            word for word in self.WORD_PATTERN.findall(text_lower.translate(self.TRANS_TABLE))
            if word not in self.STOP_WORDS
        ]
        total_words = len(words) or 1

        # Count promotional words
        promo_count = sum(word in self.PROMO_WORDS for word in words)
        
        # Count emojis
        emoji_count = sum(char in self.EMOJI_SET for char in text)
        
        scores = {
            'emoji': min(emoji_count / (total_words * 0.1), 1.0),
            'promo': min(promo_count / (total_words * 0.25), 1.0),
            'exclamations': min(len(self.EXCLAMATION_PATTERN.findall(text)) / (total_words * 0.05), 1.0)
        }
        
        return min(sum(scores.values()) / 2, 1)
