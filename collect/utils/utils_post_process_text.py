from cleantext import clean


class TextProcessor:

    def clean_text(self, text):
    
        cleaned_text = clean(
            text,
            lower = False,
            fix_unicode=True, 
            to_ascii=True,
            normalize_whitespace=True,
            no_line_breaks=True,
            no_urls = True,
            no_phone_numbers = True,
        )
    
        return cleaned_text


class SpamDetector:
    """Thread-safe utility for calculating spam scores from text."""
    
    PROMO_WORDS = frozenset({
        'bonus', 'free', 'offer', 'deposit', 'win', 'prize', 'reward',
        'exclusive', 'limited', 'special', 'click', 'subscribe',
        'guaranteed', 'instant', 'sale', 'game', '100%', 'code',
        'new', 'lucky', 'winner'
    })
    EMOJI_SET = frozenset(emoji.EMOJI_DATA.keys())
    EXCLAMATION_PATTERN = re.compile(r'!{2,}')
    STOP_WORDS = frozenset(stopwords.words('english'))
    
    @staticmethod
    def load_model():
        global nlp
        if 'nlp' not in globals():
            nlp = spacy.load("en_core_web_sm")

    @staticmethod
    def get_score(text: str) -> float:
        
        SpamDetector.load_model()

        doc = nlp(text)
        words = [
            token.lemma_.lower()
            for token in doc if token.text.lower() not in SpamDetector.STOP_WORDS
        ]
        total_words = len(words) or 1
        emoji_count = len(set(text) & SpamDetector.EMOJI_SET)
        promo_count = sum(word in SpamDetector.PROMO_WORDS for word in words)

        scores = {
            'emoji': min(emoji_count / (total_words * 0.1), 1.0),
            'promo': min(promo_count / (total_words * 0.25), 1.0),
            'exclamations': min(len(SpamDetector.EXCLAMATION_PATTERN.findall(text)) / 3, 1.0)
        }
        return min(sum(scores.values()) / 2, 1)
