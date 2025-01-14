import numpy as np
import tiktoken
from sentence_transformers import SentenceTransformer, util
from nltk.tokenize import sent_tokenize
from config import constants
import math
import pandas as pd


class TextSummarizer:

    WINDOW_SIZE=3

    POSITION_WEIGHTS = {
        'intro': 0.20,
        'centrality': 0.75,
        'conclusion': 0.05,
    }

    INTRO_SENTENCES = 3
    CONCLUSION_SENTENCES = 2
    
    def __init__(self):
        self.tokenizer = tiktoken.get_encoding("cl100k_base")
        self.model = SentenceTransformer('all-MiniLM-L6-v2')

    def _get_position_scores(self, sentences) -> tuple[np.ndarray, np.ndarray]:
        """
        Calculate separate position-based scores for intro and conclusion sentences.
        Returns tuple of (intro_scores, conclusion_scores)
        """

        num_sentences = len(sentences)
        
        intro_scores = np.zeros(num_sentences)
        conclusion_scores = np.zeros(num_sentences)
        
        # Score introductory sentences
        if num_sentences > self.INTRO_SENTENCES:
            intro_weights = np.linspace(1.0, 0.5, self.INTRO_SENTENCES)
            intro_scores[:min(self.INTRO_SENTENCES, num_sentences)] = intro_weights[:min(self.INTRO_SENTENCES, num_sentences)]
        
        # Score concluding sentences
        if num_sentences >= self.CONCLUSION_SENTENCES + self.INTRO_SENTENCES:
            conclusion_weights = np.linspace(0.5, 1.0, self.CONCLUSION_SENTENCES)
            conclusion_scores[-self.CONCLUSION_SENTENCES:] = conclusion_weights
        
        return intro_scores, conclusion_scores
    
    def text_summarize(self, text:str) -> str:
        """
        Summarizes text dynamically to meet max_tokens threshold with position weighting.
        """

        if pd.isna(text):
            return None
            
        token_count = len(self.tokenizer.encode(text))
        max_tokens = constants.MAXIMUM_ARTICLE_TOKENS
        
        if token_count < max_tokens:
            return text
            
        # Step 1: Split the text into sentences
        sentences = sent_tokenize(text)

        if len(sentences) < self.WINDOW_SIZE:
            return text[:max_tokens*4]
        
        # Step 2: Generate embeddings and centrality scores
        centrality_scores = self._compute_hybrid_scores(sentences)

        # Step 3: Calculate position scores        
        intro_scores, conclusion_scores = self._get_position_scores(sentences)

        # Step 4: Combine scores with weights
        final_scores = (
            self.POSITION_WEIGHTS['centrality'] * centrality_scores +
            self.POSITION_WEIGHTS['intro'] * intro_scores +
            self.POSITION_WEIGHTS['conclusion'] * conclusion_scores
        )
        
        # Step 5: Rank sentences by combined score
        ranked_indices = final_scores.argsort()[::-1]

        # Step 6: Accumulate sentences until breach token threshold
        tally = 0
        for num_sentences_to_keep, rank_index in enumerate(ranked_indices):
            sentence = sentences[rank_index]
            tally += len(self.tokenizer.encode(sentence))
            if tally > max_tokens:
                break
            
        top_indices = ranked_indices[:num_sentences_to_keep]
        
        # Step 7: Reassemble the summary in original order
        top_sentences = [sentences[i] for i in sorted(top_indices)]
        summary_text = ' '.join(top_sentences)
        
        return summary_text

    def _compute_hybrid_scores(self, sentences):
        
        num_sentences = len(sentences)

        embeddings = self.model.encode(
            sentences,
            batch_size=num_sentences,
            convert_to_tensor=True,
            show_progress_bar=False
        )
        
        all_similarities = util.pytorch_cos_sim(embeddings, embeddings).cpu().numpy()

        # Calculate length weights for each sentence
        sentence_lengths = np.array([len(sent.split()) for sent in sentences])
        length_weights = np.minimum(sentence_lengths/np.quantile(sentence_lengths,0.9),1)

        # Apply weights directly to similarity matrix rows
        weighted_similarities = all_similarities * length_weights.reshape(-1, 1)
        
        # Global scores
        global_scores = np.median(weighted_similarities, axis = 1)
        
        # Vectorized local scores computation
        window_size = min(self.WINDOW_SIZE, num_sentences - 1)
        local_scores = np.array([
            np.concatenate([
                weighted_similarities[i, max(0, i - window_size):i],
                weighted_similarities[i, i + 1:min(i + window_size + 1, num_sentences)]
            ]).mean()
            for i in range(num_sentences)
        ])
        
        standardized_global_scores = (global_scores - global_scores.min()) / (global_scores.max() - global_scores.min() + 1e-8)
        standardized_local_scores = (local_scores - local_scores.min()) / (local_scores.max() - local_scores.min() + 1e-8)
        
        final_scores = 0.7 * standardized_global_scores + 0.3 * standardized_local_scores
        return final_scores
