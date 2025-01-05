import numpy as np
import tiktoken
from sentence_transformers import SentenceTransformer, util
from nltk.tokenize import sent_tokenize
from config import constants
import math
import pandas as pd


class TextSummarizer:

    def __init__(self):
        self.tokenizer = tiktoken.get_encoding("cl100k_base")
        self.model = SentenceTransformer('all-MiniLM-L6-v2')

    def text_summarize(self, text:str) -> str:
        """
        Summarizes text dynamically to meet max_tokens threshold.
        """

        if pd.isna(text):
            return
            
        token_count = len(self.tokenizer.encode(text))
        max_tokens = constants.MAXIMUM_ARTICLE_TOKENS
        
        if token_count < max_tokens:
            return text
            
        # Step 1: Split the text into sentences
        sentences = sent_tokenize(text)
        
        # Step 2: Generate embeddings and centrality scores
        sentence_embeddings = self.model.encode(sentences, batch_size=32)
        similarity_matrix = util.pytorch_cos_sim(sentence_embeddings, sentence_embeddings)
        centrality_scores = similarity_matrix.sum(dim=1).cpu().numpy()
        
        # Step 3: Rank sentences by centrality and keep top N
        ranked_indices = centrality_scores.argsort()[::-1]

        # Step 4: Accumulate sentences until breach token threshold 
        tally = 0
        for num_sentences_to_keep, rank_index in enumerate(ranked_indices):
            sentence = sentences[rank_index]
            tally += len(self.tokenizer.encode(sentence))
            if tally > max_tokens:
                break
            
        top_indices = ranked_indices[:num_sentences_to_keep]
        
        # Step 5: Reassemble the summary in original order
        top_sentences = [sentences[i] for i in sorted(top_indices)]
        summary_text = ' '.join(top_sentences)
        
        return summary_text
