import numpy as np
from transformers import GPT2TokenizerFast
from sentence_transformers import SentenceTransformer, util
from nltk.tokenize import sent_tokenize
from config import constants
import math

model = SentenceTransformer('all-MiniLM-L6-v2')


def summarize(text:str, token_count:int) -> str:
    """
    Summarizes text dynamically to meet max_tokens threshold.
    """

    max_tokens = constants.MAXIMUM_ARTICLE_TOKENS
    
    # Step 1: Compute dynamic compression ratio
    compression_ratio = max_tokens / token_count 
    if compression_ratio < 1:
    
        # Step 2: Split the text into sentences
        sentences = sent_tokenize(text)
        num_sentences_to_keep = max(1, math.ceil(len(sentences) * compression_ratio))
        
        # Step 3: Generate embeddings and centrality scores
        sentence_embeddings = model.encode(sentences)
        similarity_matrix = util.pytorch_cos_sim(sentence_embeddings, sentence_embeddings)
        centrality_scores = similarity_matrix.sum(dim=1).cpu().numpy()
        
        # Step 4: Rank sentences by centrality and keep top N
        ranked_indices = centrality_scores.argsort()[::-1]
        top_indices = ranked_indices[:num_sentences_to_keep]
        
        # Step 5: Reassemble the summary in original order
        top_sentences = [sentences[i] for i in sorted(top_indices)]
        summary_text = ' '.join(top_sentences)
        
        return summary_text
        
    return text
