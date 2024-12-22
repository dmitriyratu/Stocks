from transformers import pipeline, AutoTokenizer
import multiprocessing as mp
from typing import List, Tuple
import torch
from dataclasses import dataclass
from queue import Empty
import pyprojroot
import dataclass.data_structures as ds
from pathlib import Path
from logger_config import setup_logger
import cleantext
from dataclasses import dataclass

log_file = pyprojroot.here() / Path("logs/crypto_news.log")
logger = setup_logger("SummarizeNews", log_file)


# +
class SummarizerConstants:
    MODEL_NAME = "facebook/bart-large-cnn"
    CHUNK_SIZE = 1024
    DO_SAMPLE = False
    EARLY_STOPPING = True

@dataclass
class SummarizerConfig:
    model_name: str = SummarizerConstants.MODEL_NAME
    chunk_size: int = SummarizerConstants.CHUNK_SIZE
    max_length: int = 250
    min_length: int = 50
    num_beams: int = 4
    do_sample: bool = SummarizerConstants.DO_SAMPLE
    early_stopping: bool = SummarizerConstants.EARLY_STOPPING
    length_penalty: float = 1.0


# -

class TextSummarizer:

    def __init__(self):
        
        self.config = ds.SummarizerConfig
        self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        self.model = pipeline("summarization", model=self.config.MODEL_NAME, device=self.device)
        self.tokenizer = AutoTokenizer.from_pretrained(self.config.MODEL_NAME)

    def _clean_text(self, text: str) -> str:
        """Clean text using cleantext library."""
        return cleantext.clean(text, **self.config.CLEAN_PARAMS)

    def _chunk_text(self, text: str) -> List[str]:
        """Split text into chunks of specified token size."""
        tokens = self.tokenizer(text, return_tensors="pt", truncation=False).input_ids[0]
        chunks = [tokens[i:i + self.config.CHUNK_SIZE] for i in range(0, len(tokens), self.config.CHUNK_SIZE)]
        return [self.tokenizer.decode(chunk, skip_special_tokens=True) for chunk in chunks]
    
    def _process_chunk(self, text: str) -> str:
        """Process a single chunk of text."""
        return self.model(
            text,
            max_length=self.config.max_length,
            min_length=self.config.min_length,
            num_beams=self.config.num_beams,
            do_sample=self.config.DO_SAMPLE,
            early_stopping=self.config.EARLY_STOPPING,
            length_penalty=self.config.length_penalty
        )[0]['summary_text']
    
    def summarize(self, text: str) -> Tuple[str, str]:
        """Summarize text with cleaning and chunking."""
        clean_text = self._clean_text(text)
        chunks = self._chunk_text(clean_text)
        
        if len(chunks) == 1:
            return clean_text, self._process_chunk(chunks[0])
        
        # Two-phase summarization for longer texts
        chunk_summaries = [self._process_chunk(chunk) for chunk in chunks]
        final_summary = self._process_chunk(" ".join(chunk_summaries))
        return clean_text, final_summary
