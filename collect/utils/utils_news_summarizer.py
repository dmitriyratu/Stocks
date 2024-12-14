# +
device = torch_directml.device()
summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device=device)        


summary = self.summarizer(
    article.text,
    max_length=250,
    min_length=100,
    do_sample=False
)
article_details["description"] = summary[0]["summary_text"]


elif word_count > 25:
    article_details["description"] = article.text
else:
    article_details["description"] = None
    
return article_details
