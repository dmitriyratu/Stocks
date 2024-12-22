TEXT_CLEANING_PARAMS = {
    "fix_unicode": True,
    "to_ascii": True,
    "lower": False,
    "no_line_breaks": True,
    "no_urls": True,
    "no_emails": True,
    "no_phone_numbers": True,
    "no_numbers": False,
    "no_punct": False,
    "lang": "en"
}

LLM_PARAMS = {
    'model_name':"gpt-4o",
    'temperature':0.4,
    'max_tokens':3000,
    'timeout_seconds':30,
}
