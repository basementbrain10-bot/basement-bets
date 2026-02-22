import os
import requests

def generate_content(model: str, system_prompt: str, json_mode: bool = False, max_tokens: int = 1024) -> str:
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY not set")
        
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    
    payload = {
        "contents": [{
            "parts": [{"text": system_prompt}]
        }],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": max_tokens
        }
    }
    
    if json_mode:
        payload["generationConfig"]["responseMimeType"] = "application/json"
        
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    
    data = resp.json()
    try:
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        raise ValueError(f"Unexpected Gemini response format: {data}")

def embed_content(model: str, content: str, title: str = None, task_type: str = "RETRIEVAL_QUERY") -> list[float]:
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY not set")
        
    url = f"https://generativelanguage.googleapis.com/v1beta/{model}:embedContent?key={api_key}"
    
    payload = {
        "model": model,
        "content": {"parts": [{"text": content}]},
        "taskType": task_type
    }
    
    if title:
        payload["title"] = title
        
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    
    data = resp.json()
    return data["embedding"]["values"]
