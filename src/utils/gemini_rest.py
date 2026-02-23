import os
import requests
import time

def generate_content(model: str, system_prompt: str, json_mode: bool = False, max_tokens: int = 1024, retries: int = 5) -> str:
    # API Version / Model standardization
    # Note: Gemini 1.5 Flash is often aliases to 2.5 Flash in some docs, 
    # but the API endpoint usually prefers 'gemini-1.5-flash'.
    if model == "gemini-2.5-flash" or model == "gemini-2.0-flash": 
        model = "gemini-1.5-flash-latest"
    
    # Use pro model if requested and not disabled
    if model.startswith("gemini-1.5-flash") and os.getenv("USE_PRO_TIER", "true").lower() == "true" and os.getenv("DISABLE_PRO_TIER", "false").lower() != "true":
        model = "gemini-1.5-pro-latest"
        
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY not set")
        
    url = f"https://generativelanguage.googleapis.com/v1/models/{model}:generateContent?key={api_key}"
    
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
        
    for attempt in range(retries):
        try:
            resp = requests.post(url, json=payload, timeout=60)
            
            # 429: Too Many Requests / Quota Exceeded
            if resp.status_code == 429:
                # Check if it's a quota exhaustion (RPD) vs rate limit (RPM)
                error_body = resp.json() if resp.text else {}
                msg = str(error_body.get('error', {}).get('message', '')).lower()
                
                if "quota" in msg or "daily" in msg:
                    # Daily quota exhausted, retrying won't help today
                    raise RuntimeError(f"GEMINI_QUOTA_EXHAUSTED: {msg}")
                
                if attempt < retries - 1:
                    time.sleep(30 * (2 ** attempt)) # Increased backoff for low RPM tiers
                    continue
            
            resp.raise_for_status()
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
            
        except requests.exceptions.RequestException as e:
            # Retry on 5xx errors
            if attempt < retries - 1 and getattr(e.response, 'status_code', 200) >= 500:
                time.sleep(15 * (2 ** attempt))
                continue
            raise e
        except (KeyError, IndexError) as e:
            if attempt < retries - 1:
                time.sleep(2)
                continue
            raise ValueError(f"Unexpected Gemini response format: {resp.json() if 'resp' in locals() else str(e)}")

def embed_content(model: str, content: str, title: str = None, task_type: str = "RETRIEVAL_QUERY", retries: int = 5) -> list[float]:
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
        
    for attempt in range(retries):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 429:
                if attempt < retries - 1:
                    time.sleep(15 * (2 ** attempt))
                    continue
            resp.raise_for_status()
            data = resp.json()
            return data["embedding"]["values"]
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1 and (getattr(e.response, 'status_code', 500) >= 500 or getattr(e.response, 'status_code', 200) == 429):
                time.sleep(15 * (2 ** attempt))
                continue
            raise e
