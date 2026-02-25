import os
import requests
import time
from src.config import settings

def generate_content(model: str, system_prompt: str, json_mode: bool = False, max_tokens: int = 1024, retries: int = 5) -> str:
    api_key = settings.GEMINI_API_KEY
    if not api_key:
        error_msg = "GEMINI_API_KEY not set."
        if settings.is_prod or settings.is_preview:
            error_msg += " CRITICAL: Missing from Vercel Dashboard. Please add GEMINI_API_KEY to Environment Variables and REDEPLOY."
        raise ValueError(error_msg)

    # API Version / Model standardization
    # Standardize on 1.5 for better quota availability in free tier
    if model in ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-flash-latest"]:
        model = "gemini-1.5-flash"
    elif model in ["gemini-1.5-pro", "gemini-1.5-pro-latest", "gemini-2.5-pro"]:
        model = "gemini-1.5-pro"
    
    # Use generic aliases if needed
    if model == "gemini-1.5-flash": model = "gemini-flash-latest"
    if model == "gemini-1.5-pro": model = "gemini-pro-latest"
        
    model_path = model if model.startswith("models/") else f"models/{model}"
    url = f"https://generativelanguage.googleapis.com/v1beta/{model_path}:generateContent?key={api_key}"
    
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
                error_body = resp.json() if resp.text else {}
                msg = str(error_body.get('error', {}).get('message', '')).lower()
                if "quota" in msg or "daily" in msg:
                    # If daily quota exhausted, we can't do much
                    if "daily" in msg:
                        raise RuntimeError(f"GEMINI_QUOTA_EXHAUSTED: {msg}")
                    # If just rate limit, wait
                    time.sleep(15 * (2 ** attempt))
                    continue
                
                if attempt < retries - 1:
                    time.sleep(30 * (2 ** attempt))
                    continue
            
            if resp.status_code >= 400:
                print(f"[Gemini API Error] {resp.status_code} - {resp.text}")
            
            resp.raise_for_status()
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
            
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                # Retry on most errors with backoff
                time.sleep(10 * (2 ** attempt))
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
        
    # Standardize embedding model
    if "embedding" not in model:
        model = "text-embedding-004"
        
    model_path = model if model.startswith("models/") else f"models/{model}"
    url = f"https://generativelanguage.googleapis.com/v1beta/{model_path}:embedContent?key={api_key}"
    
    payload = {
        "model": model_path,
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
