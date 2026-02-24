import os
import requests
from dotenv import load_dotenv

def list_models():
    load_dotenv()
    load_dotenv('.env.local')
    api_key = os.environ.get('GEMINI_API_KEY')
    print(f"Key starts with: {api_key[:10] if api_key else 'NONE'}")
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    resp = requests.get(url)
    print(f"Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        for m in data.get('models', []):
            if 'generateContent' in m.get('supportedGenerationMethods', []):
                print(f"- {m['name']}")
    else:
        print(resp.text)

if __name__ == "__main__":
    list_models()
