import time

from google import genai
from google.genai import types

from config import GEMINI_API_KEY, GEMINI_MODEL, SAFETY_OFF

gemini_client = genai.Client(api_key = GEMINI_API_KEY)


def call_gemini(
    prompt: str,
    max_tokens: int,
    retries: int = 3,
    temperature: float | None = None,
    response_mime_type: str | None = None,
) -> str:
    config_kwargs = {
        "max_output_tokens": max_tokens,
        "safety_settings": SAFETY_OFF,
    }

    if temperature is not None:
        config_kwargs["temperature"] = temperature

    if response_mime_type is not None:
        config_kwargs["response_mime_type"] = response_mime_type

    for attempt in range(retries):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(**config_kwargs),
            )

            if not resp.candidates:
                print("    ⚠️  Gemini: no candidates returned")
                continue
            
            text = resp.text or""
            if not text.strip():
                print("    ⚠️  Gemini: empty text returned")
                continue
            
            return resp.text.strip()

        except Exception as e:
            print(f"    ⚠️  Gemini attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(6 * (attempt + 1))

    return ""                
