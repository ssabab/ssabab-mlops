import os
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from common.env_loader import load_env

load_env()

MODEL_NAME = "google/gemma-2b-it"

def load_llm_model():
    tokenizer = AutoTokenizer.from_pretrained(
        MODEL_NAME,
        token=os.getenv("HUGGING_FACE_TOKEN")
    )
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        device_map="auto",
        torch_dtype=torch.bfloat16,
        token=os.getenv("HUGGING_FACE_TOKEN")
    )

    print("CUDA available:", torch.cuda.is_available())
    print("Current device:", torch.cuda.current_device() if torch.cuda.is_available() else "CPU")
    print("Device name:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "N/A")

    return tokenizer, model