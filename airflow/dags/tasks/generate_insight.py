import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from utils.db import get_mysql_connection

MODEL_NAME = "google/gemma-2b-it"


def generate_insight_chunk(chunk_id):
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        device_map="auto",
        torch_dtype=torch.bfloat16
    )

    prompt = f"""
    """

    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    inputs.pop("token_type_ids", None)

    outputs = model.generate(
        **inputs,
        max_new_tokens=300,
        do_sample=True,
        top_k=50,
        top_p=0.95,
        temperature=0.7
    )
    result = tokenizer.decode(outputs[0], skip_special_tokens=True)

    return chunk_id, result
