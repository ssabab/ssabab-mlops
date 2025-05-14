import os
from dotenv import load_dotenv

def load_env(path="/opt/.env"):
    if os.path.exists(path):
        load_dotenv(dotenv_path=path, override=True)
    else:
        raise FileNotFoundError(f"{path} not found")
