import yaml
import requests
import pandas as pd
from typing import Dict, Any
from logger import get_logger

logger = get_logger(__name__)

class DataFetcher:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    