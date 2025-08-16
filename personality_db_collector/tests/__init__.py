"""Tests for the personality database collector."""

import pytest
import tempfile
import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime
import json

from personality_db_collector import (
    PersonalityDBClient,
    ParquetStorage,
    VectorSearchEngine,
    PersonData,
    RatingData,
    ProfileData,
    VectorData,
    to_dict,
    from_dict
)