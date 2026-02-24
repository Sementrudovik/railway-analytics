# import neccary libraries
import pandas as pd
from pyxlsb import open_workbook
import logging
from pathlib import Path

# set up logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 