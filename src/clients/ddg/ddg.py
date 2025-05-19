import requests
import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

from duckduckgo_search import DDGS

#This library requires the use of a proxy service so I might switch to either setting up a webcrawler
#using scrapy or selenium and using the URLs from yfinance
#but there is another option of using the Bing API with the free tier through azure