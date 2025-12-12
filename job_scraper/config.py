# --- Job Scraper Configuration ---

# File Paths
MASTER_CSV = "master_jobs.csv"
DELTA_CSV = "delta_jobs.csv"

# Search Settings
# "limit for job as 20" - Sets the max results per search query.
MAX_RESULTS = 20

# Webhook Integration
WEBHOOK_URL = "http://localhost:5678/webhook/0be4c40f-5ca7-4388-aad5-02ad5a9728b4"

# Targeted Companies for Direct API Search
# Ashby is tricky because it's hard to scrape via search engines.
# We'll just hit their API directly for these known companies.
ASHBY_COMPANIES = [
    "pear", "deel", "cursor", "ramp", "notion", "linear", 
    "onebrief", "articul8", "nightfall-ai", "melotech"
]

