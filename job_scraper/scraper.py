import os
import time
import re
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from googlesearch import search as google_search
from ddgs import DDGS
from urllib.parse import urlparse, urljoin
import config

# --- Config is now loaded from job_scraper/config.py ---

def get_search_results(query):
    """
    Hit DuckDuckGo first since it's lenient with rate limits. 
    If that fails, we'll try Google as a backup.
    """
    print(f"Searching for: {query}")
    results = []
    
    # Breaking this down into simple site-specific queries helps avoid weird boolean issues with the search engines
    queries = [
        f"site:jobs.lever.co {query}",
        f"site:jobs.ashbyhq.com {query}",
        f"site:boards.greenhouse.io {query}",
        f"site:jobs.smartrecruiters.com {query}"
    ]
    
    # Let's try DuckDuckGo first
    with DDGS() as ddgs:
        for q in queries:
            try:
                print(f"Querying DDG: {q}")
                # backend='html' seems to give more reliable results for this kind of thing
                search_gen = ddgs.text(q, max_results=config.MAX_RESULTS, backend="html")
                count = 0
                for r in search_gen:
                    href = r.get('href')
                    if href:
                        results.append(href)
                        count += 1
                print(f"DDG found {count} results for '{q}'")
            except Exception as e:
                print(f"Error searching DDG for '{q}': {e}")
                # If DDG chokes, let's try Google for this query
                try:
                    print(f"Trying Google fallback for: {q}")
                    for url in google_search(q, num_results=config.MAX_RESULTS, sleep_interval=2.0):
                        results.append(url)
                except Exception as google_error:
                    print(f"Google search also failed for '{q}': {google_error}")
        
    return list(set(results)) # Clean up duplicates

def search_ashby_directly(query):
    """
    Ashby's GraphQL API is actually public (non-user), so we can query it directly.
    Much better than relying on SEO-unfriendly pages.
    """
    print(f"\nDirect Ashby API search for known companies...")
    results = []
    query_lower = query.lower()
    
    # Extract keywords from the user's query
    # We remove punctuation and boolean operators to get a clean list of terms
    clean_query = query_lower.replace('(', '').replace(')', '').replace('"', '').replace(' or ', ' ')
    keywords = [k.strip() for k in clean_query.split() if len(k.strip()) > 1]
    
    # Fallback to defaults only if parsing failed completely
    if not keywords:
        keywords = ['engineer', 'ai', 'ml', 'machine learning', 'developer']
        
    print(f"  Filtering API results for keywords: {keywords}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Content-Type': 'application/json'
    }
    
    api_url = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"
    
    for company in config.ASHBY_COMPANIES:
        try:
            print(f"  Querying API for {company}...")
            
            # This is the query Ashby's frontend uses. We just need the job postings list.
            graphql_query = {
                "operationName": "ApiJobBoardWithTeams",
                "variables": {"organizationHostedJobsPageName": company},
                "query": "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobPostings { id title locationName } } }"
            }
            
            response = requests.post(api_url, json=graphql_query, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'errors' in data:
                    print(f"    GraphQL errors: {data['errors']}")
                    continue
                
                job_postings = data.get('data', {}).get('jobBoard', {}).get('jobPostings', [])
                
                print(f"    API returned {len(job_postings)} total jobs for {company}")
                
                for job in job_postings:
                    title = job.get('title', '').lower()
                    # Does the title look relevant?
                    if any(keyword in title for keyword in keywords):
                        job_url = f"https://jobs.ashbyhq.com/{company}/{job['id']}"
                        results.append(job_url)
                        print(f"    ✓ Matched: {job.get('title')}")
                
            else:
                print(f"    API error for {company}: Status {response.status_code}")
            
            time.sleep(0.5)  # Let's not hammer their API
            
        except Exception as e:
            print(f"  Error querying {company}: {e}")
            continue
    
    return results

def extract_company_from_url(url):
    """
    Tries to grab the company name from the URL structure.
    Most ATS URLs follow a pattern like domain.com/company-name/job-id
    """
    parsed = urlparse(url)
    path_parts = parsed.path.strip('/').split('/')
    
    if 'lever.co' in parsed.netloc and len(path_parts) >= 1:
        return path_parts[0]
    elif 'ashbyhq.com' in parsed.netloc and len(path_parts) >= 1:
        return path_parts[0]
    elif 'greenhouse.io' in parsed.netloc and len(path_parts) >= 1:
        return path_parts[0]
    elif 'smartrecruiters.com' in parsed.netloc and len(path_parts) >= 1:
        return path_parts[0]
    return "Unknown"

def extract_company_website(soup, job_url, company_name):
    """
    Tries to find the main company website link on the job page.
    If we can't find it, we'll ask Google.
    """
    print(f"  Searching for company website for: {company_name}")
    
    # Strategy 1: Check meta tags
    og_url = soup.find('meta', property='og:url')
    if og_url:
        url = og_url.get('content')
        # Filter out the job board domain itself
        parsed = urlparse(url)
        if parsed.netloc and 'lever.co' not in parsed.netloc and 'greenhouse.io' not in parsed.netloc and 'ashbyhq.com' not in parsed.netloc and 'smartrecruiters.com' not in parsed.netloc:
            print(f"  Found website via og:url: {url}")
            return url
    
    # Strategy 2: Look for common link text
    for link in soup.find_all('a', href=True):
        link_text = link.get_text().lower().strip()
        href = link['href']
        
        if any(keyword in link_text for keyword in ['website', 'company site', 'visit us', 'learn more about', company_name.lower()]):
            # Make sure it's a real URL and not just a link back to the job board
            if href.startswith('http') and 'lever.co' not in href and 'greenhouse.io' not in href and 'ashbyhq.com' not in href and 'smartrecruiters.com' not in href:
                print(f"  Found website via link: {href}")
                return href
    
    # Strategy 3: Google it
    if company_name and company_name != "Unknown":
        try:
            print(f"  Fallback: Searching Google for '{company_name} official website'")
            with DDGS() as ddgs:
                results = ddgs.text(f"{company_name} official website", max_results=1)
                for result in results:
                    url = result.get('href')
                    # Sanity check the result
                    if url and 'lever.co' not in url and 'greenhouse.io' not in url and 'ashbyhq.com' not in url and 'smartrecruiters.com' not in url and 'linkedin.com' not in url:
                        print(f"  Found website via Google: {url}")
                        return url
        except Exception as e:
            print(f"  Google search failed: {e}")
    
    print(f"  No company website found for {company_name}")
    return None

def scrape_company_contacts(company_website, company_name):
    """
    Crawls the company site looking for HR emails or LinkedIn profiles.
    """
    if not company_website:
        return {"hr_email": None, "hr_name": None, "hr_linkedin": None}
    
    print(f"  Scraping contacts from: {company_website}")
    result = {"hr_email": None, "hr_name": None, "hr_linkedin": None}
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        # Where are contacts usually hidden?
        pages_to_check = [
            company_website,
            urljoin(company_website, '/careers'),
            urljoin(company_website, '/about'),
            urljoin(company_website, '/team'),
            urljoin(company_website, '/contact')
        ]
        
        for page_url in pages_to_check[:3]:  # Check first 3 pages only to keep things fast
            try:
                print(f"    Checking: {page_url}")
                response = requests.get(page_url, headers=headers, timeout=8)
                if response.status_code != 200:
                    continue
                
                soup = BeautifulSoup(response.content, 'lxml')
                text = soup.get_text()
                
                # Regex for emails
                email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                emails = re.findall(email_pattern, text)
                
                for email in emails:
                    email_lower = email.lower()
                    # Filter for likely HR addresses
                    if any(keyword in email_lower for keyword in ['recruit', 'hr', 'talent', 'career', 'hiring', 'jobs', 'people']):
                        result['hr_email'] = email
                        print(f"    Found HR email: {email}")
                        break
                
                # Regex for LinkedIn
                linkedin_pattern = r'https?://(?:www\.)?linkedin\.com/in/[\w-]+'
                linkedin_matches = re.findall(linkedin_pattern, response.text)
                if linkedin_matches:
                    result['hr_linkedin'] = linkedin_matches[0]
                    print(f"    Found LinkedIn: {linkedin_matches[0]}")
                
                # Done if we found what we needed
                if result['hr_email'] or result['hr_linkedin']:
                    break
                    
            except Exception as e:
                print(f"    Error checking {page_url}: {e}")
                continue
                
    except Exception as e:
        print(f"  Error scraping contacts: {e}")
    
    if not result['hr_email'] and not result['hr_linkedin']:
        print(f"  No contacts found for {company_name}")
    
    return result

def parse_lever(url, soup):
    """
    Lever parser. Their HTML structure is pretty consistent.
    """
    try:
        # First, check if the job is actually closed.
        page_text = soup.get_text().lower()
        if "no longer open" in page_text or "job is closed" in page_text or "position has been filled" in page_text:
            print(f"Skipping closed job (text match): {url}")
            return None
        
        # Sometimes a closed job redirects to the main company listing page
        postings = soup.find_all('div', class_='posting')
        if len(postings) > 1:
            print(f"Skipping closed job (shows job list): {url}")
            return None

        title = soup.find('h2', class_='posting-headline')
        if not title:
            title = soup.find('h2')
        title = title.get_text(strip=True) if title else "Unknown Title"
        
        # Lever usually hides location in 'sort-by-time' or 'posting-categories'
        location_div = soup.find('div', class_='location')
        location = location_div.get_text(strip=True) if location_div else "Remote/Unknown"
        
        description_div = soup.find('div', class_='content')
        if not description_div:
            description_div = soup.find('div', class_='posting-description')
        
        description = description_div.get_text(strip=True)[:500] + "..." if description_div else "No description found"
        
        # If there's no description, it's probably a junk page
        if description == "No description found" or not description.strip():
            print(f"Skipping job with no description: {url}")
            return None
        
        company = extract_company_from_url(url)
        
        # Enrichment time
        company_website = extract_company_website(soup, url, company)
        contacts = scrape_company_contacts(company_website, company)
        
        return {
            "Job Title": title,
            "Company": company,
            "Location": location,
            "Description": description,
            "Job URL": url,
            "Company Website": company_website,
            "HR Contact Email": contacts['hr_email'],
            "HR Contact Name": contacts['hr_name'],
            "HR LinkedIn": contacts['hr_linkedin'],
            "Source": "Lever"
        }
    except Exception as e:
        print(f"Error parsing Lever URL {url}: {e}")
        return None

def parse_ashby(url, soup):
    """
    Ashby is weird. We use the API for data validation but scrape HTML for the description since the API doesn't always give it freely.
    """
    try:
        print(f"  [Ashby Parser] Processing: {url}")
        
        # Quick check for closed status in the text
        text_content = soup.get_text().lower()
        if "job not found" in text_content or "no longer accepting applications" in text_content or "job is closed" in text_content:
             print(f"  [Ashby Parser] Skipping closed job (text match): {url}")
             return None

        # Grab company/job ID from URL so we can hit the API
        parsed = urlparse(url)
        path_parts = parsed.path.strip('/').split('/')
        
        print(f"  [Ashby Parser] Path parts: {path_parts}")
        
        if len(path_parts) < 2:
            print(f"  [Ashby Parser] Invalid URL structure: {url}")
            return None
            
        company_name = path_parts[0]
        job_id = path_parts[1].split('?')[0].split('/')[0]  # Strip any tracking params
        
        print(f"  [Ashby Parser] Company: {company_name}, Job ID: {job_id}")
        
        # Check against API to verify it's still live
        api_url = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"
        headers_api = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        graphql_query = {
            "operationName": "ApiJobBoardWithTeams",
            "variables": {"organizationHostedJobsPageName": company_name},
            "query": "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobPostings { id title locationName } } }"
        }
        
        response = requests.post(api_url, json=graphql_query, headers=headers_api, timeout=10)
        
        if response.status_code != 200:
            print(f"  [Ashby Parser] API returned status {response.status_code}")
            return None
            
        data = response.json()
        
        if 'errors' in data:
            print(f"  [Ashby Parser] GraphQL errors: {data['errors']}")
            return None
        
        job_postings = data.get('data', {}).get('jobBoard', {}).get('jobPostings', [])
        
        print(f"  [Ashby Parser] Found {len(job_postings)} total jobs at {company_name}")
        
        # Locate our specific job in the list
        job_data = None
        for job in job_postings:
            if job.get('id') == job_id:
                job_data = job
                break
        
        if not job_data:
            print(f"  [Ashby Parser] Job ID {job_id} not found in API response - using HTML fallback")
            # API failed us, fall back to what we can scrape from the page
            title_tag = soup.find('meta', property='og:title') or soup.find('title')
            title = title_tag.get('content') if title_tag.name == 'meta' else title_tag.get_text(strip=True) if title_tag else "Unknown Title"
            if "@" in title:
                title = title.split("@")[0].strip()
            location = "Unknown"
        else:
            print(f"  [Ashby Parser] Found job via API: {job_data.get('title')}")
            title = job_data.get('title', 'Unknown Title')
            location = job_data.get('locationName', 'Unknown')
        
        # Ashby descriptions are usually in these divs
        description = ""
        desc_selectors = [
            soup.find('div', class_='job-description'),
            soup.find('div', {'data-testid': 'job-description'}),
            soup.find('div', class_='posting-description'),
            soup.find('meta', property='og:description')
        ]
        
        for selector in desc_selectors:
            if selector:
                if selector.name == 'meta':
                    description = selector.get('content', '')
                else:
                    description = selector.get_text(strip=True)
                if description and len(description) > 50:
                    description = description[:500] + "..."
                    break
        
        if not description or len(description) < 50:
            print(f"  [Ashby Parser] No meaningful description found: {url}")
            return None

        company = extract_company_from_url(url)
        
        # Enrichment
        company_website = extract_company_website(soup, url, company)
        contacts = scrape_company_contacts(company_website, company)
        
        print(f"  [Ashby Parser] Successfully parsed job: {title}")

        return {
            "Job Title": title,
            "Company": company,
            "Location": location,
            "Description": description,
            "Job URL": url,
            "Company Website": company_website,
            "HR Contact Email": contacts['hr_email'],
            "HR Contact Name": contacts['hr_name'],
            "HR LinkedIn": contacts['hr_linkedin'],
            "Source": "Ashby"
        }
    except Exception as e:
        print(f"  [Ashby Parser] Error parsing {url}: {e}")
        return None

def parse_greenhouse(url, soup):
    """
    Greenhouse parser. Pretty standard stuff.
    """
    try:
        # Title usually in app-title or h1
        title = soup.find('h1', class_='app-title')
        if not title:
            title = soup.find('h1') 
        
        if not title:
            meta_title = soup.find('meta', property='og:title')
            if meta_title:
                title_text = meta_title.get('content')
            else:
                title_text = "Unknown Title"
        else:
            title_text = title.get_text(strip=True)

        location_div = soup.find('div', class_='location')
        if not location_div:
            location_div = soup.find('span', class_='location')
        
        location = location_div.get_text(strip=True) if location_div else "Unknown"
        
        description_div = soup.find('div', id='content')
        if not description_div:
            description_div = soup.find('div', id='main')
        
        description = description_div.get_text(strip=True)[:500] + "..." if description_div else "No description found"
        
        if description == "No description found" or not description.strip():
            print(f"Skipping job with no description: {url}")
            return None
        
        company = extract_company_from_url(url)
        
        company_website = extract_company_website(soup, url, company)
        contacts = scrape_company_contacts(company_website, company)
        
        return {
            "Job Title": title_text,
            "Company": company,
            "Location": location,
            "Description": description,
            "Job URL": url,
            "Company Website": company_website,
            "HR Contact Email": contacts['hr_email'],
            "HR Contact Name": contacts['hr_name'],
            "HR LinkedIn": contacts['hr_linkedin'],
            "Source": "Greenhouse"
        }
    except Exception as e:
        print(f"Error parsing Greenhouse URL {url}: {e}")
        return None

def parse_smartrecruiters(url, soup):
    """
    SmartRecruiters parser. They tend to use schema.org metadata which is nice.
    """
    try:
        page_text = soup.get_text().lower()
        if "no longer available" in page_text or "job is closed" in page_text or "position has been filled" in page_text:
             print(f"Skipping closed job (text match): {url}")
             return None

        title = soup.find('h1', class_='job-title')
        if not title:
            title = soup.find('h1', id='st-jobTitle')
        title = title.get_text(strip=True) if title else "Unknown Title"
        
        location = "Unknown"
        loc_meta = soup.find('meta', itemprop='addressLocality')
        if loc_meta:
            location = loc_meta.get('content')
            
        description_div = soup.find('div', itemprop='description')
        if not description_div:
             description_div = soup.find('div', class_='job-sections')
             
        description = description_div.get_text(strip=True)[:500] + "..." if description_div else "No description found"
        
        if description == "No description found" or not description.strip():
            print(f"Skipping job with no description: {url}")
            return None
        
        company = extract_company_from_url(url)
        
        company_website = extract_company_website(soup, url, company)
        contacts = scrape_company_contacts(company_website, company)
        
        return {
            "Job Title": title,
            "Company": company,
            "Location": location,
            "Description": description,
            "Job URL": url,
            "Company Website": company_website,
            "HR Contact Email": contacts['hr_email'],
            "HR Contact Name": contacts['hr_name'],
            "HR LinkedIn": contacts['hr_linkedin'],
            "Source": "SmartRecruiters"
        }
    except Exception as e:
        print(f"Error parsing SmartRecruiters URL {url}: {e}")
        return None

def is_redirect_to_listing(original_url, final_url):
    """
    Detects if we got bounced to the main careers page, which usually means the job is gone.
    """
    orig = urlparse(original_url)
    final = urlparse(final_url)
    
    # If the path got a lot shorter, we probably lost the job ID part
    if len(final.path) < len(orig.path) and orig.netloc == final.netloc:
        return True
    return False

def scrape_jobs(urls):
    job_data = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
    }

    for url in urls:
        print(f"Scraping: {url}")
        try:
            time.sleep(1) # Be nice to their servers
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"Failed to load {url}: Status {response.status_code}")
                continue
            
            # Did they redirect us?
            if is_redirect_to_listing(url, response.url):
                print(f"Skipping closed job (redirected): {url}")
                continue

            soup = BeautifulSoup(response.content, 'lxml')
            
            # Route to the right parser
            if "lever.co" in url:
                data = parse_lever(url, soup)
            elif "ashbyhq.com" in url:
                print(f"  >>> Routing to parse_ashby for: {url}")
                data = parse_ashby(url, soup)
                print(f"  >>> parse_ashby returned: {data is not None}")
            elif "greenhouse.io" in url:
                data = parse_greenhouse(url, soup)
            elif "smartrecruiters.com" in url:
                data = parse_smartrecruiters(url, soup)
            else:
                continue 
                
            if data:
                job_data.append(data)
                
        except Exception as e:
            print(f"Error processing {url}: {e}")
            
    return job_data

def send_to_webhook(csv_file):
    """
    Pushes the new jobs to our n8n webhook so we can do something with them.
    """
    if not os.path.exists(csv_file):
        print(f"CSV file {csv_file} not found, skipping webhook send.")
        return
    
    try:
        # Load up the data
        df = pd.read_csv(csv_file)
        
        # Clean up NaNs so JSON doesn't break
        df = df.fillna("")
        
        jobs_json = df.to_dict(orient='records')
        
        payload = {
            "jobs": jobs_json,
            "count": len(jobs_json),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        print(f"\nSending {len(jobs_json)} jobs to webhook: {config.WEBHOOK_URL}")
        response = requests.post(config.WEBHOOK_URL, json=payload, timeout=10)
        
        if response.status_code == 200:
            print(f"✓ Successfully sent data to webhook (Status: {response.status_code})")
        else:
            print(f"✗ Webhook returned status {response.status_code}: {response.text}")
            
    except Exception as e:
        print(f"✗ Error sending to webhook: {e}")

def process_results(new_jobs):
    if not new_jobs:
        print("No jobs found.")
        return

    new_df = pd.DataFrame(new_jobs)
    
    # Do we have a master list yet?
    if os.path.exists(config.MASTER_CSV):
        master_df = pd.read_csv(config.MASTER_CSV)
        # Figure out which ones are actually new
        existing_urls = set(master_df['Job URL'])
        delta_df = new_df[~new_df['Job URL'].isin(existing_urls)]
        
        if not delta_df.empty:
            # Add to the master list
            delta_df.to_csv(config.MASTER_CSV, mode='a', header=False, index=False)
            # Save the new ones separately
            delta_df.to_csv(config.DELTA_CSV, index=False)
            print(f"Found {len(delta_df)} new jobs. Saved to {config.DELTA_CSV} and appended to {config.MASTER_CSV}.")
            # Send notification
            send_to_webhook(config.DELTA_CSV)
        else:
            print("No new jobs found since last run.")
            # Reset delta file
            pd.DataFrame(columns=new_df.columns).to_csv(config.DELTA_CSV, index=False)
    else:
        # Fresh start
        new_df.to_csv(config.MASTER_CSV, index=False)
        new_df.to_csv(config.DELTA_CSV, index=False)
        print(f"First run. Saved {len(new_df)} jobs to {config.MASTER_CSV} and {config.DELTA_CSV}.")
        send_to_webhook(config.DELTA_CSV)

import sys

def main():
    # Did the user pass a query?
    if len(sys.argv) > 1:
        search_query = " ".join(sys.argv[1:])
    else:
        # Default fallback
        search_query = '("AI engineer" OR "Gen AI engineer" OR "AI/ML engineer")'
    
    urls = get_search_results(search_query)
    print(f"Found {len(urls)} potential job URLs from search engines.")
    
    # Hit the API for Ashby companies
    ashby_urls = search_ashby_directly(search_query)
    print(f"Found {len(ashby_urls)} potential job URLs from direct Ashby search.")
    
    # Mash them together and clean up
    all_urls = list(set(urls + ashby_urls))
    
    # Enforce strict global limit if set
    if config.MAX_RESULTS < len(all_urls):
        print(f"Limiting total jobs to {config.MAX_RESULTS} (found {len(all_urls)})")
        all_urls = all_urls[:config.MAX_RESULTS]
        
    print(f"\nTotal unique URLs to scrape: {len(all_urls)}")
    
    jobs = scrape_jobs(all_urls)
    process_results(jobs)

if __name__ == "__main__":
    main()
