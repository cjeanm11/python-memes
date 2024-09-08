import requests, os, logging
from bs4 import BeautifulSoup
from typing import List


def fetch_html(url: str) -> str:
    """Fetches the HTML content of a webpage."""
    try:
        response = requests.get(url)
        response.raise_for_status()  
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return ""

def parse_html(html: str) -> BeautifulSoup:
    """Parses HTML content with BeautifulSoup."""
    return BeautifulSoup(html, 'html.parser')

def extract_quotes(soup: BeautifulSoup) -> List[str]:
    """Extracts quotes from the Quotes to Scrape website."""
    quotes = []
    try:
        for item in soup.find_all('span', class_='text'):
            quote = item.get_text(strip=True)
            quotes.append(quote)
    except Exception as e:
        print(f"Error extracting quotes: {e}")
    
    return quotes

def get_next_page_url(soup: BeautifulSoup) -> str:
    """Finds the URL of the next page."""
    next_button = soup.find('li', class_='next')
    if next_button:
        next_link = next_button.find('a')
        if next_link:
            return next_link.get('href')
    return ""


def process_quotes(quotes: List[str]) -> List[str]:
    if not quotes:
        return []
    quotes = list(map(lambda x: x.lstrip('“').rstrip('”') + '\n', quotes))
    return quotes
    
def load(file_name: str, all_quotes: List[str]):
    try:
        with open(file_name, 'w') as fs:
            fs.writelines(all_quotes)
    except IOError as e:
        logging.error(f'Failed to load data to {file_name}: {e}')
    except Exception as e:
        logging.error(f'Unknown error {file_name}: {e}')

def main():
    base_url = 'http://quotes.toscrape.com'
    start_page = '/page/1/'
    url = base_url + start_page
    all_quotes = []
    
    while url:
        print(f"Fetching quotes from {url}")
        html = fetch_html(url)
        
        if not html:
            print("No HTML content to process.")
            break
        
        soup = parse_html(html)
        quotes = extract_quotes(soup)
        quotes = process_quotes(quotes)
        all_quotes.extend(quotes)
        
        next_page = get_next_page_url(soup)
        if next_page:
            url = base_url + next_page
        else:
            break
    
    load('./quotes.txt', all_quotes)

if __name__ == "__main__":
    main()