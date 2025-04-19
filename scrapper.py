import asyncio
import aiohttp
import json
import random
import re
import time
from dataclasses import dataclass
from typing import List, Optional
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE_URL = "https://www.otomoto.pl/osobowe"

USER_AGENTS = [
    # (same list as before — unchanged)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.66",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Brave/1.63.162",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/98.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12.5; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Brave/1.63.162",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.66",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Vivaldi/6.6.3271.57",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Vivaldi/6.6.3271.57",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.66",
]

@dataclass
class Car:
    link: str
    full_name: str
    description: str
    year: int
    mileage_km: str
    engine_capacity: str
    fuel_type: str
    price_pln: int
    make: str
    model: str
    door_count: Optional[str] = None
    vin: Optional[str] = None
    body_type: Optional[str] = None
    color: Optional[str] = None
    gearbox: Optional[str] = None
    engine_power: Optional[int] = None
    date_registration: Optional[str] = None
    transmission: Optional[str] = None
    price_indicator: Optional[str] = None
    price_range: Optional[str] = None
    no_accident: Optional[str] = None
    country_origin: Optional[str] = None
    new_used: Optional[str] = None
    registration_date_history: Optional[str] = None

    def __post_init__(self):
        # Ensure missing data is handled and not set to defaults
        if self.door_count is None:
            self.door_count = "unknown"
        if self.vin is None:
            self.vin = "unknown"
        if self.body_type is None:
            self.body_type = "unknown"
        if self.color is None:
            self.color = "unknown"
        if self.gearbox is None:
            self.gearbox = "unknown"
        if self.engine_power is None:
            self.engine_power = 0
        if self.date_registration is None:
            self.date_registration = "unknown"
        if self.transmission is None:
            self.transmission = "unknown"
        if self.price_indicator is None:
            self.price_indicator = "unknown"
        if self.price_range is None:
            self.price_range = "unknown"
        if self.no_accident is None:
            self.no_accident = "unknown"
        if self.country_origin is None:
            self.country_origin = "unknown"
        if self.new_used is None:
            self.new_used = "used"  # Default to used if not specified
        if self.registration_date_history is None:
            self.registration_date_history = "unknown"

class AsyncOtomotoScraper:
    def __init__(self, max_pages=1):
        self.base_url = BASE_URL
        self.max_pages = max_pages

    async def fetch_html(self, session, url, retries=2, delay=1):
        """Faster HTML fetching with optimized retry logic."""
        for attempt in range(retries):
            try:
                headers = {
                    "User-Agent": random.choice(USER_AGENTS)
                }
                async with session.get(url, headers=headers) as response:
                    if response.status == 403:
                        print(f"[403] Forbidden: {url} | Attempt {attempt + 1} of {retries}")
                        await asyncio.sleep(5)  # Shorter wait on 403
                        continue
                    elif response.status != 200:
                        print(f"[{response.status}] Failed to fetch: {url}")
                        if attempt < retries - 1:
                            await asyncio.sleep(delay)
                        continue
                    return await response.text()
            except Exception as e:
                print(f"[Error] Failed to fetch {url}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
        return ""

    def extract_links_from_page(self, soup: BeautifulSoup) -> List[str]:
        links = []
        for article in soup.find_all("article"):
            a_tag = article.find("a", href=True)
            if a_tag:
                link = a_tag["href"].strip()
                if not link.startswith("http"):
                    link = "https://www.otomoto.pl" + link
                if "/oferta/" in link:
                    links.append(link)
        return links

    async def fetch_car_details(self, session, url) -> Optional[Car]:
        try:
            html = await self.fetch_html(session, url)
            soup = BeautifulSoup(html, "html.parser")

            full_name = soup.find("h1")
            full_name = full_name.text.strip() if full_name else "unknown"

            desc_section = soup.find("div", {"data-testid": "content-description-section"})
            description = ""
            if desc_section:
                description = "\n".join(p.get_text(strip=True) for p in desc_section.find_all("p"))

            price_tag = soup.select_one("span.offer-price__number") or soup.select_one(".offer-price")
            price_pln = int(re.sub(r'\D', '', price_tag.text)) if price_tag else 0

            # Extract key attributes from data-testid elements
            def extract_data_testid_value(testid: str) -> str:
                tag = soup.find("div", {"data-testid": testid})
                if tag:
                    # Find all <p> elements within this div, regardless of class
                    ps = tag.find_all("p")
                    # Always use the last <p> element as it contains the value
                    if len(ps) >= 2:
                        return ps[-1].text.strip()
                return None

            make = extract_data_testid_value("make")
            model = extract_data_testid_value("model")
            door_count = extract_data_testid_value("door_count")
            body_type = extract_data_testid_value("body_type")
            color = extract_data_testid_value("color")
            gearbox = extract_data_testid_value("gearbox")
            vin = extract_data_testid_value("vin")
            date_registration = extract_data_testid_value("first_registration")
            no_accident = extract_data_testid_value("no_accident")
            # Add transmission extraction
            transmission = extract_data_testid_value("transmission")
            
            # Add new extractions
            country_origin = extract_data_testid_value("country_origin")
            new_used_status = extract_data_testid_value("new_used")
            registration_date_history = extract_data_testid_value("date_registration")
            
            # Try to extract engine power from data-testid
            engine_power_text = extract_data_testid_value("engine_power")
            engine_power = None
            if engine_power_text:
                power_match = re.search(r'\d+', engine_power_text)
                if power_match:
                    engine_power = int(power_match.group())

            detail_labels = soup.find_all("p", class_=re.compile(r"ooa-11fwepm"))
            fuel_type, mileage, engine_capacity = None, None, None
            for label in detail_labels:
                text = label.get_text(strip=True)
                if "km" in text:
                    mileage = text
                elif "cm3" in text:
                    engine_capacity = text
                elif text.lower() in ["benzyna", "diesel", "hybryda", "elektryczny", "lpg", "cng"]:
                    fuel_type = text

            # Extract old-style details as fallback
            details = {}
            for div in soup.find_all("div", class_="offer-params__item"):
                label = div.find("span", class_="offer-params__label")
                value = div.find("div", class_="offer-params__value")
                if label and value:
                    details[label.text.strip().lower()] = value.text.strip()

            year_text = details.get("rok produkcji") or extract_data_testid_value("year")
            year = int(re.search(r'\d{4}', year_text).group()) if year_text and re.search(r'\d{4}', year_text) else 0

            if not engine_power and details.get("moc"):
                power_match = re.search(r'\d+', details.get("moc"))
                if power_match:
                    engine_power = int(power_match.group())

            # Extract VIN - special case for hidden VIN
            vin = await self.extract_vin_with_playwright(url)
            
            # Extract price indicator and price range directly from the HTML
            # This is more reliable than using Playwright for this information
            price_indicator = None
            price_range = None
            
            # Find the price indicator element
            price_indicator_tag = soup.find("p", {"data-testid": re.compile("price-indicator-label-.*")})
            if price_indicator_tag:
                # Get the actual indicator text
                price_indicator = price_indicator_tag.text.strip()
                
                # Extract the indicator type from the data-testid attribute
                data_testid = price_indicator_tag.get("data-testid", "")
                indicator_match = re.search(r'price-indicator-label-(\w+)', data_testid)
                
                # Get the current car price
                price_tag = soup.select_one("span.offer-price__number") or soup.select_one(".offer-price")
                
                # Only estimate price range if we have both indicator type and price
                if indicator_match and price_tag:
                    try:
                        indicator_type = indicator_match.group(1)
                        price = int(re.sub(r'\D', '', price_tag.text))
                        
                        # Create estimated price ranges based on indicator type
                        if indicator_type == "ABOVE":
                            lower_bound = int(price * 0.85)
                            price_range = f"{lower_bound}-{price} PLN"
                        elif indicator_type == "BELOW":
                            upper_bound = int(price * 1.15)
                            price_range = f"{price}-{upper_bound} PLN"
                        elif indicator_type == "IN":
                            lower_bound = int(price * 0.9)
                            upper_bound = int(price * 1.1)
                            price_range = f"{lower_bound}-{upper_bound} PLN"
                    except (ValueError, AttributeError) as e:
                        print(f"Error estimating price range: {e}")
            
            # If Playwright failed, fall back to original method
            if not vin:
                vin_div = soup.find("div", {"data-testid": "advert-vin"})
                if vin_div:
                    vin_p = vin_div.find("p")
                    if vin_p:
                        vin = vin_p.text.strip()
                
                if not vin:
                    vin = extract_data_testid_value("vin")
                    
                if not vin and details.get("vin"):
                    vin = details.get("vin")

            # Use data-testid values first, then fall back to traditional HTML structure
            return Car(
                link=url,
                full_name=full_name,
                description=description,
                year=year,
                mileage_km=mileage if mileage else None,
                engine_capacity=engine_capacity if engine_capacity else None,
                fuel_type=fuel_type if fuel_type else None,
                price_pln=price_pln,
                make=make,
                model=model,
                door_count=door_count or details.get("liczba drzwi"),
                vin=vin,  # Updated to use our new VIN extraction
                body_type=body_type or details.get("nadwozie"),
                color=color or details.get("kolor"),
                gearbox=gearbox or details.get("skrzynia biegów"),
                engine_power=engine_power,
                date_registration=date_registration or details.get("pierwsza rejestracja"),
                transmission=transmission or details.get("napęd"),
                price_indicator=price_indicator,
                price_range=price_range,
                no_accident=no_accident,
                country_origin=country_origin,
                new_used=new_used_status,
                registration_date_history=registration_date_history
            )
        except Exception as e:
            print(f"Error parsing car detail page {url}: {e}")
            return None
    
    async def extract_vin_with_playwright(self, url) -> Optional[str]:
        """Extract VIN using Playwright with improved error handling and retries."""
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                async with async_playwright() as p:
                    # Use headless=False for debugging if needed
                    browser = await p.chromium.launch(headless=True)
                    context = await browser.new_context(
                        viewport={"width": 1920, "height": 1080},
                        user_agent=random.choice(USER_AGENTS)
                    )
                    page = await context.new_page()
                    
                    # Increase timeout and use less strict loading state
                    await page.goto(url, wait_until="load", timeout=30000)
                    
                    # Handle cookie consent popup if present
                    try:
                        consent_button = page.locator('button[id="onetrust-accept-btn-handler"]')
                        if await consent_button.count() > 0:
                            await consent_button.click()
                            await page.wait_for_timeout(1000)
                    except Exception:
                        pass
                    
                    # First check if VIN is directly visible without clicking
                    vin_element = page.locator("div[data-testid='vin'] div[data-testid='advert-vin'] p")
                    if await vin_element.count() > 0:
                        vin = await vin_element.text_content()
                        await browser.close()
                        return vin.strip()
                    
                    # Look for the "Wyświetl VIN" button and click it using JavaScript
                    vin_button = page.locator("button:has-text('Wyświetl VIN')")
                    if await vin_button.count() > 0:
                        # Click using JavaScript to bypass overlay issues
                        await page.evaluate("""() => {
                            const buttons = Array.from(document.querySelectorAll('button'));
                            const vinButton = buttons.find(b => b.textContent.includes('Wyświetl VIN'));
                            if (vinButton) vinButton.click();
                        }""")
                        
                        # Wait for the VIN to appear with increased timeout
                        try:
                            await page.wait_for_selector("div[data-testid='advert-vin'] p", timeout=10000)
                            vin_element = page.locator("div[data-testid='advert-vin'] p")
                            if await vin_element.count() > 0:
                                vin = await vin_element.text_content()
                                await browser.close()
                                return vin.strip()
                        except Exception:
                            # If we can't get the VIN after clicking, try fallback methods
                            pass
                    
                    await browser.close()
                    return None
            except Exception as e:
                if attempt < max_retries:
                    print(f"Retry {attempt+1}/{max_retries} after error extracting VIN: {e}")
                    await asyncio.sleep(5)  # Wait before retrying
                else:
                    print(f"Error extracting VIN with Playwright: {e}")
                    return None

    async def scrape(self) -> List[Car]:
        async with aiohttp.ClientSession() as session:
            # Get all links first
            all_links = []
            for page in range(1, self.max_pages + 1):
                # Reduced delay between pages
                await asyncio.sleep(0.5)
                url = f"{self.base_url}/?page={page}"
                html = await self.fetch_html(session, url)
                soup = BeautifulSoup(html, "html.parser")
                page_links = self.extract_links_from_page(soup)
                all_links.extend(page_links)

            print(f"[Scraper] Total links found: {len(all_links)}")
            
            # Process links in larger batches for better performance
            chunk_size = 8  # Process more cars at a time
            cars = []
            
            for i in range(0, len(all_links), chunk_size):
                chunk = all_links[i:i+chunk_size]
                tasks = [self.fetch_car_details(session, link) for link in chunk]
                chunk_results = await asyncio.gather(*tasks)
                cars.extend([car for car in chunk_results if car is not None])
                # Shorter delay between batches
                if i + chunk_size < len(all_links):
                    await asyncio.sleep(0.5)
            
            return cars

def transform_car_to_schema(car: Car) -> dict:
    # Convert the no_accident string to an integer (1 for "Tak", 0 for anything else)
    no_accident_value = 1 if car.no_accident and car.no_accident.lower() == "tak" else 0
    
    return {
        "params": {
            "door_count": car.door_count,
            "vin": car.vin,
            "make": car.make.lower() if car.make else "",
            "model": car.model.lower() if car.model else "",
            "is_imported_car": 0,
            "fuel_type": car.fuel_type.lower() if car.fuel_type else "",
            "no_accident": no_accident_value,
            "body_type": car.body_type,
            "mileage": re.sub(r"[^\d]", "", car.mileage_km) if car.mileage_km else "",
            "color": car.color,
            "year": str(car.year),
            "price": {
                "0": "price",
                "1": str(car.price_pln),
                "currency": "PLN",
                "gross_net": "gross"
            },
            "engine_capacity": re.sub(r"[^\d]", "", car.engine_capacity) if car.engine_capacity else "",
            "engine_power": car.engine_power,
            "gearbox": car.gearbox,
            "transmission": car.transmission,
            # Use None or 0 instead of hardcoded 1 for features that aren't confirmed
            "antilock_brake_system": None,
            "apple_carplay": None,
            "metallic": None,
            "country_origin": car.country_origin.lower() if car.country_origin else "pl",
            "date_registration": car.registration_date_history or car.date_registration,
            "has_registration": 1,
            "cepik_authorization": 1
        },
        "title": car.full_name,
        "description": car.description,
        "new_used": "new" if car.new_used and car.new_used.lower() == "nowy" else "used",
        "category_id": 29,
        "url": car.link,
        "price_indicator": car.price_indicator,
        "price_range": car.price_range
    }
    
def extract_price_range(soup: BeautifulSoup) -> Optional[str]:
    """This function only works if the price range is visible in the static HTML, which is usually not the case."""
    try:
        # Fixed deprecation warning by using 'string' instead of 'text'
        text_blocks = soup.find_all(string=re.compile(r'\d+\s*-\s*\d+\s*PLN'))
        for text in text_blocks:
            match = re.search(r'(\d[\d\s]*\d)\s*-\s*(\d[\d\s]*\d)\s*PLN', text)
            if match:
                lower = match.group(1).replace(" ", "")
                upper = match.group(2).replace(" ", "")
                return f"{lower}-{upper} PLN"
        return None
    except Exception as e:
        print(f"Failed to extract price range: {e}")
        return None


async def scrape_osobowe_pages(start_page: int = 1, end_page: int = 1):
    scraper = AsyncOtomotoScraper(max_pages=end_page)
    cars = await scraper.scrape()
    return cars

if __name__ == "__main__":
    cars_data = asyncio.run(scrape_osobowe_pages(start_page=1, end_page=1))
    final_data = [transform_car_to_schema(car) for car in cars_data]

    with open("otomoto_data_2.json", "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)

    print(f"Scraped and saved {len(final_data)} cars.")
