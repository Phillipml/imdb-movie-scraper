import asyncio
import aiohttp
import time
import csv
import random
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"
}

MAX_CONCURRENT_REQUESTS = 20


async def extract_movie_details_async(session, url):
    await asyncio.sleep(random.uniform(0, 0.1))

    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                content = await response.text()
                soup = BeautifulSoup(content, "html.parser")

                if soup is not None:
                    title = None
                    date = None

                    page_section = soup.find(
                        "section", attrs={"class": "ipc-page-section"}
                    )

                    if page_section is not None:
                        divs = page_section.find_all("div", recursive=False)

                        if len(divs) > 1:
                            target_div = divs[1]

                            title_tag = target_div.find("h1")
                            if title_tag:
                                title = title_tag.find("span").get_text()

                            date_tag = target_div.find(
                                "a", href=lambda href: href and "releaseinfo" in href
                            )
                            if date_tag:
                                date = date_tag.get_text().strip()

                            rating_tag = soup.find(
                                "div",
                                attrs={
                                    "data-testid": "hero-rating-bar__aggregate-rating__score"
                                },
                            )
                            rating = rating_tag.get_text() if rating_tag else None

                            plot_tag = soup.find(
                                "span", attrs={"data-testid": "plot-xs_to_m"}
                            )
                            plot_text = (
                                plot_tag.get_text().strip() if plot_tag else None
                            )

                            if all([title, date, rating, plot_text]):
                                print(f"✓ {title} ({date}) - {rating}")
                                return [title, date, rating, plot_text]
                            else:
                                print(f"✗ Dados incompletos para: {url}")
    except aiohttp.ClientError as e:
        print(f"Erro de conexão: {url} - {e}")
    except Exception as e:
        print(f"Erro inesperado: {url} - {e}")

    return None


async def extract_movies_async(session, soup):
    movies_table = soup.find(
        "div", attrs={"data-testid": "chart-layout-main-column"}
    ).find("ul")
    movies_table_rows = movies_table.find_all("li")

    urls = ["https://imdb.com" + movie.find("a")["href"] for movie in movies_table_rows]

    print(f"Encontrados {len(urls)} filmes para processar...")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def limited_extract(link):
        async with semaphore:
            return await extract_movie_details_async(session, link)

    tasks = [limited_extract(link) for link in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    valid_results = [result for result in results if result is not None]
    print(f"Processados com sucesso: {len(valid_results)}/{len(urls)}")

    return valid_results


async def main_async():
    start_time = time.time()
    print("Iniciando web scraping do IMDB...")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        popular_movies_url = "https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm"

        print("Acessando página principal...")
        async with session.get(popular_movies_url, headers=headers) as response:
            if response.status == 200:
                content = await response.text()
                soup = BeautifulSoup(content, "html.parser")

                print("Extraindo dados dos filmes...")
                results = await extract_movies_async(session, soup)

                output_file = "movies_async.csv"
                with open(output_file, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(
                        file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
                    )
                    writer.writerow(["Title", "Date", "Rating", "Plot"])
                    for result in results:
                        writer.writerow(result)

                print(f"Dados salvos em: {output_file}")
            else:
                print(f"Erro ao acessar página: {response.status}")

    end_time = time.time()
    print(f"Tempo total: {end_time - start_time:.2f} segundos")


if __name__ == "__main__":
    asyncio.run(main_async())
