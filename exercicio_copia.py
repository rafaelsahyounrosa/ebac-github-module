import requests
import time
import csv
import random
import concurrent.futures


from bs4 import BeautifulSoup

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

MAX_THREADS = 20


def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))
    response = BeautifulSoup(requests.get(movie_link, headers=headers).content, 'html.parser')
    movie_soup = response

    if movie_soup is not None:
        title = None
        date = None

        movie_data = movie_soup.find('section', attrs={'class': 'ipc-page-section ipc-page-section--baseAlt ipc-page-section--tp-xs ipc-page-section--bp-xs Hero__HeroParent-kvkd64-1 fARFJI'})
        if movie_data is not None:
            title = movie_data.find('h1').get_text()
            date = movie_data.find('a', attrs={'class': 'ipc-link ipc-link--baseAlt ipc-link--inherit-color TitleBlockMetaData__StyledTextLink-sc-12ein40-1 rgaOW'}).get_text().strip()

            rating = movie_soup.find('span', attrs={'class': 'AggregateRatingButton__RatingScore-sc-1ll29m0-1 iTLWoV'}).get_text()

            plot_text = movie_soup.find('span', attrs={'class': 'GenresAndPlot__TextContainerBreakpointXL-cum89p-2 gCtawA'}).get_text().strip()

        with open('movies.csv', mode='a') as file:
            movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            movie_writer.writerow(['title','date','rating','plot_text'])
            if all([title, date, rating, plot_text]):
                print(title, date, rating, plot_text)
                movie_writer.writerow([title, date, rating, plot_text])


def extract_movies(soup):
    movies_table = soup.find('table', attrs={'data-caller-name': 'chart-moviemeter'}).find('tbody')
    movies_table_rows = movies_table.find_all('tr')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(extract_movie_details, movie_links)


def main():
    start_time = time.time()

    # IMDB Most Popular Movies - 100 movies
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Main function to extract the 100 movies from IMDB Most Popular Movies
    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)


if __name__ == '__main__':
    main()