
import logging
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from bs4 import BeautifulSoup
import csv
from tqdm import tqdm
import time
import argparse
from regex_utils import *


logging.basicConfig(filename='scraper.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

movie_list_base_url = "https://letterboxd.com/films/popular/page/{}"
movie_details_base_url = "https://letterboxd.com/film/film:{}"
file_name = 'popular_movies'
fields = ['Id', 'Title', 'Year', 'Rating', 'Views', 'Likes',
          'Minutes', 'Tagline', 'Language', 'Countries', 'Directors', 'Cast']


def get_web_driver():
    op = webdriver.ChromeOptions()
    op.add_argument('headless')
    op.page_load_strategy = 'eager'
    driver = webdriver.Chrome(options=op)
    return driver


def get_movie_ids(page_number):
    driver = get_web_driver()

    try:
        driver.get(movie_list_base_url.format(page_number))
        time.sleep(1)
        content = driver.page_source.encode('utf-8').strip()
        soup = BeautifulSoup(content, 'html.parser')
        movies_scapped = soup.find_all(
            'li', class_='listitem poster-container')
        movie_ids = []

        if not movies_scapped:
            print(f'No movies scrapped, page: {page_number}')
            return movie_ids

        for movie in movies_scapped:
            # rating = movie['data-average-rating'] if movie.has_attr('data-average-rating') else 'N/A'
            info = movie.find('div', recursive=False)
            if info:
                id = info['data-film-id'] if info.has_attr(
                    'data-film-id') else None
                # title = info['data-film-name'] if  info.has_attr('data-film-name') else 'N/A'
                # year = info['data-film-release-year'] if  info.has_attr('data-film-release-year') else 'N/A'

            # movie_data.append([id, title, year, rating])
            movie_ids.append(id)

        if not movie_ids:
            print(f'No movies parsed, page: {page_number}')

        print(f'{len(movie_ids)} ids scrapped from page {page_number}')
        return movie_ids

    finally:
        driver.quit()


def get_movie_details(id):
    driver = get_web_driver()

    try:
        driver.get(movie_details_base_url.format(id))
        time.sleep(1)
        content = driver.page_source.encode('utf-8').strip()
        soup = BeautifulSoup(content, 'html.parser')
        title_span = soup.find('span', class_='name js-widont prettify')
        title = title_span.text if title_span else None

        release_year_div = soup.find('div', class_='releaseyear')
        link = release_year_div.findChild('a') if release_year_div else None
        release_year = int(link.text) if link else None

        avg_rating_span = soup.find('span', class_='average-rating')
        avg_rating_a = avg_rating_span.findChild(
            'a') if avg_rating_span else None
        avg_rating = float(avg_rating_a.text) if avg_rating_a else None

        length_p = soup.find('p', class_='text-link text-footer')
        minutes = get_number_by_regex(
            length_p.text, minutes_regex) if length_p else None

        views_a = soup.find(
            'a', class_='has-icon icon-watched icon-16 tooltip')
        if views_a and views_a.has_attr('data-original-title'):
            text = views_a.attrs['data-original-title']
            views = get_number_by_regex(text, members_regex)
        else:
            views = None

        likes_a = soup.find(
            'a', class_='has-icon icon-like icon-liked icon-16 tooltip')
        if likes_a and likes_a.has_attr('data-original-title'):
            text = likes_a.attrs['data-original-title']
            likes = get_number_by_regex(text, members_regex)
        else:
            likes = None

        tagline_h = soup.find('h4', class_='tagline')
        tagline = tagline_h.text if tagline_h else None

        description_truncate = soup.find('div', class_='truncate')
        if description_truncate:
            text_p = description_truncate.findChild('p')
            description = text_p.text if text_p else None
        else:
            description = None

        directors_list_span = soup.find('span', class_='directorlist')
        directors_list = [a.find('span', class_='prettify').text for a in directors_list_span.find_all(
            'a', class_='contributor')] if directors_list_span else None

        cast_list_div = soup.find('div', class_='cast-list text-sluglist')
        if cast_list_div:
            cast_list_a = cast_list_div.find('p').find_all(
                'a', class_='text-slug tooltip')
            cast = [a.text for a in cast_list_a]
            cast = cast[:5]
        else:
            cast = None

        details_div = soup.find('div', id='tab-details')

        # Studios
        # studios_section = details_div.find('h3', string='Studios')
        # studio = [a.text for a in studios_section.find_next('div', class_='text-sluglist').find_all('a', class_='text-slug')] if studios_section else None

        if details_div:
            countries_section = details_div.find('h3', string='Country')
            if not countries_section:
                countries_section = details_div.find('h3', string='Countries')
            countries = [a.text for a in countries_section.find_next(
                'div', class_='text-sluglist').find_all('a', class_='text-slug')] if countries_section else None

            primary_language_section = details_div.find(
                'h3', string='Primary Language')
            if not primary_language_section:
                primary_language_section = details_div.find(
                    'h3', string='Language')
            primary_language = primary_language_section.find_next(
                'div', class_='text-sluglist').find('a', class_='text-slug').text if primary_language_section else None
        else:
            countries = None
            primary_language = None

        return [
            id,
            title,
            release_year,
            avg_rating,
            views,
            likes,
            minutes,
            tagline,
            # description,
            # studio,
            primary_language,
            countries,
            directors_list,
            cast
        ]

    except WebDriverException as e:
        logging.error(f'Error getting movie details for id {id}: {e}')
        return None

    finally:
        driver.quit()


def get_movies(start, pages):
    ids = []
    for i in tqdm(range(start, pages)):
        movie_ids = get_movie_ids(i+1)
        ids.extend(movie_ids)

    logging.info(f'Number of scrapped ids on {pages} pages: {len(ids)}')

    movie_details_list = []
    for id in tqdm(ids):
        if not id:
            continue
        movie_details = get_movie_details(id)
        if movie_details:
            movie_details_list.append(movie_details)

    return movie_details_list


def write_to_file(movies, file_name=file_name, fields=fields):
    with open(f'{file_name}.csv', 'w+') as file:
        writer = csv.writer(file)
        writer.writerow(fields)
        writer.writerows(movies)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape movie details from Letterboxd.')
    parser.add_argument('-s', '--start', type=int, default=0, help='Starting page number')
    parser.add_argument('-e', '--end', type=int, default=10, help='Ending page number')
    parser.add_argument('-f', '--filename', type=str, default='popular_movies', help='Output CSV file name')

    args = parser.parse_args()

    try:
        movies = get_movies(args.start, args.end)
        write_to_file(movies=movies, file_name=args.filename)
    except Exception as e:
        logging.critical(f'Unhandled exception: {e}')
