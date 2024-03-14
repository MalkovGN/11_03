import csv
import random
import re
import time
import aiohttp
import asyncio
import requests
from aiosocksy import Socks5Auth
from aiosocksy.connector import ProxyConnector, ProxyClientRequest
from bs4 import BeautifulSoup as Soup
from fake_http_header import FakeHttpHeader

from test_07_02.headers import headers_data



def get_headers():
    try:
        fake_header = FakeHttpHeader()
        headers = fake_header.as_header_dict()
    except:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        }

    return headers


pages_data = []
bad_urls = []
bad_proxy = []


async def check_disk(session, link, sem):
    async with sem:
        with open('proxies.txt', 'r', encoding='utf-8') as proxy_file:
            proxies = proxy_file.readlines()
            proxy = random.choice(proxies)
            protocol = proxy[:9]
            ip_port = proxy.split('@')[-1]
            login = proxy[9:].split(':')[0]
            password = proxy[9:].split(':')[1].split('@')[0]
            socks = protocol + ip_port

        auth = Socks5Auth(login=login, password=password)
        try:
            async with session.get(url=link.rstrip(), proxy=socks, proxy_auth=auth, headers=get_headers()) as response:
                try:
                    data = await response.text()
                    pages_data.append({link: data})
                except Exception as ex:
                    print(f'[BAD REQUEST] --- {ex}', response.status)
                    bad_urls.append(link)
        except:
            bad_urls.append(link)

        await asyncio.sleep(1)


async def gather_data(link_idx):
    sem = asyncio.Semaphore(3)
    with open('not_saved_links.txt', 'r', encoding='utf-8') as f:
        links = f.readlines()[link_idx: link_idx + 10]

        tasks = []
        connector = ProxyConnector()
        async with aiohttp.ClientSession(connector=connector, request_class=ProxyClientRequest) as session:
            for link in links:
                task = asyncio.create_task(check_disk(session, link, sem))
                tasks.append(task)
            await asyncio.gather(*tasks)


if __name__ == '__main__':

    link_idx = 0
    with open('not_saved_links.txt', 'r', encoding='utf-8') as links_file:
        lines = links_file.readlines()
        links_file.close()

    while link_idx < len(lines):
        asyncio.run(gather_data(link_idx))
        for data in pages_data:
            soup = Soup(''.join(list(data.values())[0]), 'lxml')
            try:
            # MAIN INFO
                try:
                    name = soup.find('div', class_='person-name').text
                except:
                    name = '-'
                try:
                    location = soup.find('div', class_='content text-center has-smaller-text').find_all('span')[-2].findNext('b').text
                except:
                    location = '-'
                try:
                    all_categories_tags = soup.find_all('a', class_='person-profession')
                    all_categories_list = []
                    for category in all_categories_tags:
                        all_categories_list.append(category.text.lstrip().split(' ')[0])
                        all_categories = ' '.join(all_categories_list)
                except:
                    all_categories = '-'
                    # print(' '.join(all_categories))
                meta_title = f"{name} - {all_categories} - {location}"
                try:
                    top_categories = soup.find('section', class_='profile-section').findNext('p').text
                except:
                    top_categories = '-'
                try:
                    user_stats = soup.find('div', class_='user-stats').find_all('div', class_='user-stat')
                except:
                    user_stats = '-'
                try:
                    job_order = user_stats[0].findNext('span').text
                except:
                    job_order = '-'
                try:
                    review = user_stats[1].findNext('b').text
                except:
                    review = '-'
                try:
                    rating_one = soup.find('div', class_='rate rating-stars is-disabled').find_all('div', class_='rate-item set-on')
                    rating_half = soup.find('div', class_='rate rating-stars is-disabled').find_all('div', class_='rate-item set-half')
                    rating = len(rating_half) + len(rating_one)
                except:
                    rating = '-'
                try:
                    activity = soup.find('div', class_='content text-center has-smaller-text').find_all('p')[-2].text
                except:
                    activity = '-'
                try:
                    description = soup.find('span', class_='card-content memo').text
                except:
                    description = '-'
                try:
                    avatar = soup.find('picture').findNext('source')['data-srcset'].split(',')[-1][0:-4]
                except:
                    avatar = '-'
                try:
                    tmp = soup.find('div', class_='has-text-white active-indicator').find_all('span')[-1]
                    fogadok_munkat = True
                except:
                    fogadok_munkat = False
                phone = soup.find('div', class_='content text-center has-smaller-text').find_all('p')[-1].text
                if not phone.lstrip()[1].isdigit():
                    phone = ''

                # PRICES
                prices_data = []
                try:
                    price_block = soup.find('div', class_="shadow-md rounded-xl bg-white p-4 section-card")
                    price_titles = price_block.find_all('h1', class_='text-xl font-bold text-primary-light mb-2')
                    price_descriptions = price_block.find_all('span', class_='text-gray-500 block whitespace-pre-wrap md:col-span-2')
                    price = price_block.find_all('div', class_='mb-2 text-gray-600 hover:text-gray-600')
                    for idx in range(len(price_titles)):
                        price_digits = re.findall(r'[0-9]+', price[idx].text.replace('\xa0', ''))
                        try:
                            min_price = price_digits[0]
                            max_price = price_digits[1]
                        except:
                            max_price = price_digits[0]
                            min_price = ''
                        unit = price[idx].text.split('/')[-2].split('\xa0')[-1] + '/' + price[idx].text.split('/')[-1]
                        price_title = price_titles[idx].text
                        price_description = price_descriptions[idx].text
                        prices_data.append(
                            [
                                list(data.keys())[0].split('szakember/')[-1],
                                price_title,
                                price_description,
                                min_price,
                                max_price,
                                unit,
                            ]
                        )
                except:
                    pass

                    # print(price_titles[idx].text, ' ', price_descriptions[idx].text, ' ', min_price, max_price, unit)
                # REVIEWS
                review_data = []
                try:
                    all_reviews = soup.find('section', id='ratings').find_all('article', class_='score-media')
                    for r in all_reviews:
                        author = r.findNext('b').text
                        ava = r.findNext('img')['data-src']
                        date_time = r.findNext('time').text
                        rating = str(r.findNext('div', class_='rate is-disabled')).count('rate-item set-on')
                        review_text = r.findNext('span', class_='score-what').text
                        try:
                            response_text = r.findNext('span', class_='score-reply').text
                            print(response_text)
                        except:
                            response_text = ''
                        review_data.append(
                            [
                                list(data.keys())[0].split('szakember/')[-1],
                                author,
                                ava,
                                date_time,
                                rating,
                                review_text,
                                response_text,
                            ]
                        )
                except:
                    pass
                                # print(review_text)

                # Q&A
                qa_data = []
                try:
                    all_questions = soup.find_all('div', class_='question-desc')

                    for q in set(all_questions):
                        question = q.findNext('h2').text
                        que_description = q.findNext('p', {'class': 'text question-body'}).text
                        answer = q.findNext('p', {'class': 'text question-body'}).findNext('p', class_='text').text
                        que_date = q.findNext('p', class_='date').text
                        qa_data.append(
                            [
                                list(data.keys())[0].split('szakember/')[-1],
                                question,
                                que_description,
                                answer,
                                que_date,
                            ]
                        )
                except:
                    pass
            # print(que_date)
            # print(answer)

                with open('data/main_info.csv', 'a', encoding='utf-8') as main_file:
                    writer = csv.writer(main_file)
                    writer.writerow(
                        [list(data.keys())[0], meta_title, name, location, top_categories, all_categories, job_order, review, rating, activity, description, avatar, fogadok_munkat, phone]
                    )
                with open('data/review.csv', 'a', encoding='utf-8') as review_file:
                    writer1 = csv.writer(review_file)
                    writer1.writerows(
                        review_data
                    )
                    review_data.clear()
                with open('data/prices.csv', 'a', encoding='utf-8') as price_file:
                    writer2 = csv.writer(price_file)
                    writer2.writerows(
                        prices_data
                    )
                    prices_data.clear()
                with open('data/Q&A.csv', 'a', encoding='utf-8') as qa_file:
                    writer2 = csv.writer(qa_file)
                    writer2.writerows(
                        qa_data
                    )
                    qa_data.clear()
                    print(f'Сохранено для ссылки {list(data.keys())[0]}')

            except:
                with open('not_saved_links_v2.txt', 'a', encoding='utf-8') as not_saved_file:
                    not_saved_file.writelines(f'{list(data.keys())[0]}')
                    print(f'[BAD URL] --- {list(data.keys())[0]}')
        with open('not_saved_links.txt', 'w', encoding='utf-8') as f2:
            f2.writelines(lines[link_idx + 10: len(lines)])
            f2.close()
        with open('not_saved_links.txt', 'r', encoding='utf-8') as f3:
            lines = f3.readlines()
        pages_data.clear()
        for url in bad_urls:
            with open('not_saved_links_v2.txt', 'a', encoding='utf-8') as fff:
                fff.writelines(f'{url}\n')
        bad_urls.clear()
        time.sleep(3)


