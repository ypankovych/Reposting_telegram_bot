import telebot
import requests
import pytz
import datetime
import threading
import vk
from time import sleep

# Токен для доступа к Telegram боту
token = ''
bot = telebot.TeleBot(token)
# Ссылка на файл в котором расположена конфигурация
configuration_link = ''
cache_file = "old_links.txt"
# Авторизация Вконтакте (пользовательские данные не требуются)
api_object = vk.API(session=vk.Session())
# DO NOT MODIFY
io_lock = threading.RLock()


def get_memory_cache():
    with io_lock:
        with open(cache_file) as cache:
            return list(map(str.strip, cache.readlines())) 


def io_file(link):
    # Запись в файл использованных ссылок
    with io_lock:
        with open(cache_file, mode='a') as cache:
            try:
                cache.write('{}\n'.format(link))
            except IOError as Error:
                print(Error)


def get_time():
    # Нормализировать время под МСК
    time_zone = pytz.timezone('UTC')
    time_now = datetime.datetime.now(tz=time_zone)
    delta = datetime.timedelta(hours=3)
    current_time = time_now + delta
    return '{}:{}'.format(current_time.hour, current_time.minute)


def get_channels_configuration(channel_name=None):
    # Эта функция возвращает конфигурицию для конкретного канала если он указан
    # Иначе будет возвращена конфигурация для всех каналов
    if channel_name is not None:
        return eval(requests.get(configuration_link).text)[channel_name]
    return eval(requests.get(configuration_link).text)


def get_post(post_info):
    # Эта функция возвращает информацию о посте
    post_id = post_info.split('wall')[1]
    return api_object.wall.getById(posts=post_id)


def channels_handler(channel_name):
    # Это основная функция которая контролирует постинг по времени
    old_time = None
    while True:
        reload_cache_file(open(cache_file).readlines())
        posts_links = get_channels_configuration(channel_name)
        current_time = get_time()
        if current_time in posts_links['time'] and current_time != old_time:
            print('Пришло время для постинга: {}, в канал: {}'.format(current_time, channel_name))
            old_time = current_time
            memory_cache = get_memory_cache()
            for i in requests.get(posts_links['list']).text.split():
                if i.strip() not in memory_cache:
                    io_file(i)
                    try:
                        types_handler(get_post(i), channel_name, disable=posts_links['time'][current_time])
                        break
                    except Exception as err:
                        print(err)
        # Не менять это значение. Возможны повторения постов!
        sleep(5)


def types_handler(post, channel_name, disable=0):
    # Эта функция обрабатывает типы вложений в постах, после чего постит их
    # Если будет найден текст, он будет выслан последним
    if 'attachments' in post[0]:
        assert len(post[0]['attachments']) == 1
        for i in post[0]['attachments']:
            if i['type'] == 'doc':
                bot.send_document(channel_name, i['doc']['url'], disable_notification=disable, caption=post[0]['text'])
            elif i['type'] == 'photo':
                bot.send_photo(channel_name, i['photo']['src_big'], disable_notification=disable, caption=post[0]['text'])
    else:
        if 'text' in post[0]:
            if post[0]['text']:
                bot.send_message(channel_name, post[0]['text'], disable_notification=disable)


def main():
    # Эта функция запускает процесс для каждого канала в отдельном потоке
    for channel in get_channels_configuration():
        thread = threading.Thread(target=channels_handler, args=(channel,))
        thread.start()


def reload_cache_file(file):
    # Если в файле накопилось больше 1000 ссылок
    # Файл будет очищен и в него будет перезаписано последние 500 ссылок
    # для избежания последующих повторов
    if len(file) >= 1000:
        with open(cache_file, mode='w+') as f:
            # Заменить 500 на желаемое кол-во ссылок для отрезания
            dump = reversed(file)[:500]
            f.write('\n'.join(dump))

if __name__ == '__main__':
    main()
