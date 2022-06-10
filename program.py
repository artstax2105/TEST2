from include.s3 import S3
from os import walk
from pathlib import Path
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import datetime as dt
import re
import os
import sys

# импорт необходимых библиотек

load_dotenv()  # взяли переменные окружения их env
MIN_VIDEO_AGE = 1
s3 = S3()


def get_dir_items(path, is_folders=True):  # фунция получения элементов каталога
    return next(walk(path), (None, None, []))[1 if is_folders else 2]


def get_files_list_to_upload(root_path):  # создаем функцию для обновления файлового списка
    files_to_upload = []  # создаем список
    cameras_folders = get_dir_items(root_path, True)  # переменной присваиваем элементы дирекории
    for cameras_folder in cameras_folders:  # цикл на наличие переменной cameras_folder
        camera_id = cameras_folder[7:]  # camera_id присваиваем элементы списка начиная с 7-го

        videos_list = get_dir_items(root_path + cameras_folder, False)
        for video_file in videos_list:  # цикл по видеофайлам
            result = re.findall(r'\d{1,2}_\d{1,2}_\d{4}', video_file)  # результат= непересекающиеся значения в словарях
            date = result[0] if len(result) > 0 else False  # data= 0-й элеменнт result  если длина резалт больше 0
            if not date:
                continue  # если дата отсутствует, продолжить

            date = dt.datetime.strptime(date, '%d_%m_%Y')  # строка преобразована в формат даты и времени
            date = date.date()  # data преобразовано в формат .data

            video_age = (dt.datetime.today().date() - date).days  # узнаём сколько дней назад была сделана запись
            if video_age < MIN_VIDEO_AGE:
                continue  # проверка video_age  на соответствие минимальному

            files_to_upload.append({
                "filename": video_file,
                "camera_id": camera_id,
                "date": date,
                "path": root_path + cameras_folder + os.sep + video_file
            })  # добавляем данные (название видео, ид камеры, путь)
    return files_to_upload


def upload_video_to_s3(video):  # функция по загрузке
    bucket = os.getenv('YANDEX_S3_BUCKET')  # bucket= значение ключа 'YANDEX_S3_BUCKET'
    key = "videos/camera_" + video['camera_id'] + "/" + video['filename']  # создание пути ключа
    s3.upload_file(bucket, key, video['path'])  # загрузка значения ключа, ключа и пути

    os.remove(video['path'])  # удаление старого файла


def start_uploading():
    files_to_upload = get_files_list_to_upload(
        "/app/media/full/")  # присвоение files_to_upload файла из списка для загрузки
    for file_to_upload in files_to_upload:  # пока имеются файлы для загрузки
        upload_video_to_s3(file_to_upload)  # вызывается функция загрузки для файла


if len(sys.argv) >= 2 and sys.argv[1] == '--force':  # проверка аргументов
    print('force upload videos to Yandex S3')  # вывод сообщения о загрузке
    start_uploading()  # повторная загрузка( рекурсивная)
else:
    print('Scheduler started')  # вывод сообщения о запуске планировщика
    scheduler = BlockingScheduler()
    scheduler.add_job(
        start_uploading,  # начать загрузку
        CronTrigger.from_crontab('0 1 * * *')  # каждую минуту
    )
    scheduler.start()  # запуск планировщика
