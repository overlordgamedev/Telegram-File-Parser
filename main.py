import io
import os
import shutil
import zipfile
import rarfile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from flask import Flask, render_template, send_file
from flask_sqlalchemy import SQLAlchemy
from pyrogram import Client
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler  # Импортируем планировщик
import json

import progressbar
from aiogram import Bot

time_started = datetime.now()  # Дата для фильтрации только свежих логов с момента старта панельки

host = "http://127.0.0.1:5000"  # ссылка на хост, что бы потом повесить на кнопку телеграмм в сообщение для скачивания лога

TOKEN = '7922452913:AAHfMlQgtQSOcGKYvxyC0NHBFqE_dZelyxE'  # Токен бота

bot = Bot(token=TOKEN)  # Инициализирует бота с использованием токена
admins_tg_id = [6792787334, 7482303961]  # id для рассылки в телегу

rarfile.UNRAR_TOOL = os.path.abspath(r'WinRAR\UnRAR.exe')  # Путь до unrar.exe

folder_extractions = "extraction"  # папка для разархивации
folder_zipper_panel = "zipper_panel"  # папка для архивации
max_size = 15728640
channels = [
    "@channel"]  # каналы откуда качать файлы

# Конфигурация Flask и базы данных
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///files.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Настройка Pyrogram
api_id = 52428800  # API ID
api_hash = "5598d5435dcb722edef1754f35ee7561"  # API Hash
session_string = "AgFow5gAuKtrxl9t6OaK510lD6NgmU4Ep6D2CdSLelvEtKI32fsED_oGwLr5jFN2HCSwpWbsedSdvHC6zDmvTZ1VmvUZ-lFPTFO4ZqNXN3OHXX2hFKBw9aEmd4y6IPtZS65HDNGvvsSfv2yOc2qNDrvvLk9BrBKZ1qbbY3marOEzVKnyDnTICGebubVt1X7KQujQn9kMrwiPajWAC1KiH-OU_4EFM0-rz1oWHqZCmaiLMelOGEVmCAZPsb5Zwtt6Bm2YDfete-W7VR0adfjuOm9v-gE612WhkU3nZ26-DAZEE8BFcmZhU2Gdm0ThdBW2TjOSf2FNe9uYCKOh9Psy7cIO7G4nMQAAAAGYY-VKAA"
tg_client = Client(name="my_session", session_string=session_string, api_id=api_id, api_hash=api_hash)

with open("config.json", 'r') as file:
    config = json.load(file)


# Модель базы данных для хранения информации о скачанных файлах
class FileRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    file_id = db.Column(db.String, unique=True, nullable=False)
    file_name = db.Column(db.String, nullable=False)
    file_size = db.Column(db.Integer, nullable=False)
    download_date = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<FileRecord {self.file_name} ({self.file_size} bytes)>'


# Модель базы данных для хранения информации о папках и дополнительных данных
class FolderRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    logs_id = db.Column(db.Integer, db.ForeignKey('logs.id'), nullable=False)  # Внешний ключ на Logs
    file_record_id = db.Column(db.Integer, db.ForeignKey('file_record.id'),
                               nullable=False)  # Внешний ключ на FileRecord
    folder_name = db.Column(db.String, nullable=False)  # Название папки
    rel_path = db.Column(db.String, nullable=True)  # Столбец для относительного путя до файла

    def __repr__(self):
        return f'<FolderRecord {self.folder_name}, txt: {self.txt}, folder: {self.folder}>'


class Logs(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    file_record_id = db.Column(db.Integer, db.ForeignKey('file_record.id'), nullable=False)
    folder_name = db.Column(db.String, nullable=False)
    count_param = db.Column(db.String)
    full_path = db.Column(db.String)
    folder_records = db.relationship('FolderRecord', backref='log', lazy=True)


# Создаем таблицы
with app.app_context():
    db.create_all()


async def extract_archive(file_path, dest_folder, file_record_id):
    os.makedirs(dest_folder, exist_ok=True)

    # Определение типа архива zip или rar и его распаковка
    try:
        if file_path.endswith('.zip'):
            with zipfile.ZipFile(file_path, 'r') as archive:
                archive.extractall(dest_folder)
        elif file_path.endswith('.rar'):
            with rarfile.RarFile(file_path, 'r') as archive:
                archive.extractall(path=dest_folder)
        else:
            raise ValueError("Неподдерживаемый формат файла. Поддерживаются только zip и rar.")
    except (rarfile.BadRarFile, rarfile.PasswordRequired, rarfile.NeedFirstVolume, rarfile.RarCannotExec) as e:
        print(f"Ошибка с файлом: {e}")
        return

    # Проверка на .txt что бы удостоверится что это не 1 лог в архиве, если надо 1 лог в архиве создаем папку что бы не дописывать логику
    if bool([f for f in os.listdir(dest_folder) if f.endswith('.txt')]):
        # Создаем новую папку внутри dest_folder с уникальным именем (без расширения .zip или .rar)
        archive_name = os.path.splitext(os.path.basename(file_path))[0]  # получаем имя для папки убирая .zip/.rar
        new_folder_path = os.path.join(dest_folder, archive_name)  # новый путь до лога

        # Перемещаем все файлы и папки из dest_folder в новую папку
        for item in os.listdir(dest_folder):
            old_path = os.path.join(dest_folder, item)
            new_path = os.path.join(new_folder_path, item)

            # Проверяем, что не пытаемся переместить папку в саму себя
            if old_path != new_path:
                # Убедимся, что новая папка не существует, иначе пропустим перемещение
                if not os.path.exists(new_folder_path):
                    os.makedirs(new_folder_path)
                # Если это файл, перемещаем его
                if os.path.isfile(old_path):
                    shutil.move(old_path, new_path)
                # Если это директория, перемещаем всю директорию
                elif os.path.isdir(old_path):
                    shutil.move(old_path, new_path)

    # Загрузка ключевых слов из config.json
    with open("config.json", 'r') as file:
        keywords = json.load(file).get("name_file", [])

    # Обработка файлов и создание записей в БД
    for dir_log in os.listdir(dest_folder):
        count_list = []  # Массив для счетчика с нашими параметрами из конфига
        for keyword in keywords:
            # Подсчитаем количество файлов и папок, содержащих ключевое слово в названии
            count = 0
            for dirpath, dirnames, filenames in os.walk(os.path.join(dest_folder, dir_log)):
                # Проверка в самой папке (dirpath) — если название папки содержит ключевое слово
                if keyword in os.path.basename(dirpath):
                    count += 1
                # Проверка в папках (dirnames)
                if any(keyword in dirname for dirname in dirnames):
                    count += 1
                # Проверка в файлах (filenames)
                if any(keyword in filename for filename in filenames):
                    count += 1
            count_list.append(str(count))  # добавляем этот счетчик в архив

        # Добавление записи в таблицу Logs
        log = Logs(full_path=os.path.abspath(os.path.join(dest_folder, dir_log)), folder_name=dir_log,
                   file_record_id=file_record_id, count_param=",".join(count_list))
        db.session.add(log)
        db.session.commit()
        log_id = log.id  # Свежий созданный id

        # Кнопка скачивания для удобства
        ikb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=f"Скачать", url=f"{host}/download_folder/{log_id}"),
                ],
            ])

        # Текст
        text = (f"Название лога {dir_log}\n"
                f"Фильтры: {keywords}\n"
                f"Найдено по фильтру: {",".join(count_list)}\n")

        # Отправляем все рассылку кто в списке admins_tg_id
        for admin_tg_id in admins_tg_id:
            await bot.send_message(chat_id=admin_tg_id, text=text, reply_markup=ikb)

        # Добавление записей в таблицу FolderRecord
        for dirpath, _, filenames in os.walk(os.path.join(dest_folder, dir_log)):
            for filename in filenames:
                rel_path = os.path.relpath(os.path.join(dirpath, filename), dest_folder)
                folder_record = FolderRecord(
                    file_record_id=file_record_id,
                    folder_name=rel_path.split(os.sep)[0],
                    rel_path=rel_path,
                    logs_id=log_id
                )
                db.session.add(folder_record)
        db.session.commit()

    return dest_folder


# Функция для архивации папки
def archive_folder(folder_name):
    # Путь до папки, которую нужно архивировать
    folder_path = os.path.join(folder_extractions, folder_name)

    # Создаем архив в памяти
    archive_io = io.BytesIO()

    # Создаем архив zip
    with zipfile.ZipFile(archive_io, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Добавляем все файлы из папки в архив
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                arcname = os.path.relpath(file_path, folder_path)  # относительный путь
                zipf.write(file_path, arcname)

    archive_io.seek(0)  # Перемещаем курсор в начало, чтобы отдать файл на скачивание
    return archive_io


@app.route("/download_folder/<int:log_id>")
def download_folder(log_id):
    # Получаем лог по ID
    log = Logs.query.get(log_id)
    if not log:
        return "Log not found", 404

    # Используем полный путь из столбца full_path
    folder_path = log.full_path
    print(folder_path)
    # Проверяем, существует ли папка
    if not os.path.exists(folder_path):
        return "Folder not found", 404

    # Генерируем имя архива на основе имени папки
    folder_name = os.path.basename(folder_path)  # Название папки из полного пути
    zip_filename = f"{folder_name}.zip"

    zip_filepath = os.path.join(folder_zipper_panel, zip_filename)  # путь где создание архива
    if not os.path.exists(folder_zipper_panel):
        os.makedirs(folder_zipper_panel)
    # Создание архива
    with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                zipf.write(file_path,
                           os.path.relpath(file_path, folder_path))  # Добавляем файл в архив с относительным путем

    # Отправляем архив в браузер
    return send_file(zip_filepath, as_attachment=True)


# Прогресс бар для красоты в консольку
async def progress(current, total, file_size=None, bar=None):
    """total в примере документации должен работать, но к сожелению сломан поэтому передаем свой параметр file_size за место total
    current текущее количество скачанных байтов, bar это то что мы инициализировали"""
    try:
        print(f"current: {current}")
        print(f"full_size: {file_size}")
        if current < file_size:  # Что бы избежать ошибки ZeroDivisionError и переполнения
            bar.update(current)  #  Обновляет анимацию загрузки bar
    except ZeroDivisionError:
        pass  # Игнорировать ошибку если скачано 0 байт


async def download_smallest_archive():
    try:
        async with tg_client:
            files_records = []
            for channel in channels:  # Получаем канал из нашего списка
                async for message in tg_client.get_chat_history(channel):  # Получаем историю сообщений
                    print(message)  # Отображения сообщения из канала
                    if message.document and message.document.mime_type in ["application/zip",
                                                                           "application/vnd.rar"]:  # Ищем только rar и zip архивы
                        file = message.document
                        # Проверка на дубли по имени файла, фильтрация по размеру файлов, а так же по свежести через сравнение даты
                        if FileRecord.query.filter_by(file_name=file.file_name,
                                                      file_size=file.file_size).first() or file.file_size > max_size or file.date < time_started:
                            continue

                        print(f"Файл качается {file.file_name}")
                        bar = progressbar.ProgressBar(maxval=file.file_size, widgets=['Loading: ',
                                                                                      progressbar.AnimatedMarker()]).start()  # Инициализируем и запускаем анимацию для скачивания далее передаем в функцию для обновления
                        # Функция для скачивания файлов указываем телеграмм id файла и другие параметры
                        file_path = await tg_client.download_media(file.file_id, file_name=file.file_name,
                                                                   progress=progress,
                                                                   progress_args=(file.file_size, bar))
                        # Записываем файлы скаченные
                        file_record = FileRecord(
                            file_id=file.file_id,
                            file_name=file.file_name,
                            file_size=file.file_size,
                            download_date=file.date.astimezone()
                        )
                        db.session.add(file_record)
                        db.session.commit()

                        files_records.append(file_record)
                        print(f"Файл скачался {file.file_name}")
                        # Функция для разархивирования, передается путь скаченного архива и его id
                        await extract_archive(file_path, f"{folder_extractions}/{os.path.basename(file_path)}",
                                              file_record.id)
                        print(f"Файл разархивировался {file.file_name}")
            return files_records
    except Exception as err:
        print(err)


# Функция для планирования скачивания
def scheduled_download():
    with app.app_context():  # Создаем контекст приложения
        tg_client.loop.run_until_complete(download_smallest_archive())  # Запускаем цикл ассинхронного выполнения


# Настроим планировщик задач
scheduler = BackgroundScheduler()
scheduler.add_job(
    scheduled_download,
    'interval',  # Обозначает что будет интервал это значит каждые сколько то времени
    max_instances=1,  # Только 1 задача за раз то есть не запускаются новые пока не закроются старые
    minutes=1,  # Запуск задачи каждую минуту
    next_run_time=datetime.now()  # Запуск задачи сразу после добавления
)
scheduler.start()


@app.route("/index")
def index():
    # Читаем параметры из config.json
    with open("config.json", 'r') as file:
        config = json.load(file)
    config_columns = config.get("name_file", [])

    # Извлекаем все записи из таблицы Logs
    all_logs = Logs.query.all()

    # Передаем записи и параметры в шаблон index.html
    return render_template('index.html', all_logs=all_logs, config_columns=config_columns)


# Запускаем сервер Flask
if __name__ == "__main__":
    try:
        app.run(debug=True, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()  # Дезентигрируем планировщик
