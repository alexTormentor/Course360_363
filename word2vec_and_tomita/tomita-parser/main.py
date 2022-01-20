import json
import os
import subprocess as sub
import sys

sys.path.append("/home/user/ReverseParser/")
from pymongo import MongoClient

_id = 0
_id2 = 0
page_num = 0
                                                                                                                        # проверка и подключение к монгоБД
try:
    print('conected..')

    client = MongoClient('mongodb+srv://AlexB:456@cluster0.g7ol3.mongodb.net/ArticlesDB?retryWrites=true&w=majority')   # подключение к БД
    db = client["News"]                                                                                                 # получение данных бд
    collection = db.news                                                                                                # сбор информации из коллекции
    tonality = db.newsTomitaDB                                                                                          # тональности
    articlesDB = db.ArticlesDB                                                                                          # статьи в БД
                                                                                                                        # удаление и вывод кол-ва удалённого контента
    tomitaDel = db.newsTomitaDB.delete_many({})
    articleDel = db.ArticlesDB.delete_many({})
    print(tomitaDel.deleted_count, "tonality deleted!")
    print(articleDel.deleted_count, "articles deleted!")

except:
    print('error')                                          # в случае, если соединение не удалось - сообщаем об ошибке


def facts_for_reload(name, array: list, text):              # сохранение объектов в БД
    factInside = False                                      # сначаланичего внутри нет
    content = ''
    for object in array:                                    # идём по массиву
        if object == name:                                  # если есть объект
            factInside = True                               # то подтверждаем это
        elif object == "{":                                 # если встречаем фигурную скобку, то идём дальше
            continue
        elif object == "}":
            factInside = False                              # говорим что ничего нет, если увидели конец
        else:                                               # если же нет
            position = object.find("=")                     # то записываем, где нашли символ равенства
            if position > 0:                                # если не в начале
                left, right = object.split(" = ")           # выравниваем позицию слов
            if position <= 0:                               # если где-то в другом месте
                content = object                            # записываем объект
                content = content.lower()                   # смена регистра
            if factInside and content:                      # если имеются все элементы
                right = right.lower()                       # смена регистра
                rightnew = right.replace(' ', '_')          # вставляем подчёркивание
                rightnew = rightnew.replace('\"', '')       # убираем лишние элементы
                content = content.replace(right, rightnew)  # заменяем старый элемент
                if name == 'Person':                        # если это персона
                    DBSaveFunc(rightnew, '', content, text) # записываем в БД
                if name == 'Place':                         # если место
                    DBSaveFunc('', rightnew, content, text) # записываем в БД
                text = text.lower()                         # смена регистра
                text = text.replace(right, rightnew)        # обновление текста
                DBSaveArticle(text)                           # сохранение текста в статью


def DBSaveFunc(person, place, content, text):                                                               # сохранение в БД
    global _id                                                                                              # ID глобальный(для записей)
    _id = _id + 1                                                                                           # итерация АйДи
    data = {'Person': person, 'Place': place, 'Sentence': content, 'Text': text, 'originalID': article_id}  # формирование строки
    print(data)                                                                                             # вывод
    tonality.update_one({"Id": _id}, {"$set": data}, upsert=True)                                           # обновление тональности
    data2 = {'people': person, 'places': place}                                                             # запись персон и мест
    collection.update_one({"_id": _id2+1}, {"$set": data2}, upsert=True)                                    # обновление коллекции


def DBSaveArticle(text):                                                # сохранени в БД статьи
    global _id2                                                         # глобальный АйДи
    _id2 = _id2 + 1                                                     # итерация АйДи
    data = {'Текст': text}                                              # запись текста
    articlesDB.update_one({"Id": _id2}, {"$set": data}, upsert=True)    # обновление списка статей



def mainFunc(records: list):                                                    # основная функция
    with open('/home/user/ReverseParser/textForCourseProject.txt', 'w') as log: # открытие файла на запись
        appending = False                                                       # содержится ли упоминание интересующих объектов
        person = ''                                                             # персона
        place = ''                                                              # достопримечательность
        for record in records:                                                  # записи - пробег по всем
            global page_num                                                     # страница для пролистывания программой
            global article_id                                                   # айди статьи
            page_num = page_num + 1                                             # итерация
            print('обработка статьи: ' + str(page_num))                         # какую статью смотрим
            text = record['text']                                               # что внутри
            article_id = record['_id']                                          # ее айди
            out = TomitaGO(text)                                                # вызов томиты для текста
            for obj in out:                                                     # для всех эл. смотрим - это персона или нет
                log.write("%s\n" % obj)                                         # запись в текстовый файл
            if "Person" in out:                                                 # если видим персону
                appending = True                                                # нашли что надо
                print('success for person')                                     # сообщение об успехе
                facts_for_reload('Person', out, text)                           # загружаем данные
                print(person)                                                   # печатаем кого нашли
                print('\n')                                                     # перенос строки
            elif "Place" in out:                                                # если же нашли место
                appending = True                                                # то же самое
                print('success for place')                                      # то же самое
                facts_for_reload('Place', out, text)                            # снова...
                print(place)                                                    # еще раз..
                print('\n')                                                     # туда же..
            else:                                                               # если нечего найти
                DBSaveArticle(text)                                             # заканчиваем и сохраняем запись
    log.close()                                                                 # закрываем путь


def TomitaGO(txt):                                                                          # запуск томиты
    if os.path.split(os.getcwd())[-1] != "/home/user/ReverseParser/TomitaParser":           # проверка есть ли папка
        os.chdir("/home/user/ReverseParser/TomitaParser")                                   # путь до томиты
    with open(os.path.join(os.getcwd(), 'input.txt'), 'w', encoding='utf-8') as inputFile:  # открываем файл с текстом на запись
        inputFile.writelines(txt)                                                           # записываем
    output = []                                                                             # переменная для результата
    with open('/home/user/ReverseParser/TomitaParser/output.txt', 'r',
              encoding='utf-8') as outputFile:                                              # открываем результативный файл на чтение
        buffer = ''                                                                         # буффер
        whatNew = outputFile.readlines()                                                    # считываем новость
        appending = False                                                                   # ничего не меняем
        savebuffer = True                                                                   # но сохраняем
        for line in whatNew:                                                                # идём по новости
            if "Person" in line or "Place" in line:                                         # если нашли что хотели
                savebuffer = False                                                          # не сохраняем
                appending = True                                                            # но изменяем контент
                buffer = takeSomeInEnd(buffer)                                              # записываем последнее
                output.append(buffer.strip())                                               # прикрепляем буффер
            if appending:                                                                   # если меняем
                output.append(line.strip())                                                 # просто крепим строки
            if savebuffer:                                                                  # если сохраняем
                buffer = buffer + line                                                      # + строка
            if "}" in line:                                                                 # если у нас закончилась новость
                appending = False                                                           # менять не надо
                buffer = ' '                                                                # но чистим буффер
                savebuffer = True                                                           # но этот флаг тоже сбрасываем

    return output                                                                           # результат


def SplitContent(content):                      # разбивка текста
    import re
    result = re.split("\. |\.\.\. ", content)   # все разбивки и контент
    return result                               # результат


def takeSomeInEnd(txt):                 # взятие последнего элемента
    content = SplitContent(txt)         # трансформируем содержимое
    i = 0                               # итерация
    for _ in content:                   # проходим по контенту
        i = i + 1                       # +1 итерация
        if i == 1:                      # если мы на 1
            last_element = content[-1]  # получаем что нужно
        elif i == 0:                    # если же не было движений
            last_element = " "          # обнуляемся
        else:                           # если никаких движений сделать не получилось
            last_element = content[-2]  # просто смещаем элемент
    return last_element                 # результат


def getDataDBs(mongo):              # получаем БД
    return mongo.selectAll("news")  # ищем новости(коллекцию)


if __name__ == '__main__':
    Records = collection.find() # статьи и их данные(первым делом это)
    mainFunc(Records)           # начало работы
