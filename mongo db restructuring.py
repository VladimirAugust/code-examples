# A program for multithreaded restructuring of a large database (MondoDB).
# For all records that have certain fields, reformats these fields
# (splits a full name into separate first and last names, etc.)
import gc
import math
import sys
import time
from multiprocessing import Pool

import config
from pymongo import UpdateOne
from pymongo.errors import InvalidOperation, WTimeoutError, ExecutionTimeout, NetworkTimeout
from utilities import logFromProcess, collectCollections

MB = 1048576


def extractName(record):
    try:
        names = record[config.FIO_KEY]
        names = names.strip().title().split()
        lastName = names[0] if len(names) > 0 else ""
        firstName = names[1] if len(names) > 1 else ""
        patronymicName = names[2] if len(names) > 2 else ""
    except BaseException:
        lastName = ""
        firstName = ""
        patronymicName = ""

    return firstName, lastName, patronymicName


def processInitializer(processFunction, requestsFunction, actionName, operationsFunction, filter):
    client = config.initializeMongoClient()
    processFunction.client = client
    processFunction.actionName = actionName
    processFunction.operationsFunction = operationsFunction
    processFunction.filter = filter
    requestsFunction.client = client


def prepareOperationsForChangingNamesVkIdTel(record):
    firstName, lastName, patronymicName = extractName(record)
    operations = [
        {'$set': {
            config.FIRST_NAME_KEY: firstName,
            config.LAST_NAME_KEY: lastName,
            config.PATRONYMIC_NAME_KEY: patronymicName
        }
        },
        {'$unset': config.FIO_KEY}
    ]
    if config.VK_ID_KEY in record:
        operations.append(prepareOperationsForChangingVkId(record))
    return operations


def prepareOperationsForChangingVkId(record):
    numbers, otherInformation = extractVkIdAndOtherInformation(record[config.VK_ID_KEY])
    operations = {
        '$set': {
            config.VK_ID_KEY: numbers,
            config.OTHER_INFORMATION_KEY: otherInformation
        }
    }
    return operations


def prepareRequests(data):
    client = prepareRequests.client
    actionName = prepareRequests.actionName
    operationsFunc = prepareRequests.operationsFunction
    filter = prepareRequests.filter
    db, collection, part, partsCount = data

    logFromProcess("Попытка получить данные ({}.{})".format(db, collection))
    try:
        cursor = client[db][collection].find(filter) \
            .skip(part * config.BULK_BATCH_SIZE) \
            .limit(config.BULK_BATCH_SIZE)
        mongoData = list(cursor)
    except (WTimeoutError, ExecutionTimeout, NetworkTimeout) as e:
        logFromProcess("Возникла ошибка тайм-аут ({}.{}): {}\nПовтор операции через {} сек".format(db, collection, e,
                                                                                                   config.PROCESS_TIMEOUT_SLEEP_TIME))
        time.sleep(config.PROCESS_TIMEOUT_SLEEP_TIME)
        prepareRequests(data)

    logFromProcess(
        "Получены данные ({5:.5f} мб)\nПодготовка запросов для изменения данных ({0}) ({1}.{2}, блок {3}/{4})".format(
            actionName, db, collection, part + 1, partsCount, sys.getsizeof(mongoData) / MB))

    requests = []
    for r in mongoData:
        operations = operationsFunc(r)
        if operations:
            requests.append(UpdateOne({"_id": r['_id']}, operations))

    client.close()
    return requests, db, collection, len(mongoData)


def makeBulkRequests(data):
    client = makeBulkRequests.client
    requests, db, collection, recordsCount = data
    collection = client[db][collection]
    if not requests:
        logFromProcess("Пустой массив запросов (нет документов для изменения)")
        return
    logFromProcess(
        "Отправка запросов на сервер ({}.{}) (размер отправляемых данных: {:.5f} мб)".format(db, collection,
                                                                                             sys.getsizeof(
                                                                                                 requests) / MB))
    try:
        result = collection.bulk_write(requests, ordered=False)
        logFromProcess(
            "Успешно обновлены {} записей (из {}) ({}.{})".format(result.modified_count, recordsCount, db, collection))
    except InvalidOperation as e:
        logFromProcess("Не были успешно обновлены {} записей\nОшибка: {}".format(recordsCount, e))
    except (WTimeoutError, ExecutionTimeout, NetworkTimeout) as e:
        logFromProcess("Возникла ошибка тайм-аут ({}.{}): {}\nПовтор операции через {} сек".format(db, collection, e,
                                                                                                   config.PROCESS_TIMEOUT_SLEEP_TIME))
        time.sleep(config.PROCESS_TIMEOUT_SLEEP_TIME)
        makeBulkRequests(data)
    finally:
        client.close()


def extractVkIdAndOtherInformation(vkId):
    if type(vkId) == int:
        return vkId, ""
    numbers = ""
    otherInformation = ""
    for pos, c in enumerate(vkId):
        if c.isdigit():
            numbers += c
        else:
            otherInformation = vkId[pos:]
            break

    return int(numbers), otherInformation.strip()


def processCollections(client, collections, filter, operationsFunction, operationName):
    for db, collection in collections:
        timeForCollection = time.time()
        threadData = []
        print("Начало работы с коллекцией {}.{}".format(db, collection))
        print("Подсчёт количества докуметов для изменения: ", end='')
        documentsCount = client[db][collection].count_documents(filter)
        print("найдено {} записей для изменения".format(documentsCount))

        if (documentsCount == 0):
            continue
        count = math.ceil(documentsCount / config.BULK_BATCH_SIZE)
        threadData.extend((db, collection, i, count) for i in range(count))

        print("Запуск потоков")

        with Pool(config.PROCESSES_POOL_SIZE, processInitializer,
                  (prepareRequests, makeBulkRequests, operationName, operationsFunction, filter)) as pool:
            print("Подготовка запросов для обновление записей")
            requests = pool.map(prepareRequests, threadData)
            print("Отправка запросов")
            pool.map(makeBulkRequests, requests)

        print("Работа над коллекцией {}.{} заняла {:2f} сек".format(db, collection, time.time() - timeForCollection))
        gc.collect()


if __name__ == '__main__':
    try:
        print("Подключение к MongoDB...")
        client = config.initializeMongoClient()
        collections = collectCollections(client)
    except Exception as e:
        print("Ошибка подключения:", e)
        exit(1)

    startTime = time.time()

    # часть 1
    print("Поиск и изменение записей для изменения ФИО и Vk ID (если присутствует)")
    filter = {config.FIO_KEY: {"$exists": True}}
    processCollections(client, collections, filter, prepareOperationsForChangingNamesVkIdTel, "изменение ФИО, Vk Id ")

    # часть 2: поиск и изменение только записей в которых всё ещё поле Vk неформатировано
    print("Поиск и изменение записей для изменения Vk ID")
    filter = {config.VK_ID_KEY: {"$exists": True},
              config.OTHER_INFORMATION_KEY: {"$exists": False}}
    processCollections(client, collections, filter, prepareOperationsForChangingVkId, "изменение Vk Id")

    print("Программа работала ", time.time() - startTime, " секунд")
