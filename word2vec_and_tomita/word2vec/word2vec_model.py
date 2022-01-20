PATH = '/home/user/Word2Vec_KL_CourseProject/TxtFiles/*.txt'
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.ml.feature import Word2Vec
import re
import string
import string
import datetime


def remove_punctuation(text):
    """
    Удаление пунктуации из текста
    """
    return text.translate(str.maketrans('', '', string.punctuation))


def remove_linebreaks(text):
    """
    Удаление разрыва строк из текста
    """
    return text.strip()


def get_only_words(tokens):
    """
    Получение списка токенов, содержащих только слова
    """
    return list(filter(lambda x: re.match('[а-яА-я]+', x), tokens))


def create_w2v_model():
    spark = SparkSession \
        .builder \
        .appName("Word2VecApplication") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.memory.offHeap.enabled", True) \
        .config("spark.memory.offHeap.size", "2g") \
        .getOrCreate()

    input_file = spark.sparkContext.wholeTextFiles(PATH)

    print("""

        Подготовка данных (1)...

        """)
    prepared_data = input_file.map(lambda x: (x[0], remove_punctuation(x[1])))

    print("""

        Подготовка данных (2)...

        """)
    df = prepared_data.toDF()

    print("""

        Подготовка данных (3)...

        """)
    prepared_df = df.selectExpr('_2 as text')

    print("""

        Разбитие на токены...

        """)
    tokenizer = Tokenizer(inputCol='text', outputCol='words')
    words = tokenizer.transform(prepared_df)

    print("""

        Очистка от стоп-слов...

        """)
    stop_words = StopWordsRemover.loadDefaultStopWords('russian')
    remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
    filtered = remover.transform(words)
    print("""

        Вычисление значение TF...

            """)
    vectorizer = CountVectorizer(inputCol='filtered', outputCol='raw_features').fit(filtered)
    featurized_data = vectorizer.transform(filtered)
    featurized_data.cache()
    # vocabulary = vectorizer.vocabulary
    # print("""
    #
    #     Вывод на экран словаря...
    #
    #     """)
    # print(vocabulary)

    idf = IDF(inputCol='raw_features', outputCol='features')
    idf_model = idf.fit(featurized_data)
    rescaled_data = idf_model.transform(featurized_data)

    # Вывести таблицу rescaled_data
    print("rescaled_data")
    rescaled_data.show()

    print("""

        Построение модели...

        """)

    print("Word2vec")
    word2Vec = Word2Vec(vectorSize=2, minCount=0, inputCol='words', outputCol='result')
    model = word2Vec.fit(filtered)
    w2v_df = model.transform(filtered)
    w2v_df.show()
    w2v_model = word2Vec.fit(words)

    print("""

        Сохранение модели...

        """)
    today = datetime.datetime.today()
    model_name = today.strftime("word2vec_model/")

    print("""

        Model  """ + model_name + """  saved

        """)




    w2v_model.save(model_name)
    print("\n\n")

    spark.stop()
