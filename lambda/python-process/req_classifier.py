# Code needed by the model `code` used in classify_reqs_camara.py

from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.svm import LinearSVC

# Vamos eliminar nomes de ministros dos textos:
ministros = ['augusto', 'heleno', 'ribeiro', 'pereira', 'sérgio', 'fernando', 'moro', 
             'paulo', 'roberto', 'nunes', 'guedes', 'carlos', 'alberto', 'dos', 'santos', 
             'cruz', 'osmar', 'terra', 'marcos', 'cesar', 'pontes', 'tarcísio', 'gomes', 
             'de', 'freitas', 'luiz', 'henrique', 'mandetta', 'mauro', 'campbell', 'marques', 
             'ricardo', 'vélez', 'rodríguez', 'marcelo', 'álvaro', 'antônio', 'bento', 'costa', 
             'lima', 'leite', 'de', 'albuquerque', 'júnior', 'ricardo', 'de', 'aquino', 'salles', 
             'wellington', 'moreira', 'franco', 'ernesto', 'henrique', 'fraga', 'araújo', 'audo', 
             'araújo', 'faleiro', 'tereza', 'cristina', 'corrêa', 'da', 'costa', 'dias', 'onyx', 
             'dornelles', 'lorenzoni', 'rogério', 'simonetti', 'marinho', 'abraham', 'bragança', 
             'de', 'vasconcellos', 'weintraub', 'fernando', 'azevedo', 'e', 'silva', 'damares', 
             'regina', 'alves', 'marcelo', 'henrique', 'teixeira', 'dias', 'wagner', 'de', 'campos', 
             'rosário', 'luiz', 'eduardo', 'ramos', 'baptista', 'pereira', 'jorge', 'antônio', 'de', 
             'oliveira', 'francisco', 'andré', 'luiz', 'de', 'almeida', 'mendonça', 'roberto', 'de', 
             'oliveira', 'campos', 'neto', 'walter', 'souza', 'braga', 'netto']

# Create stopwords list:
nltk_stopwords = ['de', 'a', 'o', 'que', 'e', 'é', 'do', 'da', 'em', 'um', 'para', 'com', 'não', 'uma', 
                  'os', 'no', 'se', 'na', 'por', 'mais', 'as', 'dos', 'como', 'mas', 'ao', 'ele', 'das', 
                  'à', 'seu', 'sua', 'ou', 'quando', 'muito', 'nos', 'já', 'eu', 'também', 'só', 'pelo', 
                  'pela', 'até', 'isso', 'ela', 'entre', 'depois', 'sem', 'mesmo', 'aos', 'seus', 'quem', 
                  'nas', 'me', 'esse', 'eles', 'você', 'essa', 'num', 'nem', 'suas', 'meu', 'às', 'minha', 
                  'numa', 'pelos', 'elas', 'qual', 'nós', 'lhe', 'deles', 'essas', 'esses', 'pelas', 'este', 
                  'dele', 'tu', 'te', 'vocês', 'vos', 'lhes', 'meus', 'minhas', 'teu', 'tua', 'teus', 'tuas', 
                  'nosso', 'nossa', 'nossos', 'nossas', 'dela', 'delas', 'esta', 'estes', 'estas', 'aquele', 
                  'aquela', 'aqueles', 'aquelas', 'isto', 'aquilo', 'estou', 'está', 'estamos', 'estão', 
                  'estive', 'esteve', 'estivemos', 'estiveram', 'estava', 'estávamos', 'estavam', 'estivera', 
                  'estivéramos', 'esteja', 'estejamos', 'estejam', 'estivesse', 'estivéssemos', 'estivessem', 
                  'estiver', 'estivermos', 'estiverem', 'hei', 'há', 'havemos', 'hão', 'houve', 'houvemos', 
                  'houveram', 'houvera', 'houvéramos', 'haja', 'hajamos', 'hajam', 'houvesse', 'houvéssemos', 
                  'houvessem', 'houver', 'houvermos', 'houverem', 'houverei', 'houverá', 'houveremos', 
                  'houverão', 'houveria', 'houveríamos', 'houveriam', 'sou', 'somos', 'são', 'era', 'éramos', 
                  'eram', 'fui', 'foi', 'fomos', 'foram', 'fora', 'fôramos', 'seja', 'sejamos', 'sejam', 
                  'fosse', 'fôssemos', 'fossem', 'for', 'formos', 'forem', 'serei', 'será', 'seremos', 'serão', 
                  'seria', 'seríamos', 'seriam', 'tenho', 'tem', 'temos', 'tém', 'tinha', 'tínhamos', 'tinham', 
                  'tive', 'teve', 'tivemos', 'tiveram', 'tivera', 'tivéramos', 'tenha', 'tenhamos', 'tenham', 
                  'tivesse', 'tivéssemos', 'tivessem', 'tiver', 'tivermos', 'tiverem', 'terei', 'terá', 
                  'teremos', 'terão', 'teria', 'teríamos', 'teriam']
stopwords = list(set(nltk_stopwords + ministros))


def lowercase_f(x):
    """
    Returns all strings in a Pandas Series `x` in lowercase.
    """
    return x.str.lower()

def remove_punctuation_f(x):
    """
    Returns all strings in a Pandas Series `x` with punctuations removed.
    """
    return x.str.translate(str.maketrans('', '', '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'))

def remove_stopwords_f(x, stopwords):
    """
    Returns all strings in a Pandas Series `x` with words in list `stopwords` removed.
    """
    def rm_words(text):
        word_list = text.split()
        word_list = [word for word in word_list if not word in set(stopwords)]
        return ' '.join(word_list)

    return x.apply(rm_words)

def keep_only_letters_f(x):
    """
    Returns all strings in a Pandas Series `x` with all characters that are not letters removed.
    """
    return x.str.replace('[^a-z A-ZÁÂÃÀÉÊÍÔÓÕÚÜÇáâãàéêíóôõúüç]', '')


def create_tested_model():

    # Preprocessing:
    lowercase = FunctionTransformer(lowercase_f)
    remove_punctuation = FunctionTransformer(remove_punctuation_f)
    remove_stopwords = FunctionTransformer(remove_stopwords_f, kw_args={'stopwords': stopwords})
    keep_only_letters = FunctionTransformer(keep_only_letters_f)

    vectorizer = CountVectorizer(lowercase=False, binary=True, ngram_range=(1,2), max_df=0.6)
    classifier = LinearSVC(C=1)
    pipeline   = Pipeline([('lowercase', lowercase),
                           ('rm_punct',  remove_punctuation),
                           ('rm_words',  remove_stopwords),
                           ('az_only',   keep_only_letters),
                           ('w2v',       vectorizer),
                           ('classif',   classifier)])

    return pipeline


def create_prod_model():

    # Preprocessing:
    lowercase = FunctionTransformer(lowercase_f)
    remove_punctuation = FunctionTransformer(remove_punctuation_f)
    remove_stopwords = FunctionTransformer(remove_stopwords_f, kw_args={'stopwords': stopwords})
    keep_only_letters = FunctionTransformer(keep_only_letters_f)

    vectorizer = CountVectorizer(lowercase=False, binary=True, ngram_range=(1,2), max_df=0.3)
    classifier = LinearSVC(C=0.3)
    pipeline   = Pipeline([('lowercase', lowercase),
                           ('rm_punct',  remove_punctuation),
                           ('rm_words',  remove_stopwords),
                           ('az_only',   keep_only_letters),
                           ('w2v',       vectorizer),
                           ('classif',   classifier)])

    return pipeline
