#####################################################################
### Funções utilizadas no modelo de machine learning para ordenar ###
### matérias do Diário Oficial da União por relevância.           ###
#####################################################################

import re
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
import numpy as np

### Preprocessing       ###
### no fitting required ###


def tipo_edicao_Q(edicao):
    """
    Define se edição do artigo é ordinária ou extra.
    """
    return 'Extra' if len(edicao.split('-')) > 1 else 'Ordinária'


def get_article_type(string):
    """
    Parse article title (identifica) into a type (e.g. portaria, ato).
    """
    # List of specific replacements:
    speficic_replace = [('julgamento', 'julgamento '), ('adiamento', 'adiamento '), ('licitação', 'licitação '),
                        ('adjudicação', 'adjudicação '), ('homologação', 'homologação '), 
                        ('suspensão', 'suspensão '), ('prorrogação', 'prorrogação '), 
                        ('homologado', 'homologado '), ('retificação', 'retificação '), 
                        ('alteração', 'alteração '), ('revogação', 'revogação '), ('habilitação', 'habilitação '), 
                        ('pregão', ' pregão')]
    
    # If string is not str (e.g. NaN), return empty string:
    if type(string) != str:
        return ''
    
    # Process string:
    proc_string = string
    proc_string = proc_string.lower()
    # Remove excess of whitespace:
    proc_string = ' '.join(proc_string.strip().split())
    # Remove dates:
    proc_string = re.sub('de \d{1,2}[°º]? de [a-zç]{3,11} de \d{4}', '', proc_string)
    # Remove article numbering:
    proc_string = re.sub('n[°ºª] ?[0-9\.,/\-]*', '', proc_string)
    # Replace characters by space:
    proc_string = re.sub('/', ' ', proc_string)
    proc_string = re.sub('\|', ' ', proc_string)
    
    # Make specific replacements:
    for spec_rep in speficic_replace:
        proc_string = proc_string.replace(spec_rep[0], spec_rep[1])
    
    proc_string = ' '.join(proc_string.split()).strip()
    return proc_string


def count_levels(orgao, splitter):
    """
    Return the number of levels in the `orgao` (split by `splitter`).
    """
    return len(orgao.split(splitter))


def create_level_df(series, splitter='/', prefix='org'):
    """
    Split a Pandas `series` into columns of a dataframe using `splitter` 
    as the column separator. The lack of levels in a certain row 
    translates to empty columns on the right.
    """
    # Get number of levels:
    max_levels  = series.apply(lambda s: count_levels(s, splitter)).max()
    # Set names of columns:
    columns = [prefix + str(i) for i in range(1, max_levels + 1)]
    # Split Series into columns by splitter:
    splitted = pd.DataFrame(series.str.split(splitter).values.tolist(), columns=columns, index=series.index)
    
    return splitted


class PreprocessDOU(BaseEstimator, TransformerMixin):
    """
    Preprocess (no fitting required) a Pandas DataFrame containing information 
    about DOU publications. These are preprocessing specific to DOU, and 
    some combine or split columns. The steps are:

    -- Clean title to get only the kind of document;
    -- Find out from 'edicao' column if this is an ordinary or extra edition
       and set it as a new column;
    -- Set 'secao' as str;
    -- Fill missing values with `fillna`;
    -- Join columns containing texts (according to requested columns in 
       `colunas_relevantes`);
    -- Split `orgao` column into their hierarchy levels if 'orgaos' is 
       among `colunas_relevantes`.
    
    Input
    -----

    colunas_relevantes : list of str
        List of columns to keep in the output. These might be columns present in the 
        input DataFrame or new columns created by this transformer. To split column 
        'orgao' into columns 'org1', 'org2', ..., place 'orgaos' in `colunas_relevantes`.

    fillna : str
        A string used to identify that a value is missing.


    Output
    ------

    A Pandas DataFrame.
    """    

    def __init__(self, colunas_relevantes=None, fillna=None):
        self.colunas_relevantes = colunas_relevantes
        self.fillna = fillna
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        
        # Select all dataset:
        df = X.copy()            

        # PREPROCESS SOME COLUMNS:
        
        # Remove dates and numbers from title:
        if 'titulo' in self.colunas_relevantes:
            df['titulo'] = df.identifica.apply(get_article_type)
        
        # Is materia from ordinary or extra edition:
        if 'tipo_edicao' in self.colunas_relevantes:
            df['tipo_edicao'] = df.edicao.astype(str).apply(tipo_edicao_Q)
        
        # Transform seção to string:
        if 'secao' in self.colunas_relevantes:
            df['secao'] = df.secao.astype(str)
        
        # Fill missing values:
        if self.fillna != None:
            df = df.fillna(value=self.fillna)
        
        # Create a single text column with ementa and text:
        if 'ementa_text' in self.colunas_relevantes:
            df['ementa_text'] = df.ementa + ' ' + df.fulltext
        
        # Create a single text column with titulo, ementa and text:
        if 'tit_ementa_text' in self.colunas_relevantes:
            df['titulo'] = df.identifica.apply(get_article_type)
            df['tit_ementa_text'] = df.titulo + ' ' + df.ementa + ' ' + df.fulltext
        
        # Create a single text column with titulo, orgao, ementa and text:
        if 'tit_org_ementa_text' in self.colunas_relevantes:
            df['titulo'] = df.identifica.apply(get_article_type)
            orgao_text   = df.orgao.str.replace('/', ' ')
            df['tit_org_ementa_text'] = df.titulo + ' ' + orgao_text + ' ' + df.ementa + ' ' + df.fulltext

        # Create a single text column with orgao, ementa and text:
        if 'org_ementa_text' in self.colunas_relevantes:
            orgao_text   = df.orgao.str.replace('/', ' ')
            df['org_ementa_text'] = orgao_text + ' ' + df.ementa + ' ' + df.fulltext
            
        # Split orgaos by their level:
        if 'orgaos' in self.colunas_relevantes:
            # Split orgaos by '/':
            orgaos = create_level_df(df.orgao)
            # Transform 'orgaos' column into level columns:
            self.colunas_relevantes = list(filter(lambda s: s != 'orgaos', self.colunas_relevantes))
            self.colunas_relevantes = self.colunas_relevantes + list(orgaos.columns)
            # Fill missing values in orgaos: 
            if self.fillna != None:
                orgaos = orgaos.fillna(value=self.fillna)
            df = df.join(orgaos)
            assert len(df.dropna(how='any')) == len(X), 'Number of rows is not preserved'
        
        # Only output selected columns (default: passthrough):
        df = df[self.colunas_relevantes]

        return df


def remove_punctuation(text, keep_cash=True):
    """
    Remove punctuation from text.
    
    Input
    -----
    
    text : str
        Text to remove punctuation from.
        
    keep_cash : bool (default True)
        Whether to remove the dollar sign '$' or not.
    """
    # Define which punctuation to remove:
    punctuation = '!"#%&\'()*+,-.:;/<=>?@[\\]^_`{|}~'
    if keep_cash == False:
        punctuation = punctuation + '$'
    
    return text.translate(str.maketrans('', '', punctuation))


def remove_stopwords(text, stopwords):
    """
    Remove list of words (str) in `stopwords` from string `text`. 
    """
    word_list = text.split()
    word_list = [word for word in word_list if not word in set(stopwords)]
    return ' '.join(word_list)


def stem_words(text, stemmer):
    """
    Given a `stemmer`, use it to stem `text`.  
    """
    #stemmer   = nltk.stem.RSLPStemmer()   # Português
    #stemmer   = nltk.stem.PorterStemmer() # Inglês
    word_list = text.split()
    word_list = [stemmer.stem(word) for word in word_list]
    return ' '.join(word_list)


def remove_accents(string, i=0):
    """
    Input: string
    
    Returns the same string, but without all portuguese-valid accents.
    """
    accent_list = [('Ç','C'),('Ã','A'),('Á','A'),('À','A'),('Â','A'),('É','E'),('Ê','E'),('Í','I'),('Õ','O'),
                   ('Ó','O'),('Ô','O'),('Ú','U'),('Ü','U'),('ç','c'),('ã','a'),('á','a'),('à','a'),('â','a'),
                   ('é','e'),('ê','e'),('í','i'),('õ','o'),('ó','o'),('ô','o'),('ú','u'),('ü','u')]
    if i >= len(accent_list):
        return string
    else:
        string = string.replace(*accent_list[i])
        return remove_accents(string, i + 1)

    
def keep_only_letters(text, keep_cash=True):
    """
    Remove from string `text` all characters that are not letters (letters include those 
    with portuguese accents). If `keep_cash` is true, do not remove the dollar sign 
    '$'.
    """
    if keep_cash == True:
        extra_chars = '$'
    else:
        extra_chars = ''
        
    only_letters = re.sub('[^a-z A-ZÁÂÃÀÉÊÍÔÓÕÚÜÇáâãàéêíóôõúüç' + extra_chars + ']', '', text)
    only_letters = ' '.join(only_letters.split())
    return only_letters


def num_to_scale(x, b=2):
    """
    Given a float `x`, returns its log on base `b`.
    """
    return int(np.round(np.log(np.clip(x, a_min=1, a_max=None)) / np.log(b)))


def number_scale_token(matchobj):
    """
    Given a regex match object that is supposed to match a number in 
    Brazilian format (e.g. 10.643.543,05), replace it by a string token 
    whose length is proportional to its scale (i.e. log).
    """
    full_match   = matchobj.group(0)
    target_match = matchobj.group(1)
    
    target_float = float(target_match.replace('.', '').replace(',', '.'))
    target_new   = ' xx' + 'v' * num_to_scale(target_float) + 'xx '    
    full_new     = full_match.replace(target_match, target_new)
    return full_new


def values_to_token(text, regex_list):
    """
    Given a string `text` and a list of regex patterns for values 
    in Brazilian format (e.g. 24.532,78), replace them by a string 
    token whose length is prop. to the log of the value.
    """
    tokenized = text
    for regex in regex_list:
        tokenized = re.sub(regex, number_scale_token, tokenized)
        
    return tokenized


class PreProcessText(BaseEstimator, TransformerMixin):
    """
    Preprocess text (no fitting required) stored in a DataFrame columns.
    
    Given a list of columns `text_cols`, apply a series of transformations 
    to all of them, as requested by the instance parameters:
    
    -- Set all to lowercase;
    -- Transform numbers representing R$ amounts into tokens 
       according to their scale;
    -- Remove punctuation from text;
    -- Do not remove the dollar sign '$';
    -- Remove stopwords (list of str);
    -- Use `stemmer` to stem the words;
    -- Remove accents; 
    -- Keep only letters (and the dollar sign, if requested).
    
    Return
    ------
    
    Pandas DataFrame.
    """
    def __init__(self, text_cols=None, lowercase=True, value_tokens=True, remove_punctuation=True, 
                 keep_cash=True, stopwords=None, stemmer=None, strip_accents=True, only_letters=True):
        self.text_cols          = text_cols
        self.lowercase          = lowercase
        self.value_tokens       = value_tokens
        self.remove_punctuation = remove_punctuation
        self.keep_cash          = keep_cash
        self.stopwords          = stopwords
        self.stemmer            = stemmer
        self.strip_accents      = strip_accents
        self.only_letters       = only_letters
        
        # Value to token regex:
        regex1_str = r'[rR]\$ ?(\d{1,3}(?:\.\d{3}){0,4}\,?\d{0,2})'
        regex2_str = r'(\d{1,3}(?:\.\d{3}){0,4}\,?\d{0,2}) (?:reais|REAIS|Reais)'
        regex3_str = r'(\d{1,3}(?:\.\d{3}){0,4},\d{2})(?:[^%]|$)'
        regex1 = re.compile(regex1_str)
        regex2 = re.compile(regex2_str)
        regex3 = re.compile(regex3_str)
        self.regex_list = [regex1, regex2, regex3]
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        df = X.copy()
        
        # Apply transforms to all X columns that are text:
        if self.text_cols != None:
            for col in self.text_cols:
                #if df[col].dtype == np.dtype('O'):
                # Lowercase:
                if self.lowercase:
                    df[col] = df[col].str.lower()
                # Transform R$ values to tokens:
                if self.value_tokens:
                    df[col] = df[col].apply(lambda s: values_to_token(s, self.regex_list))
                # Remove punctuation:
                if self.remove_punctuation:
                    df[col] = df[col].apply(lambda s: remove_punctuation(s, self.keep_cash))
                # Remove stopwords:
                if self.stopwords != None:
                    df[col] = df[col].apply(lambda s: remove_stopwords(s, self.stopwords))
                # Stem words:
                if self.stemmer != None:
                    df[col] = df[col].apply(lambda s: stem_words(s, self.stemmer))
                # Remove accents:
                if self.strip_accents:
                    df[col] = df[col].apply(remove_accents)
                # Keep only leters:
                    df[col] = df[col].apply(lambda s: keep_only_letters(s, self.keep_cash))
        return df

    
### Model builders ###

#import nltk
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge

def create_model():
    
    # Pré-processamento (s/ fit):
    text_col = 'tit_org_ementa_text'
    colunas_relevantes = ['tipo_edicao'] + [text_col]

    stopwords = ['de', 'a', 'o', 'que', 'e', 'é', 'do', 'da', 'em', 'um', 'para', 'com', 'não', 'uma', 'os', 'no', 'se', 'na', 'por', 'mais',
                 'as', 'dos', 'como', 'mas', 'ao', 'ele', 'das', 'à', 'seu', 'sua', 'ou', 'quando', 'muito', 'nos', 'já', 'eu', 'também',
                 'só', 'pelo', 'pela', 'até', 'isso', 'ela', 'entre', 'depois', 'sem', 'mesmo', 'aos', 'seus', 'quem', 'nas', 'me', 'esse',
                 'eles', 'você', 'essa', 'num', 'nem', 'suas', 'meu', 'às', 'minha', 'numa', 'pelos', 'elas', 'qual', 'nós', 'lhe', 'deles',
                 'essas', 'esses', 'pelas', 'este', 'dele', 'tu', 'te', 'vocês', 'vos', 'lhes', 'meus', 'minhas', 'teu', 'tua', 'teus',
                 'tuas', 'nosso', 'nossa', 'nossos', 'nossas', 'dela', 'delas', 'esta', 'estes', 'estas', 'aquele', 'aquela', 'aqueles',
                 'aquelas', 'isto', 'aquilo', 'estou', 'está', 'estamos', 'estão', 'estive', 'esteve', 'estivemos', 'estiveram', 'estava',
                 'estávamos', 'estavam', 'estivera', 'estivéramos', 'esteja', 'estejamos', 'estejam', 'estivesse', 'estivéssemos',
                 'estivessem', 'estiver', 'estivermos', 'estiverem', 'hei', 'há', 'havemos', 'hão', 'houve', 'houvemos', 'houveram', 'houvera',
                 'houvéramos', 'haja', 'hajamos', 'hajam', 'houvesse', 'houvéssemos', 'houvessem', 'houver', 'houvermos', 'houverem', 'houverei',
                 'houverá', 'houveremos', 'houverão', 'houveria', 'houveríamos', 'houveriam', 'sou', 'somos', 'são', 'era', 'éramos', 'eram',
                 'fui', 'foi', 'fomos', 'foram', 'fora', 'fôramos', 'seja', 'sejamos', 'sejam', 'fosse', 'fôssemos', 'fossem', 'for', 'formos',
                 'forem', 'serei', 'será', 'seremos', 'serão', 'seria', 'seríamos', 'seriam', 'tenho', 'tem', 'temos', 'tém', 'tinha', 'tínhamos',
                 'tinham', 'tive', 'teve', 'tivemos', 'tiveram', 'tivera', 'tivéramos', 'tenha', 'tenhamos', 'tenham', 'tivesse', 'tivéssemos',
                 'tivessem', 'tiver', 'tivermos', 'tiverem', 'terei', 'terá', 'teremos', 'terão', 'teria', 'teríamos', 'teriam']

    dou_extractor = PreprocessDOU(colunas_relevantes, 'xxnuloxx')

    proc_text = PreProcessText(lowercase=False, remove_punctuation=False, keep_cash=True, stopwords=stopwords, 
                              stemmer=None, strip_accents=False, only_letters=False,
                              text_cols=[text_col])

    #preprocess = Pipeline([('dou', dou_extractor), ('pretext', proc_text)])

    # Fit processing and model:
    vectorizer    = CountVectorizer(lowercase=False, binary=True, ngram_range=(1,2), max_df=1.0, min_df=1)
    #vectorizer    = TfidfVectorizer(lowercase=False, binary=True, ngram_range=(1,1), max_df=0.6, min_df=1)

    encoder_extra = OneHotEncoder(drop='first')
    processor     = ColumnTransformer([('vec',   vectorizer,    text_col),
                                       ('extra', encoder_extra, ['tipo_edicao'])
    ])

    #classifier  = ElasticNet(alpha=0.023, l1_ratio=0.4, selection='random', max_iter=5000)
    classifier  = Ridge(1000)
    #classifier  = Lasso(0.03, max_iter=4000, selection='random', tol=1e-4)
    #fit_pipe    = Pipeline([('proc', processor), ('fit', classifier)])

    pipeline = Pipeline([('dou', dou_extractor), ('pretext', proc_text), ('proc', processor), ('fit', classifier)])

    return pipeline
