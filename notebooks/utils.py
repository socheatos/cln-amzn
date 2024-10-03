import pyspark
from pyspark.sql import functions as F

def showCol(df:pyspark.sql.DataFrame, col: str|list, n=10, truncate=False):
    # print
    if isinstance(col, list):
        df.select(*[F.col(c) for c in col]).show(n, truncate)
    if isinstance(col, str):
        df.select(F.col(col)).show(n, truncate)



def arrTostr(col):
    pass

def extractRank(df:pyspark.sql.DataFrame, col):
    '''
    Use regex to extract the numerical rank of any item that has the format
    {d+} in Clothing, Shoes & Jewelry to ensure that it is in this clothing category :>
    '''
    temp = F.regexp_extract()

    pass