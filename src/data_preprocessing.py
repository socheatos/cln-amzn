import pyspark
from pyspark.sql import functions as F
from pyspark.sql import dataframe
from pyspark.sql.types import StringType, ArrayType
import re
import utils

def replaceNAs(df:pyspark.sql.dataframe.DataFrame):
    '''
    Handles null values
        - drop null ASIN, title, feature, description
        - replace null BRANDS as "unknown"
    '''
    pass


def preprocess(file_path,spark:pyspark):
    df = spark.read.json(file_path)
    # remove irrelevant/sparse data
    # df = df.drop('imageURL','date','tech1','tech2','details','fit')\
    #         .dropDuplicates()\
    #         .na.drop(subset = ['description','features'])
    df = df.drop('imageURL','date','tech1','tech2','details','fit')\
            .dropDuplicates()
    
    # extract the rank and price and cast them as numeric values
    EXP = r'(\d*\,*\d*\,*\d+)\s*in\s*Clothing,\s*Shoes\s*\&*\s*Jewelry'
    df = df.withColumn('rank',F.regexp_replace(
                                                F.regexp_extract('rank',EXP,1),",",'')
                                                    .cast('int')
                                            )

    # regex extract price and change to float type
    EXP = r'\$*(\d+\.*\d+)'
    df = df.withColumn('price',F.regexp_extract('price',EXP,1)
                                                    .cast('float')
                                            )
    
    # clean up the category column, remove redundant "clothing, shoes & jewelry values"
    # removes features and categories that overlap 
    df = df.withColumn('category',F.slice(F.col('category'),3,F.size('category')))\
            .withColumn('overlap',F.array_intersect(F.col('category'),F.col('feature')))\
            .withColumn('category',F.array_except(F.col('category'),F.col('feature'))).drop('overlap')
    

    # change string type 
    # for category, replace empty spaces with , by 
    df = df.withColumns({'description':F.lower(F.concat_ws(',','description')),
                                        'feature':  F.lower(F.concat_ws(',','feature')),
                                        'category': F.regexp_replace(
                                                                     F.lower(F.concat_ws(',','category')),
                                                                     '-| ',''
                                        ),
                                        'title': F.regexp_replace(
                                                                F.lower(F.col('title'))
                                                                ,r"'", ""),\
                                        'brand': F.lower(F.col('brand'))
                                        })
    df = df.withColumn('category', F.expr(f"regexp_extract_all(category, '[a-z]+', 0)"))
    df = df.withColumn('feature',F.trim(F.regexp_replace(F.col("feature"), "\n|\\s{2,}", " ")))  # Remove newlines/extra spaces

    # drop duplicate entries of categories
    df = df.withColumn("category",F.array_distinct(F.col('category')))

    # lower case the brnads
    df = df.withColumn('brand', F.lower(F.col('brand')))


    return df
    
def extract_brand(df:dataframe.DataFrame):
    BRANDS = [b[0] for b in df.select('brand').distinct().collect() if b[0] is not None]
    KEYWORDS = ['womens','women','mens','men','little','boys','girls','baby','unisex','adult','juniors','ladies']

    brand_pattern = r"\b(" + "|".join([re.escape(b) for b in BRANDS]) + r")\b"
    keyword_pattern = r"^(.*?)\ "+r"\b(" + "|".join([re.escape(b) for b in KEYWORDS]) + r")\b"


    df = df.withColumn(
                "brand_match",
                F.when(
                    F.col('title').rlike(brand_pattern),
                    F.regexp_extract('title',brand_pattern,0).alias('match'),
                )
            ).withColumn(
                'keyword_match',
                F.when(
                    (F.col('brand_match').isNull()) &
                    (F.col('title').rlike(keyword_pattern)),
                    F.regexp_extract('title',keyword_pattern,1).alias('match'),
                )
            ).withColumn(
                "brand_extracted",
                F.coalesce(F.col('brand'),F.col("brand_match"), F.col("keyword_match"))
            )
    df = df.drop(
                F.col('brand_match'),
                F.col('keyword_match'),
    )

    BRANDS = [b[0] for b in df.select('brand_extracted').collect() if b[0] is not None]
    BRANDS = [b for b in BRANDS if b!='']
    brand_pattern = r"\b(" + "|".join([re.escape(b) for b in BRANDS]) + r")\b"
    df = df.withColumn('matched',
                                F.regexp_extract(F.col('title'), brand_pattern,0))\
                        .withColumn('brand_extracted',F.coalesce(F.col('brand_extracted'),F.col('matched')))
    df = df.drop(F.col('matched'))
    return df