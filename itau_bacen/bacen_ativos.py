from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, BooleanType


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


'''
Criação do Schema
'''

new_schema = StructType() \
      .add("instituicao_financeira", StringType(), True) \
      .add("codigo",IntegerType(), True) \
      .add("conglomerado",StringType(), True) \
      .add("conglomerado_finannceiro",IntegerType(), True) \
      .add("conglomerado_prudencial",IntegerType(), True) \
      .add("tcb", StringType(), True) \
      .add("tc", IntegerType(), True) \
      .add("ti", IntegerType(), True) \
      .add("cidade",StringType(), True) \
      .add("uf", StringType(), True) \
      .add("data", StringType(), True) \
      .add("disponibilidade_a",StringType(), True) \
      .add("aplicacoes_interfinanceiras_de_liquidez_b", StringType(), True) \
      .add("tvm_e_instrumentos_inanceiros_derivativos_c", StringType(), True) \
      .add("operacoes_de_credito", StringType(), True) \
      .add("provisao_sobre_op_de_credito", StringType(), True) \
      .add("operacoes_de_credito_liquidas_de_provisao", StringType(), True) \
      .add("arrendamento_mercantil", StringType(), True) \
      .add("imobilizado_de_arrendamento", StringType(), True) \
      .add("credores_por_antecipacao_de_valor_residual", StringType(), True)\
      .add("provisao_sobre_arrendamento_mercantil_para_cl", StringType(), True)\
      .add("arrendamento_mercantil_liquido_de_provisao", StringType(), True)\
      .add("outros_creditos_liquido_de_provisao_f", StringType(), True)\
      .add("outros_ativos_realizaveis_g", StringType(), True) \
      .add("permanente_ajustado_h", StringType(), True) \
      .add("ativo_total_ajustado_i", StringType(), True) \
      .add("credores_por_antecipacao_de_valor_residual_j", StringType(), True) \
      .add("ativo_total_k", StringType(), True) \
      .add("remover", StringType(), True) \

'''
Leitura de CSV
'''

tempdf = spark.read.format('csv').\
    option("encoding", "utf8").\
    schema(new_schema).\
    options(inferSchema='True', delimiter=";").\
    load('Bacen - IfData - Ativos.csv')

tempdf.printSchema()

'''
Cria parquet
'''

tempdf.write.parquet('temp.parquet', mode='overwrite')

'''
Le parquet
'''
dfnew = spark.read.parquet('temp.parquet')

'''
Modelagem de Dados
'''

dfnew = dfnew.where(dfnew['instituicao_financeira']!= 'null' )
dfnew.cache()
dfnew.distinct()

'''
Mostra o dataframe
'''
dfnew.show()