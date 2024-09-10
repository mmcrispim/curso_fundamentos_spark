import pytest
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName('TestEDA-Script').getOrCreate()

def test_dataframe_schema(spark):
    dados_dsa = [
        ('Roberto', 'Vendas', 30000),
        ('Michael', 'Vendas', 46000),
        ('Julio', 'Vendas', 41000),
        ('Maria', 'Contabilidade', 30000),
        ('Roberto', 'Vendas', 30000),
        ('Gustavo', 'Contabilidade', 33000),
        ('Jenifer', 'Contabilidade', 39000),
        ('Ana', 'Marketing', 30000),
        ('Ana', 'Marketing', 30000),
        ('Saulo', 'Vendas', 41000),
    ]
    columns = ['nome_funcionario', 'departmento', 'salario']
    
    df = spark.createDataFrame(data=dados_dsa, schema=columns)
    
    # Verifica se o schema é o esperado
    expected_schema = ['nome_funcionario', 'departmento', 'salario']
    assert df.columns == expected_schema

def test_distinct_count(spark):
    dados_dsa = [
        ('Roberto', 'Vendas', 30000),
        ('Michael', 'Vendas', 46000),
        ('Julio', 'Vendas', 41000),
        ('Maria', 'Contabilidade', 30000),
        ('Roberto', 'Vendas', 30000),
        ('Gustavo', 'Contabilidade', 33000),
        ('Jenifer', 'Contabilidade', 39000),
        ('Ana', 'Marketing', 30000),
        ('Ana', 'Marketing', 30000),
        ('Saulo', 'Vendas', 41000),
    ]
    columns = ['nome_funcionario', 'departmento', 'salario']
    
    df = spark.createDataFrame(data=dados_dsa, schema=columns)
    
    distinctDF = df.distinct()
    
    # Verifica se o número de linhas distintas está correto
    assert distinctDF.count() == 9

def test_drop_duplicates(spark):
    dados_dsa = [
        ('Roberto', 'Vendas', 30000),
        ('Michael', 'Vendas', 46000),
        ('Julio', 'Vendas', 41000),
        ('Maria', 'Contabilidade', 30000),
        ('Roberto', 'Vendas', 30000),
        ('Gustavo', 'Contabilidade', 33000),
        ('Jenifer', 'Contabilidade', 39000),
        ('Ana', 'Marketing', 30000),
        ('Ana', 'Marketing', 30000),
        ('Saulo', 'Vendas', 41000),
    ]
    columns = ['nome_funcionario', 'departmento', 'salario']
    
    df = spark.createDataFrame(data=dados_dsa, schema=columns)
    
    # Remove duplicatas com base em todas as colunas
    df_no_duplicates = df.dropDuplicates()
    
    # Verifica se a contagem de linhas após remover duplicatas está correta
    assert df_no_duplicates.count() == 9

def test_drop_duplicates_columns(spark):
    dados_dsa = [
        ('Roberto', 'Vendas', 30000),
        ('Michael', 'Vendas', 46000),
        ('Julio', 'Vendas', 41000),
        ('Maria', 'Contabilidade', 30000),
        ('Roberto', 'Vendas', 30000),
        ('Gustavo', 'Contabilidade', 33000),
        ('Jenifer', 'Contabilidade', 39000),
        ('Ana', 'Marketing', 30000),
        ('Ana', 'Marketing', 30000),
        ('Saulo', 'Vendas', 41000),
    ]
    columns = ['nome_funcionario', 'departmento', 'salario']
    
    df = spark.createDataFrame(data=dados_dsa, schema=columns)
    
    # Remove duplicatas com base em 'departmento' e 'salario'
    df_drop_dis = df.dropDuplicates(['departmento', 'salario'])
    
    # Verifica se a contagem de linhas após remover duplicatas nessas colunas está correta
    assert df_drop_dis.count() == 6
