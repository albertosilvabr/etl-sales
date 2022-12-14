import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("etl_sales_consolidation").getOrCreate()

# Constantes
path_normalization = "/data-lake/normalization/"
path_consolidation = "/data-lake/consolidation/"
#path_normalization = "/home/jovyan/work/dataset/store/normalization/"
#path_consolidation = "/home/jovyan/work/dataset/store/consolidation/"

# Funcoes para leitura de arquivos
def createTempView(path, table_name):
    df = spark.read.parquet(path)
    df.createOrReplaceTempView(table_name)

def sql(query):
    return spark.sql(query)    
    
def q(query, n=30):
    return spark.sql(query)    

def saveParquet(df, path):
    df.write.mode("overwrite").format("parquet").save(path)

    
# Ler os arquivos na camada Normalization e salva na camada consolidation
def main():
    
    # Registra tabela TempView para uso do Spark SQL
    # Obs: Tabelas que serao relaciodas em de uma consulta
    createTempView(path_normalization + "product", "product")
    createTempView(path_normalization + "district", "district")
    createTempView(path_normalization + "city", "city")
    createTempView(path_normalization + "state", "state")
    createTempView(path_normalization + "zone", "zone")      
    
    #==============================================================
    #                    Tabela Fato Sale
    #==============================================================
    
    # Registra tabela TempView para uso do Spark SQL
    createTempView(path_normalization + "sale", "sale")
    createTempView(path_normalization + "sale_item", "sale_item")
    
    # Relaciona os dados necessarios para a gerar a tebala Fato Sale
    df_fact_sale = sql("""
        SELECT s.id          AS sale_key
             , s.id_customer AS customer_key
             , s.id_branch   AS branch_key
             , s.id_employee AS employee_key
             , p.id          AS product_key
             , si.quantity 
             , p.cost_price 
             , p.sale_price 
             , (si.quantity * p.cost_price) AS amount_cost_price
             , (si.quantity * p.sale_price) as amount_sale_price
        FROM sale s INNER JOIN sale_item si ON s.id  = si.id_sale 
                    INNER JOIN product p    ON p.id  = si.id_product                  
      """)
    
    saveParquet(df_fact_sale, path_consolidation + "fact_sale")
    
    
    #==============================================================
    #                    Tabela Dimensao Product
    #============================================================== 
    
    # Registra tabela TempView para uso do Spark SQL
    createTempView(path_normalization + "product_group", "product_group")
    createTempView(path_normalization + "supplier", "supplier")
    
    # Relaciona os dados necessarios para a gerar a tebala Dimensao Product
    df_dim_product = sql("""
        SELECT p.id    as product_key
             , p.name  AS product_name
             , pg.name AS product_group_name
             , s.name  AS supplier_name
          FROM product p INNER JOIN product_group pg ON p.id_product_group = pg.id
                         INNER JOIN supplier s       ON p.id_supplier      = s.id 
    """)

    saveParquet(df_dim_product, path_consolidation + "dim_product")
    
    
    #==============================================================
    #                    Tabela Dimensao Customer
    #============================================================== 
    
    # Registra tabela TempView para uso do Spark SQL    
    createTempView(path_normalization + "customer", "customer")
    createTempView(path_normalization + "marital_status", "marital_status")   
    
    # Relaciona os dados necessarios para a gerar a tebala Dimensao Customer
    df_dim_customer = sql("""
        SELECT c.id    AS  customer_key
             , c.name  AS  customer_name
             , c.income 
             , c.gender
             , d.name  AS district_name
             , m.name  as marital_status
             , z.name  AS zone_name
             , cit.name AS city_name
             , s.name  AS state_name
          FROM customer c INNER JOIN district d       ON d.id   = c.id_district               
                          INNER JOIN city cit         ON cit.id = d.id_city
                          INNER JOIN state s          ON s.id   = cit.id_state
                          INNER JOIN zone z           ON z.id   = d.id_zone
                          INNER JOIN marital_status m ON m.id   = c.id_marital_status     
            """)

    saveParquet(df_dim_customer, path_consolidation + "dim_customer")    
        
    #==============================================================
    #                    Tabela Dimensao Branch
    #============================================================== 
    
    # Registra tabela TempView para uso do Spark SQL        
    createTempView(path_normalization + "branch", "branch")
       
    # Relaciona os dados necessarios para a gerar a tebala Dimensao Branch
    df_dim_branch = sql("""
        SELECT bc.id    AS branch_key
             , bc.name  AS branch_name
             , dt.name  AS district_name
             , z.name   AS zone_name
             , cit.name AS city_name
             , s.name   AS state_name
          FROM branch bc  INNER JOIN district dt ON dt.id  = bc.id_district              
                          INNER JOIN city cit    ON cit.id = dt.id_city
                          INNER JOIN state s     ON s.id   = cit.id_state
                          INNER JOIN zone z      ON z.id   = dt.id_zone
    """)

    saveParquet(df_dim_branch, path_consolidation + "dim_branch")    

    
    #==============================================================
    #                    Tabela Dimensao Employee
    #============================================================== 
    
    # Registra tabela TempView para uso do Spark SQL 
    createTempView(path_normalization + "employee", "employee")
    createTempView(path_normalization + "department", "department")    
    
    # Relaciona os dados necessarios para a gerar a tebala Dimensao Employee
    df_dim_employee = sql("""
        SELECT e.id    AS  employee_key
             , e.name  AS  employee_name
             , d.name  AS  department_name       
          FROM employee e INNER JOIN department d ON d.id = e.id_department   
    """)

    saveParquet(df_dim_employee, path_consolidation + "dim_employee")    
    
    spark.stop()            
  
if __name__ == "__main__":
    main()