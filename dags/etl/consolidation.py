from pyspark.sql import SparkSession
from utils import *
from config import *

# Sessão Spark
spark = get_spark_session("ETL Sales - Consolidation Layer")

# Ler os arquivos na camada Normalization e salva na camada consolidation
def main():
    
    # Registra tabela TempView para uso do Spark SQL
    # Obs: Tabelas que serao relaciodas em de uma consulta
    createTempView(spark, BUCKET_NORMALIZATION + "product", "product")
    createTempView(spark, BUCKET_NORMALIZATION + "district", "district")
    createTempView(spark, BUCKET_NORMALIZATION + "city", "city")
    createTempView(spark, BUCKET_NORMALIZATION + "state", "state")
    createTempView(spark, BUCKET_NORMALIZATION + "zone", "zone")      
    
    #==============================================================
    #                    Tabela Fato Sale
    #==============================================================
    
    # Registra tabela TempView para uso do Spark SQL
    createTempView(spark, BUCKET_NORMALIZATION + "sale", "sale")
    createTempView(spark, BUCKET_NORMALIZATION + "sale_item", "sale_item")
    
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
      """, spark)
    
    saveParquet(df_fact_sale, BUCKET_CONSOLIDATION + "fact_sale")
    
    
    #==============================================================
    #                    Tabela Dimensao Product
    #============================================================== 
    
    # Registra tabela TempView para uso do Spark SQL
    createTempView(spark, BUCKET_NORMALIZATION + "product_group", "product_group")
    createTempView(spark, BUCKET_NORMALIZATION + "supplier", "supplier")
    
    # Relaciona os dados necessarios para a gerar a tebala Dimensao Product
    df_dim_product = sql("""
        SELECT p.id    as product_key
             , p.name  AS product_name
             , pg.name AS product_group_name
             , s.name  AS supplier_name
          FROM product p INNER JOIN product_group pg ON p.id_product_group = pg.id
                         INNER JOIN supplier s       ON p.id_supplier      = s.id 
    """, spark)

    saveParquet(df_dim_product, BUCKET_CONSOLIDATION + "dim_product")
    
    
    #==============================================================
    #                    Tabela Dimensao Customer
    #============================================================== 
    
    # Registra tabela TempView para uso do Spark SQL    
    createTempView(spark, BUCKET_NORMALIZATION + "customer", "customer")
    createTempView(spark, BUCKET_NORMALIZATION + "marital_status", "marital_status")   
    
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
            """, spark)

    saveParquet(df_dim_customer, BUCKET_CONSOLIDATION + "dim_customer")    
        
    #==============================================================
    #                    Tabela Dimensao Branch
    #============================================================== 
    
    # Registra tabela TempView para uso do Spark SQL        
    createTempView(spark, BUCKET_NORMALIZATION + "branch", "branch")
       
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
    """, spark)

    saveParquet(df_dim_branch, BUCKET_CONSOLIDATION + "dim_branch")    

    
    #==============================================================
    #                    Tabela Dimensao Employee
    #============================================================== 
    
    # Registra tabela TempView para uso do Spark SQL 
    createTempView(spark, BUCKET_NORMALIZATION + "employee", "employee")
    createTempView(spark, BUCKET_NORMALIZATION + "department", "department")    
    
    # Relaciona os dados necessarios para a gerar a tebala Dimensao Employee
    df_dim_employee = sql("""
        SELECT e.id    AS  employee_key
             , e.name  AS  employee_name
             , d.name  AS  department_name       
          FROM employee e INNER JOIN department d ON d.id = e.id_department   
    """, spark)

    saveParquet(df_dim_employee, BUCKET_CONSOLIDATION + "dim_employee")    
    
    spark.stop()            
  
if __name__ == "__main__":
    main()