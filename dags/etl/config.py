URI = "s3a://"
BUCKET_BASE = "datalake"
LANDING_ZONE = "landing"
NAROMALIZATION_LAYER = "normalization"
CONSOLIDATION_LAYER = "consolidation"
BUCKET_BASE = "datalake"
BUCKET_LANDING = f"{URI}{BUCKET_BASE}/"
BUCKET_NORMALIZATION =  f"{URI}{BUCKET_BASE}/{NAROMALIZATION_LAYER}/"
BUCKET_CONSOLIDATION = f"{URI}{BUCKET_BASE}/{CONSOLIDATION_LAYER}/"

MINIO_URL = "http://minio:9000"
SPARK_MINIO_ACCESS_KEY = "spark"
SPARK_MINIO_SECRET_KEY = "sparkpass"
