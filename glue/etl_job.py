import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime

# 获取参数
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_path',
    'output_path',
    'database_name',
    'table_name'
])

# 初始化
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 60)
print(f"Glue Job: {args['JOB_NAME']}")
print(f"输入路径: {args['input_path']}")
print(f"输出路径: {args['output_path']}")
print(f"数据库: {args['database_name']}")
print(f"表名: {args['table_name']}")
print("=" * 60)

def main():
    """主处理函数"""
    
    # 1. 读取输入数据
    print("步骤1: 读取数据...")
    df = spark.read.json(args['input_path'])
    print(f"数据行数: {df.count()}")
    
    # 2. 数据清洗
    print("步骤2: 数据清洗...")
    df_clean = clean_data(df)
    
    # 3. 数据转换
    print("步骤3: 数据转换...")
    df_transformed = transform_data(df_clean)
    
    # 4. 数据质量检查
    print("步骤4: 数据质量检查...")
    quality_report = check_data_quality(df_transformed)
    
    # 5. 保存到 S3
    print("步骤5: 保存到 S3...")
    output_path = f"{args['output_path']}/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"
    df_transformed.write.mode('overwrite').parquet(output_path)
    
    # 6. 更新 Glue Data Catalog
    print("步骤6: 更新 Data Catalog...")
    update_data_catalog(df_transformed, output_path)
    
    # 7. 保存质量报告
    print("步骤7: 保存质量报告...")
    quality_report.write.mode('append').json(f"{args['output_path']}/quality_reports/")
    
    print("=" * 60)
    print("ETL 处理完成!")
    print(f"输出位置: {output_path}")
    print("=" * 60)
    
    job.commit()

def clean_data(df):
    """数据清洗"""
    # 删除空值过多的列
    total_rows = df.count()
    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        null_percentage = (null_count / total_rows) * 100
        
        if null_percentage > 90:  # 如果90%以上是空值，删除该列
            print(f"删除列 {col} (空值率: {null_percentage:.2f}%%)")
            df = df.drop(col)
    
    # 删除完全重复的行
    df = df.dropDuplicates()
    
    return df

def transform_data(df):
    """数据转换"""
    # 添加处理时间戳
    df = df.withColumn('processing_timestamp', F.current_timestamp())
    
    # 添加批处理ID
    df = df.withColumn('batch_id', F.lit(datetime.now().strftime('%Y%m%d%H%M%S')))
    
    # 标准化列名
    for col in df.columns:
        new_name = col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
        if new_name != col:
            df = df.withColumnRenamed(col, new_name)
    
    # 数据类型转换
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    
    return df

def check_data_quality(df):
    """数据质量检查"""
    from pyspark.sql import Row
    
    quality_metrics = []
    
    # 基本统计
    total_rows = df.count()
    total_columns = len(df.columns)
    
    quality_metrics.append(Row(
        metric_name='total_rows',
        metric_value=str(total_rows),
        timestamp=datetime.now()
    ))
    
    quality_metrics.append(Row(
        metric_name='total_columns',
        metric_value=str(total_columns),
        timestamp=datetime.now()
    ))
    
    # 检查每列的空值率
    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
        
        quality_metrics.append(Row(
            metric_name=f'null_percentage_{col}',
            metric_value=f'{null_percentage:.2f}',
            timestamp=datetime.now()
        ))
    
    # 创建质量报告 DataFrame
    quality_schema = StructType([
        StructField("metric_name", StringType()),
        StructField("metric_value", StringType()),
        StructField("timestamp", TimestampType())
    ])
    
    return spark.createDataFrame(quality_metrics, quality_schema)

def update_data_catalog(df, table_location):
    """更新 Glue Data Catalog"""
    try:
        # 创建临时视图
        df.createOrReplaceTempView("temp_table")
        
        # 创建数据库（如果不存在）
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args['database_name']}")
        
        # 创建表
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {args['database_name']}.{args['table_name']}
            USING PARQUET
            LOCATION '{table_location}'
            AS SELECT * FROM temp_table
        """)
        
        print(f"Data Catalog 已更新: {args['database_name']}.{args['table_name']}")
        
    except Exception as e:
        print(f"更新 Data Catalog 失败: {str(e)}")

if __name__ == "__main__":
    main()