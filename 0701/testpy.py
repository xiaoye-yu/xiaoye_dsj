import argparse
from pyhive import hive
from datetime import datetime
import os


def get_all_ads_tables(hive_host, hive_port, hive_database):
    """获取 Hive 数据库中所有前缀为 ads 的表名"""
    try:
        connection = hive.Connection(
            host=hive_host,
            port=hive_port,
            database=hive_database
        )
        cursor = connection.cursor()

        # 获取所有表，然后在 Python 中过滤
        cursor.execute("SHOW TABLES")

        tables = []
        for table in cursor.fetchall():
            table_name = table[0]
            if 'ads' in table_name.lower():  # 保留原始过滤逻辑
                print(table_name)
                tables.append(table_name)  # 收集匹配的表名

        return tables  # 返回完整的表名列表

    except Exception as e:
        print(f"获取 Hive 表列表失败: {e}")
        exit(1)
    finally:
        if connection:
            connection.close()


def get_hive_table_columns(hive_host, hive_port, hive_database, table):
    """获取 Hive 表的所有字段及其类型"""
    try:
        connection = hive.Connection(
            host=hive_host,
            port=hive_port,
            database=hive_database
        )
        cursor = connection.cursor()

        # 获取表结构信息
        cursor.execute(f"DESCRIBE {table}")
        columns = []
        for row in cursor.fetchall():
            column_info = {
                'name': row[0],
                'type': row[1]
            }
            columns.append(column_info)

        return columns

    except Exception as e:
        print(f"获取表 {table} 的字段信息失败: {e}")
        return []
    finally:
        if connection:
            connection.close()


def map_hive_to_mysql_type(hive_type):
    """将 Hive 数据类型映射到 MySQL 数据类型"""
    hive_type = hive_type.lower()

    # 数值类型
    if 'tinyint' in hive_type:
        return 'INT'
    elif 'smallint' in hive_type:
        return 'INT'
    elif 'int' in hive_type or 'integer' in hive_type:
        return 'INT'
    elif 'bigint' in hive_type:
        return 'BIGINT'
    elif 'float' in hive_type:
        return 'FLOAT'
    elif 'double' in hive_type:
        return 'DOUBLE'
    elif 'decimal' in hive_type:
        return 'DECIMAL' + hive_type[hive_type.find('('):]

    # 日期时间类型
    elif 'date' in hive_type:
        return 'VARCHAR(255)'
    elif 'timestamp' in hive_type:
        return 'VARCHAR(255)'

    # 字符串类型
    elif 'string' in hive_type:
        return 'VARCHAR(255)'
    elif 'binary' in hive_type:
        return 'VARCHAR(255)'

    # 其他类型
    else:
        return 'VARCHAR(255)'


def generate_mysql_ddl(table, columns):
    """生成 MySQL DDL 语句（添加字符集配置）"""
    mysql_ddl = f"CREATE TABLE IF NOT EXISTS {table} (\n"

    # 添加字段定义
    for col in columns:
        mysql_type = map_hive_to_mysql_type(col['type'])
        mysql_ddl += f"    {col['name']} {mysql_type},\n"

    # 添加表级字符集配置
    mysql_ddl = mysql_ddl.rstrip(",\n") + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

    return mysql_ddl


def save_mysql_ddl_to_file(ddl, table, output_dir="mysql_ddl_scripts"):
    """将 MySQL DDL 语句保存到文件"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建目录: {output_dir}")

    ddl_file = os.path.join(output_dir, f"{table}.sql")
    with open(ddl_file, "w") as f:
        f.write(ddl)

    print(f"  → MySQL DDL 已生成: {ddl_file}")


def generate_seatunnel_config(hive_metastore_uri, mysql_host, mysql_port, mysql_user, mysql_password, table, columns,
                              database="gmall"):
    """
    生成 SeaTunnel 配置文件，包含 query 和 fields 配置

    参数:
    hive_metastore_uri: Hive Metastore URI
    mysql_host: MySQL 服务器地址
    mysql_port: MySQL 端口
    mysql_user: MySQL 用户名
    mysql_password: MySQL 密码
    table: 要同步的表名
    columns: 表字段信息
    database: MySQL 数据库名
    """
    output_file = f"{table}_hive_to_mysql.conf"

    # 生成字段列表，用于 fields 和 query 配置
    column_names = [col['name'] for col in columns]
    # 构造 query 语句，这里假设目标表名和 Hive 表名相同，可根据实际调整
    query_sql = f"INSERT INTO {table} ({', '.join(column_names)}) VALUES ({', '.join(['?'] * len(column_names))})"

    config_content = f"""# 自动生成的 SeaTunnel 配置文件
# 源表: {database}.{table}
# 目标表: {database}.{table}
# 生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

env {{
  execution.parallelism = 1
  job.mode = "BATCH"
}}

source {{
  Hive {{
    table_name = "{database}.{table}"
    metastore_uri = "{hive_metastore_uri}"
    hive.hadoop.conf-path = "/etc/hadoop/conf"
    fields = {column_names}
  }}
}}

transform {{

}}

sink {{
  Jdbc {{
    url = "jdbc:mysql://{mysql_host}:{mysql_port}/{database}"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "{mysql_user}"
    password = "{mysql_password}"
    table = "{table}"
    generate_sink_sql = false
    query = "{query_sql}"
    fields = {column_names}
  }}
}}
"""
    if not os.path.exists("conf"):
        os.makedirs("conf")
        print(f"创建目录: conf")
    ddl_file = os.path.join("conf", f"{output_file}")
    with open(ddl_file, "w") as f:
        f.write(config_content)

    print(f"  → 配置文件已生成: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='生成 SeaTunnel Hive 到 MySQL 同步配置文件')
    parser.add_argument('--mysql_user', default='root', help='MySQL 用户名 (默认: root)')

    # 使用固定值
    hive_host = "cdh01"
    hive_port = 10000
    mysql_host = "cdh01"
    mysql_port = "3306"
    mysql_password = "123456"
    hive_database = "gmall"
    mysql_database = "gmall"
    hive_metastore = "thrift://cdh01:9083"

    args = parser.parse_args()

    # 获取所有前缀为 ads 的表名
    print("正在获取 Hive gmall 数据库中前缀为 ads 的表列表...")
    tables = get_all_ads_tables(
        hive_host=hive_host,
        hive_port=hive_port,
        hive_database=hive_database
    )

    # 为每张表生成配置文件和 DDL
    for table in tables:
        print(f"\n处理表: {table}")

        # 获取表字段信息
        columns = get_hive_table_columns(
            hive_host=hive_host,
            hive_port=hive_port,
            hive_database=hive_database,
            table=table
        )

        if not columns:
            print(f"  → 警告: 未获取到字段信息，跳过此表")
            continue

        print(f"  → 找到 {len(columns)} 个字段")

        # 生成 MySQL DDL 语句
        mysql_ddl = generate_mysql_ddl(
            table=table,
            columns=columns
        )

        # 保存 MySQL DDL 到文件
        save_mysql_ddl_to_file(
            ddl=mysql_ddl,
            table=table
        )

        # 生成 SeaTunnel 配置文件
        generate_seatunnel_config(
            hive_metastore_uri=hive_metastore,
            mysql_host=mysql_host,
            mysql_port=mysql_port,
            mysql_user=args.mysql_user,
            mysql_password=mysql_password,
            table=table,
            columns=columns,
            database=mysql_database
        )

    print("\n所有文件生成完成!")
    print(f"配置文件: 当前目录下的 conf/<表名>_hive_to_mysql.conf")
    print(f"MySQL DDL: mysql_ddl_scripts 目录下的 <表名>.sql")