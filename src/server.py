import random
import matplotlib.pyplot as plt
import requests
from mcp.server.fastmcp import FastMCP
import pyarrow.csv as pacsv
import pyarrow.parquet as papq
import pyarrow as pa
import pandas as pd  # Import pandas
import os
from typing import Any
# Create server
mcp = FastMCP("Echo Server")


@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    print(f"[debug-server] add({a}, {b})")
    return a + b


@mcp.tool()
def get_secret_word() -> str:
    print("[debug-server] get_secret_word()")
    return random.choice(["apple", "banana", "cherry"])


@mcp.tool()
def get_current_weather(city: str) -> str:
    print(f"[debug-server] get_current_weather({city})")

    endpoint = "https://wttr.in"
    response = requests.get(f"{endpoint}/{city}")
    return response.text

@mcp.tool()
def read_csv_with_arrow(file_path: str, display_data: bool = True, to_parquet: bool = False, parquet_path: str = None) -> pa.Table | None:
    """
    使用 Apache Arrow 读取 CSV 文件，并可选择显示数据和/或转换为 Parquet 格式。

    Args:
        file_path: CSV 文件的路径。
        display_data: 是否显示读取的数据（前几行）。默认为 True。
        to_parquet: 是否将 CSV 数据转换为 Parquet 格式。默认为 False。
        parquet_path:  如果 to_parquet 为 True，则指定保存 Parquet 文件的路径。
                       如果为 None，则使用 CSV 文件名加上 .parquet 扩展名。

    Returns:
        读取的 Arrow Table 对象。如果发生错误，返回 None。
    """
    try:
        # 读取 CSV 文件
        table = pacsv.read_csv(file_path)

        if display_data:
            print("CSV Data (First 5 rows):")
            # 显示前5行，如果数据少于5行，则显示所有行
            print(table.to_pandas().head())

        if to_parquet:
            if parquet_path is None:
                # 如果未提供 Parquet 路径，则根据 CSV 文件名生成
                parquet_path = file_path.rsplit('.', 1)[0] + '.parquet'  # 去掉 .csv 并加上 .parquet
            papq.write_table(table, parquet_path)
            print(f"CSV data converted and saved to Parquet: {parquet_path}")

        return table

    except pa.ArrowInvalid as e:
        print(f"Error reading CSV file: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
  
@mcp.tool()
def filter_arrow_table_by_expr(table: Any, filter_expr: str) -> pa.Table:
    """
    使用字符串表达式对 Arrow Table 进行筛选。

    Args:
        table: 要筛选的 Arrow Table。
        filter_expr: 字符串表达式，例如 "age > 30 and country == 'China'"

    Returns:
        过滤后的 Arrow Table。
    """
    try:
        # 将 Arrow Table 转为 Pandas DataFrame

         # 如果是 dict，先转成 Arrow Table
        if isinstance(table, dict):
            print("Received dict, converting to pyarrow.Table...")
            table = pa.Table.from_pandas(pd.DataFrame(table))
         # 如果是 pandas.DataFrame，也处理一下
        elif isinstance(table, pd.DataFrame):
            table = pa.Table.from_pandas(table)

        
        df = table.to_pandas()

        # 使用 query 方法进行筛选
        filtered_df = df.query(filter_expr)

        # 转回 Arrow Table
        return pa.Table.from_pandas(filtered_df)
    except Exception as e:
        print(f"Error in filtering with expression '{filter_expr}': {e}")
        return table  # 返回原始数据以避免程序崩溃

@mcp.tool()
def plot_dict_and_save(data: dict, 
                       title: str = "Dict Plot", 
                       xlabel: str = "Keys", 
                       ylabel: str = "Values", 
                       save_path: str = None):
    """
    读取一个字典并绘制柱状图，然后保存为 PNG 图片文件。

    参数:
        data (dict): 要绘图的数据，格式为 {key: value}
        title (str): 图表标题
        xlabel (str): x轴标签
        ylabel (str): y轴标签
        save_path (str): PNG 图片保存路径（包括文件名）
    """
    if not isinstance(data, dict):
        raise ValueError("输入数据必须是字典(dict)类型。")
    
    # 提取键和值
    keys = list(data.keys())
    values = list(data.values())
    
    # 创建图形
    plt.figure(figsize=(8, 5))
    plt.bar(keys, values, color='skyblue', edgecolor='black')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    # 创建目录（如果不存在）
    dir_name = os.path.dirname(save_path)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # 保存图像
    plt.savefig(save_path, format='png')
    plt.close()  # 关闭图像，释放内存
    print(f"图像已保存为: {save_path}")


@mcp.tool()
def write_table_to_csv(table: Any, output_path: str) -> bool:
    """
    将数据 写入 CSV 文件。

    Args:
        table: 要写入的 Arrow Table。
        output_path: 输出的 CSV 文件路径。

    Returns:
        写入成功返回 True，失败返回 False。
    """
    print(f"Read table with {table.num_rows} rows and {table.num_columns} columns.")
    try:
         # 自动转换 dict 或 DataFrame 为 pyarrow.Table
        if isinstance(table, dict):
            table = pa.Table.from_pandas(pd.DataFrame(table))
        elif isinstance(table, pd.DataFrame):
            table = pa.Table.from_pandas(table)

        if not isinstance(table, pa.Table):
            raise TypeError("Provided data is not a valid pyarrow.Table")

        pacsv.write_csv(table, output_path)
        print(f"Table successfully written to CSV: {output_path}")
        return True
    except Exception as e:
        print(f"Failed to write table to CSV: {e}")
        return False
    
if __name__ == "__main__":
    mcp.run(transport="sse")