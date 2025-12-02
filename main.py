import sys
import pdfminer.psparser
import pdfminer.pdfparser
# 检查 pdfparser 是否缺少 PSSyntaxError 类
if not hasattr(pdfminer.pdfparser, 'PSSyntaxError'):
    # 如果缺少，将 psparser 中的类加过去，防止报错
    pdfminer.pdfparser.PSSyntaxError = pdfminer.psparser.PSSyntaxError

import warnings
# 忽略运行时的警告信息
warnings.filterwarnings("ignore")

import os
import json
import time
import requests
import psycopg2

# 导入 Unstructured 相关解析库
from unstructured.partition.pdf import partition_pdf
from unstructured.partition.html import partition_html
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.md import partition_md
from unstructured.chunking.title import chunk_by_title
from unstructured.staging.base import dict_to_elements

# 导入 LangChain 相关库
from langchain_core.documents import Document
from langchain.embeddings.base import Embeddings

# ======================================================
# 百度 Embedding 类 (纯 API Key 直连)
# ======================================================
class BaiduEmbeddings(Embeddings):
    def __init__(self):
        # ⚠️⚠️⚠️ 在这里填入您 n8n 里使用的 API Key ⚠️⚠️⚠️
        self.API_KEY = "您的_API_KEY_粘贴在这里"
        
        # 设置百度 Embedding-V1 接口地址，固定维度 384
        self.url = "https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/embeddings/embedding-v1"

    def embed_query(self, text: str):
        # 检查文本是否为空，如果为空直接返回零向量
        if not text or not text.strip():
            return [0.0] * 384

        # 强制截断文本，因为百度 V1 模型限制 Token 数量
        # 这里限制 384 个字符，确保不会报 "prompt tokens too long"
        if len(text) > 384:
            text = text[:384]

        # 设置请求头，使用 Bearer Token 方式鉴权
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.API_KEY}" 
        }
        
        # 构造请求体，百度要求 input 必须是列表
        payload = {"input": [text]} 

        try:
            # 发送 POST 请求到百度服务器，设置 30 秒超时
            res = requests.post(self.url, headers=headers, json=payload, timeout=30).json()
            
            # 如果返回结果包含 data 且不为空，说明成功
            if "data" in res and len(res["data"]) > 0:
                # 提取并返回第一个向量
                return res["data"][0]["embedding"]
            
            # 如果返回包含错误码，打印错误信息
            if "error_code" in res:
                print(f"❌ 百度 API 报错: {res}")
            
            # 发生错误时的保底返回值 (384维零向量)
            return [0.0] * 384

        except Exception as e:
            # 捕获网络或其他异常，打印日志
            print(f"❌ 请求异常: {e}")
            # 返回保底零向量
            return [0.0] * 384

    # 批量处理接口，循环调用单个处理
    def embed_documents(self, texts):
        return [self.embed_query(t) for t in texts]

# 初始化嵌入模型实例
embeddings = BaiduEmbeddings()


# Unstructured API 配置
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared

# 初始化 Unstructured 客户端，使用 Serverless 地址
s = UnstructuredClient(
    api_key_auth="BAjhezCSAMEhJ9CrvLvJTVlbZjoT30",
    server_url="https://api.unstructuredapp.io"
)

# 通过 API 解析 PDF 文件
def parse_pdf_via_api(filepath):
    print("→ Strong API 解析 PDF:", filepath)
    # 设置最大重试次数
    max_retries = 3
    
    # 开始重试循环
    for attempt in range(max_retries):
        try:
            # 以二进制读取模式打开文件
            with open(filepath, "rb") as f:
                # 构造文件对象
                files = shared.Files(
                    content=f.read(),
                    file_name=os.path.basename(filepath),
                )
            
            # 设置解析参数
            req = shared.PartitionParameters(
                files=files,
                strategy="hi_res",          # 使用高精度策略
                hi_res_model_name="yolox",  # 使用 YOLOX 模型
                pdf_infer_table_structure=True, # 启用表格结构推断
                languages=["chi_sim"],      # 关键：指定简体中文，防止乱码
                skip_infer_table_types=[],  # 不跳过任何表格类型
            )
            
            # 调用 API 进行分区解析
            resp = s.general.partition(req)
            # 将响应转换为元素列表并返回
            return dict_to_elements(resp.elements)
            
        except Exception as e:
            # 打印当前尝试的错误信息
            print(f"⚠️ 解析重试 {attempt + 1}: {e}")
            # 如果不是最后一次尝试，等待 5 秒
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                # 如果所有尝试都失败，打印失败日志
                print("❌ API 解析失败，跳过文件")
                # 返回空列表
                return [] 

# 文件解析分发逻辑
def parse_file(filepath):
    # 获取文件扩展名并转为小写
    ext = filepath.lower().split(".")[-1]
    
    # 根据扩展名调用不同的解析函数
    if ext == "html": return partition_html(filename=filepath)
    elif ext == "pptx": return partition_pptx(filename=filepath)
    elif ext == "md": return partition_md(filename=filepath)
    elif ext == "pdf": return parse_pdf_via_api(filepath)
    else:
        # 不支持的文件类型，打印提示
        print("不支持的文件:", filepath)
        return []

# 遍历文件夹加载所有文件
def load_all_elements_from_folder(folder):
    all_elements = []
    # 检查文件夹是否存在
    if not os.path.exists(folder):
        return []
        
    # 遍历文件夹中的文件名
    for fname in os.listdir(folder):
        # 拼接完整路径
        path = os.path.join(folder, fname)
        # 确保是文件而不是文件夹
        if os.path.isfile(path):
            print("解析文件:", path)
            # 解析文件获取元素
            els = parse_file(path)
            # 将元素添加到总列表
            all_elements.extend(els)
    return all_elements

# 对元素进行切分
def chunk_elements(elements):
    return chunk_by_title(
        elements,
        # 如果文本少于50字符，尝试合并
        combine_text_under_n_chars=50,
        # 设置最大字符数为500，适应百度API长度限制
        max_characters=500, 
    )

# 将分块转换为文档
def convert_chunks_to_documents(chunks):
    docs = []
    for c in chunks:
        # 提取元数据
        meta = c.metadata.to_dict()
        # 确保 source 字段存在
        meta["source"] = meta.get("filename", "")
        # 去除文本两端的空白字符
        clean_text = c.text.strip()
        # 如果文本不为空，创建 Document 对象
        if clean_text:
            docs.append(Document(page_content=clean_text, metadata=meta))
    return docs

# 数据库操作逻辑
# 数据库连接配置字典
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "postgres",
    "user": "postgres",
    "password": "zydsslxd"
}

# 初始化数据库结构
def init_db():
    try:
        # 建立数据库连接
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True 
        cur = conn.cursor()
        
        # 启用向量插件 vector
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        
        # 定义建表 SQL，维度为 384（在没有库的情况下）
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS documents (
            id bigserial PRIMARY KEY,
            content text,
            metadata jsonb,
            embedding vector(384)
        );
        """
        # 执行建表语句
        cur.execute(create_table_sql)
        print("✅ 数据库准备就绪 (384维)")
        
        # 关闭游标和连接
        cur.close()
        conn.close()
    except Exception as e:
        # 打印初始化失败错误
        print(f"❌ 数据库初始化失败: {e}")
        exit()

# 将文档写入数据库
def write_documents_to_supabase(documents):
    # 如果没有文档，直接返回
    if not documents: return
    
    # 建立数据库连接
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # 插入数据的 SQL 命令模板
    sql = "INSERT INTO documents (content, metadata, embedding) VALUES (%s, %s, %s)"

    print(f"正在写入 {len(documents)} 条数据...")
    
    # 遍历所有文档
    for i, doc in enumerate(documents):
        try:
            # 获取文本内容
            txt = doc.page_content
            # 二次检查，如果内容为空则跳过
            if not txt or not txt.strip():
                continue

            # 将元数据转为 JSON 字符串
            meta_json = json.dumps(doc.metadata)
            # 调用百度 API 获取向量
            vector = embeddings.embed_query(txt)
            
            # 检查向量是否有效 (非全0)
            if any(vector):
                # 格式化向量为字符串格式 "[0.1, 0.2...]"
                vector_str = "[" + ",".join(str(v) for v in vector) + "]"
                # 执行插入操作
                cur.execute(sql, (txt, meta_json, vector_str))
            
            # 每处理 10 条数据提交一次
            if (i + 1) % 10 == 0:
                conn.commit()
                print(f"→ 已处理 {i + 1} 条")
                
        except Exception as e:
            # 捕获写入错误并打印
            print(f"❌ 写入失败: {e}")
            # 回滚事务，防止卡死
            conn.rollback()

    # 提交剩余的数据
    conn.commit()
    # 关闭连接
    conn.close()
    print("→ 全部完成")

# 主流程函数
def ingest_folder_into_supabase(folder="E:\documents"):
    #  初始化数据库表结构
    init_db()

    # 加载文件夹下的所有文件元素
    print("=== Load elements ===")
    elements = load_all_elements_from_folder(folder)

    # 如果没有提取到任何元素，结束程序
    if not elements:
        print("⚠️ 未提取到内容")
        return

    # 对元素进行切分
    print(f"=== Chunk ({len(elements)} elements) ===")
    chunks = chunk_elements(elements)

    # 将切分块转换为文档对象
    print(f"=== Build documents ({len(chunks)} chunks) ===")
    docs = convert_chunks_to_documents(chunks)

    # 将文档向量化并写入数据库
    print("=== Insert into DB ===")
    write_documents_to_supabase(docs)

    print("=== Done ===")

# 程序入口
if __name__ == "__main__":
    # 执行主流程，传入文档目录
    ingest_folder_into_supabase("E:\documents")
