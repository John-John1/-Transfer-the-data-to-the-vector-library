# ========================
# 1. 忽略 warning
# ========================
import warnings
warnings.filterwarnings("ignore")

# ========================
# 2. 基础库
# ========================
import os
import json
import requests
from IPython.display import JSON, Image

# ========================
# 3. Unstructured Strong API（用于 PDF hi_res yolox）
# ========================
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared

# ========================
# 4. Unstructured 本地解析器
# ========================
from unstructured.partition.html import partition_html
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.md import partition_md
from unstructured.partition.pdf import partition_pdf
from unstructured.staging.base import dict_to_elements

from unstructured.chunking.title import chunk_by_title

# ========================
# 5. Supabase / pgvector
# ========================
from supabase import create_client
import psycopg2

# ========================
# 6. LangChain 类型包装
# ========================
from langchain_core.documents import Document
from langchain.embeddings.base import Embeddings


# ========================
# 7. 百度向量模型（已实现）
# ========================
class BaiduEmbeddings(Embeddings):
    def __init__(self):
        self.API_KEY = "YOUR_BAIDU_API_KEY"
        self.SECRET_KEY = "YOUR_BAIDU_SECRET_KEY"
        self.token = self.fetch_access_token()

    def fetch_access_token(self):
        """ 获取百度 access token """
        url = "https://aip.baidubce.com/oauth/2.0/token"
        params = {
            "grant_type": "client_credentials",
            "client_id": self.API_KEY,
            "client_secret": self.SECRET_KEY
        }
        response = requests.post(url, params=params).json()
        return response["access_token"]

    def embed_query(self, text):
        """ 单条向量 """
        url = f"https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/embeddings/embedding-v1?access_token={self.token}"
        payload = {"input": text}
        res = requests.post(url, json=payload).json()
        return res["data"][0]["embedding"]

    def embed_documents(self, texts):
        """ 多条向量 """
        return [self.embed_query(t) for t in texts]


embeddings = BaiduEmbeddings()


# ========================
# 8. Unstructured Strong API 客户端
# ========================
s = UnstructuredClient(
    api_key_auth="YOUR_UNSTRUCTURED_API_KEY",       # 如果不用 PDF 强视觉解析，可随便填
    server_url="https://api.unstructured.io",
)


# ==============================
# 9. Supabase / PostgreSQL 配置
# ==============================
SUPABASE_URL = "http://localhost:54321"
SUPABASE_KEY = "<你的service_role_key>"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

conn = psycopg2.connect(
    host="supabase-db",    # 你用 docker compose 时这是正确的
    port=5432,
    database="postgres",
    user="postgres",
    password="zydsslxd",
)
cur = conn.cursor()


# ========================
# 10. 文件解析函数
# ========================
def parse_file(filepath):
    ext = filepath.lower().split(".")[-1]

    if ext == "html":
        return partition_html(filename=filepath)

    elif ext == "pptx":
        return partition_pptx(filename=filepath)

    elif ext == "md":
        return partition_md(filename=filepath)

    elif ext == "pdf":
        try:
            with open(filepath, "rb") as f:
                files = shared.Files(content=f.read(), file_name=filepath)

            req = shared.PartitionParameters(
                files=files,
                strategy="hi_res",
                hi_res_model_name="yolox",
                pdf_infer_table_structure=True,
                skip_infer_table_types=[],
            )

            resp = s.general.partition(req)
            return dict_to_elements(resp.elements)

        except Exception:
            # 如果 Strong API 出错，回退为本地解析
            print("⚠ Strong API 解析失败，切换到本地 partition_pdf（fast）")
            return partition_pdf(filename=filepath, strategy="fast")

    else:
        print("Unsupported file:", filepath)
        return []


# ========================
# 11. 批量解析目录
# ========================
def load_all_elements_from_folder(folder):
    all_elements = []
    for fname in os.listdir(folder):
        path = os.path.join(folder, fname)
        if os.path.isfile(path):
            print("解析文件:", path)
            els = parse_file(path)
            all_elements.extend(els)
    return all_elements


# ========================
# 12. 分块
# ========================
def chunk_elements(elements):
    return chunk_by_title(elements, combine_text_under_n_chars=100, max_characters=3000)


# ========================
# 13. 转 Document
# ========================
def convert_chunks_to_documents(chunks):
    docs = []
    for c in chunks:
        metadata = c.metadata.to_dict()
        metadata["source"] = metadata.get("filename", "")
        docs.append(Document(page_content=c.text, metadata=metadata))
    return docs


# ========================
# 14. 写入数据库
# ========================
def write_documents_to_supabase(documents):
    sql = """
        INSERT INTO documents (content, metadata, embedding)
        VALUES (%s, %s, %s)
    """

    for doc in documents:
        content = doc.page_content
        meta = json.dumps(doc.metadata)
        vec = embeddings.embed_query(content)
        vec_str = "[" + ",".join(str(x) for x in vec) + "]"
        cur.execute(sql, (content, meta, vec_str))

    conn.commit()
    print("写入完成")


# ========================
# 15. 主入口
# ========================
def ingest_folder_into_supabase(folder="E:/documents"):
    print("====== 解析文件夹 ======")
    elements = load_all_elements_from_folder(folder)

    print("====== 分块 ======")
    chunks = chunk_elements(elements)

    print("====== 转 Document ======")
    docs = convert_chunks_to_documents(chunks)

    print("====== 写入 Supabase ======")
    write_documents_to_supabase(docs)

    print("=== 全流程完成 ===")


# 运行
ingest_folder_into_supabase("E:/documents")
