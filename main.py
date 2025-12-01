# ========================
# 1. 忽略 warning
# ========================
import warnings
warnings.filterwarnings("ignore")

# ========================
# 2. Python 基础库
# ========================
import os
import json
import requests

# ========================
# 3. Unstructured Strong API（PDF 高精度解析）
# ========================
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared
from unstructured.staging.base import dict_to_elements

# ========================
# 4. Unstructured 本地解析器（不导入 partition_pdf）
# ========================
from unstructured.partition.html import partition_html
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.md import partition_md
from unstructured.chunking.title import chunk_by_title

# ========================
# 5. PostgreSQL / pgvector
# ========================
import psycopg2

# ========================
# 6. LangChain 类型包装
# ========================
from langchain_core.documents import Document
from langchain.embeddings.base import Embeddings



# ======================================================
# 7. 百度向量模型（最新版 API-only）
# ======================================================
class BaiduEmbeddings(Embeddings):
    def __init__(self):
        # 保留你的 API，不改、不隐藏
        self.API_KEY = "bce-v3/ALTAK-GGlAZiiVpbSzn1mZZkl0U/8d2c1619ccfb488a569efedaa257f9e42aa74b83"
        self.url = "https://qianfan.baidubce.com/v2/embeddings"

    def embed_query(self, text: str):
        res = requests.post(
            self.url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.API_KEY}",
            },
            json={"input": text}
        ).json()

        return res["data"][0]["embedding"]

    def embed_documents(self, texts):
        return [self.embed_query(t) for t in texts]


embeddings = BaiduEmbeddings()



# ======================================================
# 8. Unstructured Strong API（你的 key）
# ======================================================
s = UnstructuredClient(
    api_key_auth="BAjhezCSAMEhJ9CrvLvJTVlbZjoT30",
    server_url="https://api.unstructured.io"
)



# ======================================================
# 9. PostgreSQL（真实 supabase-db，端口 5433）
# ======================================================
conn = psycopg2.connect(
    host="localhost",
    port=5433,             # ← 你映射的端口
    database="postgres",
    user="postgres",
    password="zydsslxd"    # ← 你的真实密码
)
cur = conn.cursor()



# ======================================================
# 10. Strong API 解析 PDF
# ======================================================
def parse_pdf_via_api(filepath):
    print("→ Strong API 解析 PDF:", filepath)

    with open(filepath, "rb") as f:
        filedata = f.read()
        files = shared.Files(
            content=filedata,
            file_name=os.path.basename(filepath),
        )

    req = shared.PartitionParameters(
        files=files,
        strategy="hi_res",
        hi_res_model_name="yolox",
        pdf_infer_table_structure=True,
        skip_infer_table_types=[],
    )

    resp = s.general.partition(req)
    return dict_to_elements(resp.elements)



# ======================================================
# 11. 通用解析器（按扩展名）
# ======================================================
def parse_file(filepath):
    ext = filepath.lower().split(".")[-1]

    if ext == "html":
        return partition_html(filename=filepath)

    elif ext == "pptx":
        return partition_pptx(filename=filepath)

    elif ext == "md":
        return partition_md(filename=filepath)

    elif ext == "pdf":
        return parse_pdf_via_api(filepath)

    else:
        print("⚠ Unsupported file type:", filepath)
        return []


# ======================================================
# 12. 批量解析目录
# ======================================================
def load_all_elements_from_folder(folder):
    all_elements = []
    for fname in os.listdir(folder):
        path = os.path.join(folder, fname)
        if os.path.isfile(path):
            print("解析文件:", path)
            els = parse_file(path)
            all_elements.extend(els)
    return all_elements



# ======================================================
# 13. 分块（chunk_by_title）
# ======================================================
def chunk_elements(elements):
    return chunk_by_title(
        elements,
        combine_text_under_n_chars=100,
        max_characters=3000,
    )



# ======================================================
# 14. Element → LangChain Document
# ======================================================
def convert_chunks_to_documents(chunks):
    docs = []
    for c in chunks:
        meta = c.metadata.to_dict()
        meta["source"] = meta.get("filename", "")
        docs.append(Document(page_content=c.text, metadata=meta))
    return docs



# ======================================================
# 15. 写入 PostgreSQL + pgvector
# ======================================================
def write_documents_to_supabase(documents):
    sql = """
        INSERT INTO documents (content, metadata, embedding)
        VALUES (%s, %s, %s)
    """

    for doc in documents:
        text = doc.page_content
        metadata_json = json.dumps(doc.metadata)

        # 调用百度向量模型
        vector = embeddings.embed_query(text)
        vector_str = "[" + ",".join(str(v) for v in vector) + "]"

        cur.execute(sql, (text, metadata_json, vector_str))

    conn.commit()
    print("→ 数据写入完成")



# ======================================================
# 16. 主流程
# ======================================================
def ingest_folder_into_supabase(folder="E:/documents"):
    print("=== 1. 解析目录 ===")
    elements = load_all_elements_from_folder(folder)

    print("=== 2. 分块 ===")
    chunks = chunk_elements(elements)

    print("=== 3. 构建 Document ===")
    docs = convert_chunks_to_documents(chunks)

    print("=== 4. 写入数据库 ===")
    write_documents_to_supabase(docs)

    print("=== 全流程完成 ===")



# ======================================================
# 17. 程序入口
# ======================================================
ingest_folder_into_supabase("E:/documents")
