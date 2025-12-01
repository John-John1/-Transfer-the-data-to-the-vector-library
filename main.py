import warnings
warnings.filterwarnings("ignore")

import os
import json
import requests
import psycopg2

# ========== Strong API 配置 ==========
UNS_API_KEY = "YOUR_UNSTRUCTURED_API_KEY"     # ← 你填你的 Strong API Key
UNS_API_URL = "https://platform.unstructured.io/api/v1/general"

# ========== 百度向量模型 ==========
class BaiduEmbeddings:
    def __init__(self):
        self.API_KEY = "YOUR_BAIDU_API_KEY"    # ← 你填你的百度 key
        self.url = "https://qianfan.baidubce.com/v2/embeddings"

    def embed_query(self, text):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.API_KEY}"
        }
        payload = {"input": text}

        resp = requests.post(self.url, headers=headers, json=payload).json()
        return resp["data"][0]["embedding"]

embeddings = BaiduEmbeddings()


# ========== Supabase PGVector 连接（你的主机+端口） ==========
conn = psycopg2.connect(
    host="localhost",      # ← 你 WSL 映射出来的地址
    port=5433,             # ← 你 docker-compose 映射的端口
    database="postgres",
    user="postgres",
    password="zydsslxd"
)
cur = conn.cursor()


# ================================================================
# 1. Strong API 解析 PDF
# ================================================================
def parse_pdf_strong(filepath):
    print("→ Strong API 解析 PDF:", filepath)

    with open(filepath, "rb") as f:
        files = {
            "files": (
                os.path.basename(filepath),
                f,
                "application/pdf"
            )
        }

        data = {
            "strategy": "hi_res",
            "hi_res_model_name": "yolox",
            "pdf_infer_table_structure": True,
            "skip_infer_table_types": [],
        }

        headers = {
            "unstructured-api-key": UNS_API_KEY,
            "Accept": "application/json"
        }

        resp = requests.post(
            UNS_API_URL,
            headers=headers,
            data={"json": json.dumps(data)},
            files=files,
            timeout=600
        )

    if resp.status_code != 200:
        print("❌ Strong API 错误：", resp.text)
        return []

    result = resp.json()
    elements = result.get("elements", [])
    print(f"✓ 解析完成：获得 {len(elements)} 个 Element")
    return elements


# ================================================================
# 2. 分块（不依赖 unstructured-inference，只简单按标题分块）
# ================================================================
def chunk_elements(elements):
    chunks = []
    current = {"text": "", "metadata": {}}

    for el in elements:
        if el.get("type") == "Title":  # 遇到标题就开始一个新块
            if current["text"].strip():
                chunks.append(current)
            current = {"text": el.get("text", "") + "\n", "metadata": el.get("metadata", {})}
        else:
            current["text"] += el.get("text", "") + "\n"

    if current["text"].strip():
        chunks.append(current)

    print(f"✓ 分块完成：{len(chunks)} 个 chunk")
    return chunks


# ================================================================
# 3. 写入 Supabase (pgvector)
# ================================================================
def write_to_pgvector(chunks):
    sql = """
        INSERT INTO documents (content, metadata, embedding)
        VALUES (%s, %s, %s)
    """

    for c in chunks:
        text = c["text"]
        metadata_json = json.dumps(c["metadata"], ensure_ascii=False)

        vector = embeddings.embed_query(text)
        vector_str = "[" + ",".join(str(v) for v in vector) + "]"

        cur.execute(sql, (text, metadata_json, vector_str))

    conn.commit()
    print("✓ 写入 Supabase 完成")


# ================================================================
# 4. 主入口
# ================================================================
def ingest_folder(folder_path):
    print("=== 开始解析目录 ===")

    for fname in os.listdir(folder_path):
        path = os.path.join(folder_path, fname)
        if not os.path.isfile(path):
            continue

        if fname.lower().endswith(".pdf"):
            elements = parse_pdf_strong(path)
            chunks = chunk_elements(elements)
            write_to_pgvector(chunks)

    print("=== 全部处理完成 ===")


# 运行
ingest_folder("E:/documents")
