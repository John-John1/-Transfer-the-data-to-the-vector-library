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
from IPython.display import JSON, Image

# ========================
# 3. Unstructured (文档解析)
# ========================
from unstructured.partition.html import partition_html
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.md import partition_md
from unstructured.partition.pdf import partition_pdf
from unstructured.staging.base import dict_to_elements
from unstructured.chunking.title import chunk_by_title

# ========================
# 4. Supabase / pgvector
# ========================
from supabase import create_client
import psycopg2

# ========================
# 5. LangChain 类型包装
# ========================
from langchain_core.documents import Document
from langchain.embeddings.base import Embeddings


# ========================
# 6. 你的百度向量模型（你只要补充 API 调用即可）
# ========================
class BaiduEmbeddings(Embeddings):
    def embed_documents(self, texts):
        # TODO：写你自己的调用百度API代码，输出二维数组
        # return [[float, float, ...], [...]]
        raise NotImplementedError("请填入百度API调用逻辑")

    def embed_query(self, text):
        # TODO：单条文本生成向量
        raise NotImplementedError("请填入百度API调用逻辑")


embeddings = BaiduEmbeddings()



# ==============================
# 7. 配置 Supabase / PostgreSQL
# ==============================
SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = "YOUR_SUPABASE_KEY"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

conn = psycopg2.connect(
    host="supabase-db",
    port=5432,
    database="postgres",
    user="postgres",
    password="YOUR_PASSWORD",
)
cur = conn.cursor()


# ========================
# 8. 文件解析函数
# ========================
def parse_file(filepath):
    ext = filepath.lower().split(".")[-1]

    if ext == "html":
        els = partition_html(filename=filepath)

    elif ext == "pptx":
        els = partition_pptx(filename=filepath)

    elif ext == "md":
        els = partition_md(filename=filepath)

    elif ext == "pdf":
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
        els = dict_to_elements(resp.elements)

    else:
        print("Unsupported file:", filepath)
        return []

    return els



# ========================
# 9. 批量解析文件夹
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
# 10. Chunk by Title
# ========================
def chunk_elements(elements):
    chunks = chunk_by_title(
        elements,
        combine_text_under_n_chars=100,
        max_characters=3000,
    )
    return chunks



# ========================
# 11. 转 Document + 填 metadata
# ========================
def convert_chunks_to_documents(chunks):
    docs = []

    for c in chunks:
        metadata = c.metadata.to_dict()

        # 清理无关字段
        if "languages" in metadata:
            del metadata["languages"]

        # 设置 source 字段用于过滤或混合搜索
        metadata["source"] = metadata.get("filename", "")

        docs.append(Document(
            page_content=c.text,
            metadata=metadata
        ))

    return docs



# ========================
# 12. 写入 Supabase (pgvector)
# ========================
def write_documents_to_supabase(documents):
    insert_sql = """
        INSERT INTO documents (content, metadata, embedding)
        VALUES (%s, %s, %s)
    """

    for doc in documents:
        text = doc.page_content
        metadata_json = json.dumps(doc.metadata)

        # 调百度向量模型生成 embedding
        vector = embeddings.embed_query(text)
        embedding_str = "[" + ",".join(str(x) for x in vector) + "]"

        cur.execute(insert_sql, (text, metadata_json, embedding_str))

    conn.commit()
    print("写入 Supabase 完成。")



# ========================
# 13. 主流程：解析 → 分块 → 向量 → 入库
# ========================
def ingest_folder_into_supabase(folder="./my_files"):
    print("\n===== 1. 解析所有文件 =====")
    elements = load_all_elements_from_folder(folder)

    print("\n===== 2. 按标题结构分块 =====")
    chunks = chunk_elements(elements)

    print("\n===== 3. 转 Document（附加自定义 metadata） =====")
    documents = convert_chunks_to_documents(chunks)

    print("\n===== 4. 写入 Supabase（pgvector） =====")
    write_documents_to_supabase(documents)

    print("\n=== 全流程完成！===")



# ========================
# 调用入口
# ========================
ingest_folder_into_supabase("./your_folder_containing_pdfs_html_pptx")
