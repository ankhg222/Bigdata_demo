import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn
import os

app = FastAPI(title="Job Recommendation API")

# Mount thư mục static
static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir)

app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Cấu trúc request
class RecommendRequest(BaseModel):
    user_profile: str
    top_k: int = 5

# Global variables
model = None
df = None
job_embeddings = None

@app.on_event("startup")
def load_resources():
    global model, df, job_embeddings
    print("Loading data and model...")
    try:
        df = pd.read_csv("job_data.csv")
        job_embeddings = np.load("job_embeddings.npy")
        model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        print(f"Data loaded: {df.shape[0]} jobs")
    except Exception as e:
        print(f"Error loading resources: {e}")

@app.get("/")
def read_index():
    return FileResponse(os.path.join(static_dir, "index.html"))

@app.post("/api/recommend")
def recommend_jobs(request: RecommendRequest):
    if df is None or job_embeddings is None or model is None:
        raise HTTPException(status_code=500, detail="Mô hình hoặc dữ liệu chưa được tải.")
    
    try:
        # Generate embedding
        user_embedding = model.encode([request.user_profile])
        
        # Calculate cosine similarity
        similarities = cosine_similarity(user_embedding, job_embeddings)[0]
        
        # Lấy top_k jobs
        top_k = min(request.top_k, len(df))
        top_indices = np.argsort(similarities)[::-1][:top_k]
        
        # Build response
        results = []
        for idx in top_indices:
            if idx >= len(df):
                continue
            row = df.iloc[idx]
            
            # Xử lý các cột NaN nếu có
            def safe_get(val):
                return str(val) if pd.notna(val) else ""
            
            original_salary = safe_get(row.get('salary', ''))
            
            if "Y-u'll l-ve i-USD" in original_salary or not original_salary or original_salary.lower() == 'nan':
                display_salary = "Thương lượng"
            else:
                display_salary = original_salary
                if "đ" in display_salary.lower() or "vnd" in display_salary.lower():
                    display_salary = display_salary.replace(" USD", "").replace("USD", "").strip()

            results.append({
                "title": safe_get(row.get('title_clean', row.get('title', 'Unknown'))),
                "company": safe_get(row.get('company', 'Unknown Company'))[:50], # Truncate long names
                "skills": safe_get(row.get('skills_clean', row.get('skills', 'No skills listed'))),
                "salary": display_salary,
                "url": safe_get(row.get('url', '#')),
                "similarity_score": round(float(similarities[idx]), 4)
            })
            
        return {"recommendations": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    print("Khởi động server trên http://localhost:8000")
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
