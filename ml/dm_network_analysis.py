import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from itertools import combinations
from collections import Counter

def main():
    print("[INFO] Bắt đầu mô hình Phân tích mạng lưới kỹ năng (Skill Network Analysis)...")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_dir, "..", "data", "processed", "Data_ITJOB_Cleaned.csv")
    results_dir = os.path.join(current_dir, "..", "data", "mining_results")
    os.makedirs(results_dir, exist_ok=True)
    report_path = os.path.join(results_dir, "network_analysis_report.txt")
    plot_path = os.path.join(results_dir, "skill_network.png")
    
    if not os.path.exists(data_path):
        print(f"[ERROR] Không tìm thấy file: {data_path}")
        return

    df = pd.read_csv(data_path)
    df['skills_clean'] = df['skills_clean'].fillna('')
    
    # Generate co-occurrences
    print("[INFO] Đang xây dựng tập cạnh (edges)...")
    co_occurrences = Counter()
    for skills_str in df['skills_clean']:
        if not skills_str:
            continue
        skills = [s.strip() for s in skills_str.split(',') if s.strip()]
        # Create pairs
        for pair in combinations(sorted(skills), 2):
            co_occurrences[pair] += 1
            
    # Filter edges with weight >= 15 to keep graph manageable
    min_weight = 15
    edges = [(u, v, w) for (u, v), w in co_occurrences.items() if w >= min_weight]
    
    if not edges:
        print("[ERROR] Không đủ liên kết để vẽ đồ thị. Giảm min_weight xuống.")
        return
        
    print(f"[INFO] Tạo đồ thị với {len(edges)} cạnh (weight >= {min_weight})...")
    G = nx.Graph()
    G.add_weighted_edges_from(edges)
    
    # Calculate PageRank
    print("[INFO] Tính toán PageRank...")
    pagerank = nx.pagerank(G, weight='weight')
    top_pr = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:20]
    
    # Calculate Degree Centrality
    degree = nx.degree_centrality(G)
    top_degree = sorted(degree.items(), key=lambda x: x[1], reverse=True)[:20]
    
    # Plot Graph (Draw Top 40 nodes for visibility)
    top_nodes = [node for node, score in sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:40]]
    sub_G = G.subgraph(top_nodes)
    
    plt.figure(figsize=(14, 14))
    pos = nx.spring_layout(sub_G, k=0.4, seed=42)
    
    # Node size based on PageRank
    node_sizes = [pagerank[node] * 70000 for node in sub_G.nodes()]
    
    nx.draw_networkx_nodes(sub_G, pos, node_size=node_sizes, node_color='lightgreen', alpha=0.8, edgecolors='white')
    nx.draw_networkx_edges(sub_G, pos, width=1.2, alpha=0.4, edge_color='gray')
    nx.draw_networkx_labels(sub_G, pos, font_size=11, font_weight='bold')
    
    plt.title("Skill Network Co-occurrence Graph (Top 40 Skills by PageRank)", fontsize=16)
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(plot_path, dpi=300)
    plt.close()
    
    print("[INFO] Đang lưu báo cáo...")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== BÁO CÁO PHÂN TÍCH MẠNG LƯỚI KỸ NĂNG (NETWORK ANALYSIS) ===\n\n")
        f.write(f"Số lượng Nodes (kỹ năng): {G.number_of_nodes()}\n")
        f.write(f"Số lượng Edges (liên kết >= {min_weight}): {G.number_of_edges()}\n\n")
        
        f.write("--- TOP 20 KỸ NĂNG LÕI (THEO PAGERANK) ---\n")
        f.write("(Các kỹ năng mang tính chất nền tảng, liên kết tới nhiều kỹ năng quan trọng khác)\n")
        for i, (skill, score) in enumerate(top_pr, 1):
            f.write(f"{i:2d}. {skill:<25} | Điểm PageRank: {score:.4f}\n")
            
        f.write("\n--- TOP 20 KỸ NĂNG XUẤT HIỆN NHIỀU NHẤT CÙNG KỸ NĂNG KHÁC (DEGREE CENTRALITY) ---\n")
        for i, (skill, score) in enumerate(top_degree, 1):
            f.write(f"{i:2d}. {skill:<25} | Điểm Degree: {score:.4f}\n")
            
    print(f"[OK] Đã lưu báo cáo: {report_path}")
    print(f"[OK] Đã lưu đồ thị: {plot_path}")

if __name__ == "__main__":
    main()
