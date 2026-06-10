import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import warnings
warnings.filterwarnings('ignore')

def main():
    # 1. Setup paths
    input_path = "d:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "d:/HDFS/JOB_MARKET_BIGDATA/data/mining_results"
    plots_dir = os.path.join(output_dir, "plots")
    
    os.makedirs(plots_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "clustering_report.txt")
    
    # 2. Load and Preprocess Data
    print("Loading data...")
    df = pd.read_csv(input_path)
    
    # Fill NAs
    df['salary_final_vnd'] = df['salary_final_vnd'].fillna(df['salary_final_vnd'].median())
    df['yoe_extracted'] = df['yoe_extracted'].fillna(0)
    df['skill_count'] = df['skill_count'].fillna(0)
    df['is_remote'] = df['is_remote'].fillna(0)
    df['job_level'] = df['job_level'].fillna('Unknown')
    
    # Encode Job Level
    le = LabelEncoder()
    df['job_level_encoded'] = le.fit_transform(df['job_level'])
    
    features = ['salary_final_vnd', 'yoe_extracted', 'skill_count', 'is_remote', 'job_level_encoded']
    X = df[features]
    
    # 3. Scaling and PCA
    print("Scaling and PCA...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    
    # 4. Elbow Method
    print("Calculating Elbow method...")
    inertia = []
    K_range = range(2, 11)
    for k in K_range:
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(X_scaled)
        inertia.append(kmeans.inertia_)
        
    plt.figure(figsize=(8, 5))
    plt.plot(K_range, inertia, marker='o')
    plt.title('Elbow Method For Optimal K')
    plt.xlabel('Number of Clusters (K)')
    plt.ylabel('Inertia')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'cluster_elbow.png'))
    plt.close()
    
    # 5. K-Means Clustering (Using K=4 as default)
    optimal_k = 4
    print(f"Applying K-Means with K={optimal_k}...")
    kmeans = KMeans(n_clusters=optimal_k, random_state=42)
    clusters = kmeans.fit_predict(X_scaled)
    df['Cluster'] = clusters
    
    # 6. Evaluate
    sil_score = silhouette_score(X_scaled, clusters)
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== JOB MARKET CLUSTERING REPORT ===\n\n")
        f.write(f"Total samples: {len(df)}\n")
        f.write(f"Optimal K selected: {optimal_k}\n")
        f.write(f"Silhouette Score: {sil_score:.4f}\n\n")
        
        f.write("Cluster Profiles (Mean values):\n")
        cluster_profiles = df.groupby('Cluster')[features].mean()
        f.write(cluster_profiles.to_string())
        f.write("\n")
        
    print(f"Reports saved to {report_path}")
    
    # 7. Scatter plot PCA
    print("Generating PCA scatter plot...")
    plt.figure(figsize=(10, 8))
    sns.scatterplot(x=X_pca[:, 0], y=X_pca[:, 1], hue=clusters, palette='Set1', legend='full', s=60, alpha=0.7)
    plt.title(f'K-Means Clusters in PCA 2D Space (K={optimal_k})')
    plt.xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.2%} variance)')
    plt.ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.2%} variance)')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'cluster_pca_scatter.png'))
    plt.close()
    
    # 8. Radar Chart
    print("Generating Radar Chart...")
    # Get mean of scaled features for radar chart to be on same scale
    cluster_centers = pd.DataFrame(kmeans.cluster_centers_, columns=features)
    
    # Min-max scale the centers for better radar visualization (range 0 to 1)
    from sklearn.preprocessing import MinMaxScaler
    radar_scaler = MinMaxScaler()
    centers_radar = radar_scaler.fit_transform(cluster_centers)
    
    angles = np.linspace(0, 2 * np.pi, len(features), endpoint=False).tolist()
    angles += angles[:1]
    
    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))
    for i in range(optimal_k):
        values = centers_radar[i].tolist()
        values += values[:1]
        ax.plot(angles, values, linewidth=2, label=f'Cluster {i}')
        ax.fill(angles, values, alpha=0.1)
        
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_thetagrids(np.degrees(angles[:-1]), features)
    
    plt.legend(loc='upper right', bbox_to_anchor=(0.1, 0.1))
    plt.title('Cluster Profiles (Normalized)')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'cluster_radar.png'))
    plt.close()
    
    print("All clustering tasks completed.")

if __name__ == "__main__":
    main()
