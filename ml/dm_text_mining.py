import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import warnings
warnings.filterwarnings('ignore')

try:
    from wordcloud import WordCloud
except ImportError:
    print("Please install wordcloud: pip install wordcloud")
    import sys
    sys.exit(1)

def main():
    # 1. Setup paths
    input_path = "d:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "d:/HDFS/JOB_MARKET_BIGDATA/data/mining_results"
    plots_dir = os.path.join(output_dir, "plots")
    
    os.makedirs(plots_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "text_mining_report.txt")
    
    # 2. Load Data
    print("Loading data...")
    df = pd.read_csv(input_path)
    
    df['description_clean'] = df['description_clean'].fillna('')
    df['job_level'] = df['job_level'].fillna('Unknown')
    
    # Get text data
    text_data = df['description_clean'].tolist()
    
    # 3. Overall Word Cloud
    print("Generating Overall Word Cloud...")
    text_combined = " ".join(text_data)
    wordcloud = WordCloud(width=800, height=400, background_color='white', max_words=100).generate(text_combined)
    
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title('Overall Word Cloud from Job Descriptions')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'text_overall_wordcloud.png'))
    plt.close()
    
    # 4. TF-IDF and LDA
    print("Vectorizing text...")
    # Use CountVectorizer for LDA as it works better with word counts
    vectorizer = CountVectorizer(max_features=1000, stop_words='english', lowercase=True)
    X = vectorizer.fit_transform(text_data)
    feature_names = vectorizer.get_feature_names_out()
    
    num_topics = 5
    print(f"Running LDA with {num_topics} topics...")
    lda = LatentDirichletAllocation(n_components=num_topics, random_state=42)
    lda_output = lda.fit_transform(X)
    
    # Get top words for each topic
    n_top_words = 10
    topics_words = {}
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== TEXT MINING REPORT ===\n\n")
        f.write(f"Total documents: {len(text_data)}\n")
        f.write(f"Number of topics: {num_topics}\n\n")
        
        for topic_idx, topic in enumerate(lda.components_):
            top_features_ind = topic.argsort()[:-n_top_words - 1:-1]
            top_features = [feature_names[i] for i in top_features_ind]
            topics_words[f"Topic {topic_idx}"] = top_features
            
            f.write(f"Topic {topic_idx}:\n")
            f.write(", ".join(top_features) + "\n\n")
            
            # Generate Word Cloud for each topic
            topic_words_freq = {feature_names[i]: topic[i] for i in top_features_ind}
            wc = WordCloud(width=400, height=300, background_color='white').generate_from_frequencies(topic_words_freq)
            
            plt.figure(figsize=(6, 4))
            plt.imshow(wc, interpolation='bilinear')
            plt.axis('off')
            plt.title(f'Word Cloud for Topic {topic_idx}')
            plt.tight_layout()
            plt.savefig(os.path.join(plots_dir, f'text_topic_{topic_idx}_wordcloud.png'))
            plt.close()
            
    # Assign dominant topic to each document
    df['Dominant_Topic'] = np.argmax(lda_output, axis=1)
    
    # 5. Topic distribution by Job Level
    print("Generating Topic Distribution chart...")
    topic_dist = pd.crosstab(df['job_level'], df['Dominant_Topic'], normalize='index')
    
    # Plot stacked bar chart
    topic_dist.plot(kind='bar', stacked=True, figsize=(12, 6), colormap='viridis')
    plt.title('Topic Distribution by Job Level')
    plt.xlabel('Job Level')
    plt.ylabel('Proportion')
    plt.legend(title='Topic', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'text_topic_distribution.png'))
    plt.close()
    
    print(f"Reports saved to {report_path}")
    print("All text mining tasks completed.")

if __name__ == "__main__":
    main()
