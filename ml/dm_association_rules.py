import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
import warnings
warnings.filterwarnings('ignore')

try:
    from mlxtend.preprocessing import TransactionEncoder
    from mlxtend.frequent_patterns import apriori, association_rules
except ImportError:
    print("Please install mlxtend: pip install mlxtend")
    import sys
    sys.exit(1)

def main():
    # 1. Setup paths
    input_path = "d:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "d:/HDFS/JOB_MARKET_BIGDATA/data/mining_results"
    plots_dir = os.path.join(output_dir, "plots")
    
    os.makedirs(plots_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "association_rules_report.txt")
    
    # 2. Load Data
    print("Loading data...")
    df = pd.read_csv(input_path)
    
    # Process skills
    df['skills_clean'] = df['skills_clean'].fillna('')
    # Split by comma and strip whitespace
    transactions = df['skills_clean'].apply(lambda x: [skill.strip() for skill in x.split(',') if skill.strip() != '']).tolist()
    
    # Remove empty transactions
    transactions = [t for t in transactions if len(t) > 0]
    
    # 3. Transaction Encoder
    print("Encoding transactions...")
    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    df_trans = pd.DataFrame(te_ary, columns=te.columns_)
    
    # 4. Apriori Algorithm
    print("Running Apriori...")
    # min_support = 0.05 means skill appears in at least 5% of jobs
    frequent_itemsets = apriori(df_trans, min_support=0.05, use_colnames=True)
    
    # 5. Association Rules
    print("Generating rules...")
    if len(frequent_itemsets) > 0:
        rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.3)
        # Filter lift > 1
        rules = rules[rules['lift'] > 1]
        rules = rules.sort_values('lift', ascending=False)
        
        # Save Report
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=== ASSOCIATION RULES REPORT ===\n\n")
            f.write(f"Total transactions: {len(transactions)}\n")
            f.write(f"Frequent Itemsets found: {len(frequent_itemsets)}\n")
            f.write(f"Rules generated (lift > 1): {len(rules)}\n\n")
            f.write("Top 20 Rules by Lift:\n")
            
            top_rules = rules.head(20)
            for idx, row in top_rules.iterrows():
                ant = ", ".join(list(row['antecedents']))
                con = ", ".join(list(row['consequents']))
                f.write(f"{ant} -> {con} (Conf: {row['confidence']:.2f}, Lift: {row['lift']:.2f})\n")
                
        print(f"Report saved to {report_path}")
        
        if top_rules.empty:
            print("No rules found with lift > 1. Skipping graphs.")
        else:
            # 6. Network Graph
            print("Generating Network Graph...")
            plt.figure(figsize=(12, 10))
            G = nx.DiGraph()
            
            # Use top 20 rules for graph clarity
            for idx, row in top_rules.iterrows():
                ant = ", ".join(list(row['antecedents']))
                con = ", ".join(list(row['consequents']))
                G.add_edge(ant, con, weight=row['lift'])
                
            pos = nx.spring_layout(G, k=1.5, seed=42)
            edges = G.edges()
            weights = [G[u][v]['weight'] for u,v in edges]
            
            nx.draw(G, pos, with_labels=True, node_color='lightblue', 
                    node_size=3000, font_size=10, font_weight='bold', 
                    edge_color=weights, edge_cmap=plt.cm.Blues, width=2,
                    arrowsize=20)
                    
            plt.title('Association Rules Network (Top 20 by Lift)')
            plt.tight_layout()
            plt.savefig(os.path.join(plots_dir, 'association_network.png'))
            plt.close()
            
            # 7. Heatmap
            print("Generating Heatmap...")
            # Get unique skills from top rules
            unique_skills = list(set([item for sublist in top_rules['antecedents'] for item in sublist] + 
                                     [item for sublist in top_rules['consequents'] for item in sublist]))
            
            # Create an empty matrix
            matrix = pd.DataFrame(index=unique_skills, columns=unique_skills).fillna(0)
            
            # Fill matrix with Lift values
            for idx, row in top_rules.iterrows():
                for ant in row['antecedents']:
                    for con in row['consequents']:
                        matrix.loc[ant, con] = float(row['lift'])
                        
            # Ensure matrix is numeric type before passing to seaborn
            matrix = matrix.astype(float)
            
            plt.figure(figsize=(10, 8))
            sns.heatmap(matrix, annot=True, cmap='YlGnBu', fmt='.2f')
            plt.title('Lift Heatmap of Top Association Rules')
            plt.tight_layout()
            plt.savefig(os.path.join(plots_dir, 'association_heatmap.png'))
            plt.close()
        
    else:
        print("No frequent itemsets found with the given min_support.")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=== ASSOCIATION RULES REPORT ===\n\n")
            f.write("No frequent itemsets found with min_support=0.05\n")

    print("All association rules tasks completed.")

if __name__ == "__main__":
    main()
