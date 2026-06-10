import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

def main():
    # 1. Setup paths
    input_path = "d:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "d:/HDFS/JOB_MARKET_BIGDATA/data/mining_results"
    plots_dir = os.path.join(output_dir, "plots")
    
    os.makedirs(plots_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "salary_prediction_report.txt")
    
    # 2. Load and Preprocess Data
    print("Loading data...")
    df = pd.read_csv(input_path)
    
    # Filter valid salary
    df = df[df['salary_final_vnd'] > 0].copy()
    
    # Fill NA for relevant columns
    df['job_level'] = df['job_level'].fillna('Unknown')
    df['location_clean'] = df['location_clean'].fillna('Unknown')
    df['yoe_extracted'] = df['yoe_extracted'].fillna(0)
    df['skill_count'] = df['skill_count'].fillna(0)
    df['is_remote'] = df['is_remote'].fillna(0)
    df['skills_clean'] = df['skills_clean'].fillna('')
    
    # Define features and target
    target = 'salary_final_vnd'
    y = df[target]
    
    # 3. Feature Engineering with ColumnTransformer
    categorical_features = ['job_level', 'location_clean']
    numeric_features = ['yoe_extracted', 'skill_count', 'is_remote']
    text_feature = 'skills_clean'
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', 'passthrough', numeric_features),
            ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features),
            ('tfidf', TfidfVectorizer(max_features=20), text_feature)
        ]
    )
    
    # Fit and transform features
    print("Engineering features...")
    X = preprocessor.fit_transform(df)
    
    # Get feature names for importance plotting later
    cat_feature_names = preprocessor.named_transformers_['cat'].get_feature_names_out(categorical_features)
    tfidf_feature_names = preprocessor.named_transformers_['tfidf'].get_feature_names_out()
    feature_names = numeric_features + list(cat_feature_names) + list(tfidf_feature_names)
    
    # 4. Train/Test Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 5. Define Models
    models = {
        "Linear Regression": LinearRegression(),
        "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42),
        "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
        "XGBoost": XGBRegressor(n_estimators=100, random_state=42)
    }
    
    results = {}
    best_model_name = None
    best_r2 = -float('inf')
    best_model = None
    best_predictions = None
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== SALARY PREDICTION REPORT ===\n\n")
        f.write(f"Total samples used: {len(df)}\n")
        f.write(f"Features used: {len(feature_names)}\n\n")
        
        # 6. Train & Evaluate
        print("Training models...")
        for name, model in models.items():
            print(f"Training {name}...")
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            
            mae = mean_absolute_error(y_test, y_pred)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            r2 = r2_score(y_test, y_pred)
            
            results[name] = {'MAE': mae, 'RMSE': rmse, 'R2': r2}
            
            f.write(f"Model: {name}\n")
            f.write(f"  MAE:  {mae:,.2f} VND\n")
            f.write(f"  RMSE: {rmse:,.2f} VND\n")
            f.write(f"  R2:   {r2:.4f}\n\n")
            
            if r2 > best_r2:
                best_r2 = r2
                best_model_name = name
                best_model = model
                best_predictions = y_pred
                
        f.write(f"Best Model: {best_model_name} (R2: {best_r2:.4f})\n")
    
    print(f"Reports saved to {report_path}")
    
    # 7. Plots
    print("Generating plots...")
    
    # Plot 1: Model Comparison (R2)
    plt.figure(figsize=(10, 6))
    sns.barplot(x=list(results.keys()), y=[res['R2'] for res in results.values()], palette='viridis')
    plt.title('Model Comparison - R² Score')
    plt.ylabel('R² Score')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'salary_model_comparison_r2.png'))
    plt.close()
    
    # Plot 2: Scatter Predicted vs Actual (Best Model)
    plt.figure(figsize=(10, 6))
    plt.scatter(y_test, best_predictions, alpha=0.5, color='blue')
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
    plt.title(f'Actual vs Predicted Salary ({best_model_name})')
    plt.xlabel('Actual Salary (VND)')
    plt.ylabel('Predicted Salary (VND)')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'salary_predicted_vs_actual.png'))
    plt.close()
    
    # Plot 3: Feature Importance (if applicable)
    if hasattr(best_model, 'feature_importances_'):
        importances = best_model.feature_importances_
        indices = np.argsort(importances)[-20:]  # Top 20 features
        
        plt.figure(figsize=(12, 8))
        plt.barh(range(len(indices)), importances[indices], align='center')
        plt.yticks(range(len(indices)), [feature_names[i] for i in indices])
        plt.title(f'Top 20 Feature Importances ({best_model_name})')
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, 'salary_feature_importance.png'))
        plt.close()
        
    print("All tasks completed.")

if __name__ == "__main__":
    main()
