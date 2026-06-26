import os
import sys
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Add scripts directory to path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts"))
from db_utils import get_db_connection

# Page configuration
st.set_page_config(
    page_title="Hyperlocal Dark Store Ops Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling for Dark Theme & Glassmorphism feel
st.markdown("""
<style>
    .reportview-container {
        background: #0F172A;
    }
    .metric-card {
        background-color: #1E293B;
        border: 1px solid #334155;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: bold;
        color: #38BDF8;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #94A3B8;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }
</style>
""", unsafe_allow_html=True)

# Helper to fetch data
def run_query(query, params=None):
    conn = get_db_connection()
    try:
        df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Database query error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# Title
st.title("⚡ Hyperlocal Dark Store Ops Dashboard")
st.subheader("Real-time Demand Forecasting Pipeline — Bangalore Dark Stores")

# Tabs
tab1, tab2, tab3 = st.tabs(["🗺️ Demand Forecast Map", "📈 Model Performance", "🔧 Pipeline Health"])

# ----------------- Tab 1: Demand Forecast Map -----------------
with tab1:
    st.header("Hourly Demand Forecast")
    
    # Get latest predictions
    pred_data = run_query("""
        SELECT zone_id, forecast_hour, predicted_order_count, confidence_flag, generated_at 
        FROM predictions 
        WHERE generated_at = (SELECT MAX(generated_at) FROM predictions)
        ORDER BY zone_id, forecast_hour
    """)
    
    if pred_data.empty:
        st.warning("No prediction data found. Please run the forecasting DAG.")
    else:
        latest_updated = pd.to_datetime(pred_data['generated_at'].iloc[0]).strftime("%Y-%m-%d %H:%M:%S")
        st.info(f"Last Updated: **{latest_updated}**")
        
        # Get unique forecast horizons (usually two hours)
        forecast_hours = sorted(pred_data['forecast_hour'].unique())
        
        if len(forecast_hours) >= 2:
            horizon_options = {
                f"Next 1 Hour ({pd.to_datetime(forecast_hours[0]).strftime('%I:%M %p')})": forecast_hours[0],
                f"Next 2 Hours ({pd.to_datetime(forecast_hours[1]).strftime('%I:%M %p')})": forecast_hours[1]
            }
        else:
            horizon_options = {f"Next Hour ({pd.to_datetime(fh).strftime('%I:%M %p')})": fh for fh in forecast_hours}
            
        horizon_label = st.selectbox("Select Forecast Horizon:", list(horizon_options.keys()))
        selected_hour = horizon_options[horizon_label]
        
        # Filter predictions for selected hour
        hour_preds = pred_data[pred_data['forecast_hour'] == selected_hour].copy()
        
        # Fetch 7-day averages for color-coding
        avg_7d_df = run_query("""
            SELECT zone_id, AVG(order_count) as avg_orders 
            FROM features 
            WHERE timestamp >= (SELECT MAX(timestamp) FROM features) - INTERVAL '7 days'
            GROUP BY zone_id
        """)
        
        if not avg_7d_df.empty:
            hour_preds = pd.merge(hour_preds, avg_7d_df, on='zone_id', how='left')
        else:
            hour_preds['avg_orders'] = 5.0  # default baseline
            
        # Color coding logic:
        # Green: Normal, Amber: >1.2x Avg, Red: >1.5x Avg
        def get_color(row):
            pred = row['predicted_order_count']
            avg = row['avg_orders'] if pd.notna(row['avg_orders']) else 5.0
            if pred > 1.5 * avg:
                return 'High Demand (Red)'
            elif pred > 1.2 * avg:
                return 'Elevated (Amber)'
            else:
                return 'Normal (Green)'
                
        hour_preds['status'] = hour_preds.apply(get_color, axis=1)
        
        # Color map for plot
        color_map = {
            'High Demand (Red)': '#EF4444',
            'Elevated (Amber)': '#F59E0B',
            'Normal (Green)': '#10B981'
        }
        
        # Plotly Horizontal Bar Chart
        fig = px.bar(
            hour_preds,
            x='predicted_order_count',
            y='zone_id',
            color='status',
            color_discrete_map=color_map,
            orientation='h',
            labels={
                'predicted_order_count': 'Predicted Orders',
                'zone_id': 'Delivery Zone',
                'status': 'Demand Status'
            },
            title=f"Predicted Order Volume per Zone — {horizon_label}",
            category_orders={"zone_id": ["Z5", "Z4", "Z3", "Z2", "Z1"]} # Z1 at top
        )
        
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#E2E8F0',
            xaxis=dict(showgrid=True, gridcolor='#334155'),
            yaxis=dict(showgrid=False)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Display Grid Metrics
        cols = st.columns(len(hour_preds))
        for idx, row in hour_preds.reset_index(drop=True).iterrows():
            zone = row['zone_id']
            pred = row['predicted_order_count']
            avg = row['avg_orders']
            status = row['status']
            
            with cols[idx]:
                color_hex = color_map[status]
                st.markdown(f"""
                <div class="metric-card" style="border-top: 4px solid {color_hex};">
                    <div class="metric-label">{zone} Forecast</div>
                    <div class="metric-value" style="color: {color_hex};">{pred:.1f}</div>
                    <div style="font-size: 0.8rem; color: #64748B; margin-top: 5px;">
                        7-Day Avg: {avg:.1f} orders
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
        # Recent Alerts
        st.subheader("🚨 Active Dispatch Alerts")
        alerts_df = run_query("""
            SELECT zone_id, message, created_at 
            FROM alerts 
            ORDER BY created_at DESC 
            LIMIT 5
        """)
        
        if alerts_df.empty:
            st.success("No active dispatch alerts. All stores operating normally.")
        else:
            for _, alert in alerts_df.iterrows():
                st.warning(f"**[{alert['created_at'].strftime('%H:%M')}] Zone {alert['zone_id']}**: {alert['message']}")

# ----------------- Tab 2: Model Performance -----------------
with tab2:
    st.header("Model Performance & Retraining Log")
    
    runs_df = run_query("""
        SELECT run_id, timestamp, train_rows, val_rows, mape, rmse, mae, promoted, 
               COALESCE(baseline_mape, 28.5) as baseline_mape
        FROM model_runs 
        ORDER BY timestamp DESC
    """)
    
    if runs_df.empty:
        st.warning("No model training runs found. Please run the model training DAG.")
    else:
        # Metrics KPI
        latest_run = runs_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Latest Model MAPE", f"{latest_run['mape']:.2f}%", delta=f"{latest_run['mape'] - latest_run['baseline_mape']:.2f}%", delta_color="inverse")
        c2.metric("Latest Model RMSE", f"{latest_run['rmse']:.2f}")
        c3.metric("Training Rows", f"{latest_run['train_rows']:,}")
        c4.metric("Validation Rows", f"{latest_run['val_rows']:,}")
        
        # Line chart of MAPE over time
        st.subheader("Model Error (MAPE) Trend")
        runs_plot = runs_df.sort_values(by='timestamp')
        
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=runs_plot['timestamp'],
            y=runs_plot['mape'],
            mode='lines+markers',
            name='Model MAPE',
            line=dict(color='#38BDF8', width=3),
            marker=dict(size=8)
        ))
        fig_trend.add_trace(go.Scatter(
            x=runs_plot['timestamp'],
            y=runs_plot['baseline_mape'],
            mode='lines',
            name='Naive Baseline MAPE',
            line=dict(color='#EF4444', dash='dash')
        ))
        
        fig_trend.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#E2E8F0',
            xaxis=dict(showgrid=True, gridcolor='#334155'),
            yaxis=dict(showgrid=True, gridcolor='#334155', title="MAPE (%)")
        )
        st.plotly_chart(fig_trend, use_container_width=True)
        
        # Baseline Comparison Bar
        st.subheader("Latest Run vs Naive Baseline")
        fig_bar = go.Figure(data=[
            go.Bar(name='LightGBM Model', x=['MAPE (%)'], y=[latest_run['mape']], marker_color='#10B981'),
            go.Bar(name='Naive Baseline', x=['MAPE (%)'], y=[latest_run['baseline_mape']], marker_color='#EF4444')
        ])
        fig_bar.update_layout(
            barmode='group',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#E2E8F0',
            yaxis=dict(gridcolor='#334155')
        )
        st.plotly_chart(fig_bar, use_container_width=True)
        
        # Recent training runs table
        st.subheader("Model Runs History")
        st.dataframe(
            runs_df[['timestamp', 'mape', 'rmse', 'train_rows', 'promoted']].head(10),
            use_container_width=True
        )

# ----------------- Tab 3: Pipeline Health -----------------
with tab3:
    st.header("Airflow Pipeline Status")
    
    # Read pipeline status
    health_df = run_query("""
        SELECT DISTINCT ON (dag_name) dag_name, run_time, status, rows_processed
        FROM pipeline_runs 
        ORDER BY dag_name, run_time DESC
    """)
    
    if health_df.empty:
        st.warning("No pipeline run logs found in database.")
    else:
        # Check if any DAG failed
        failed_dags = health_df[health_df['status'] == 'FAILED']
        
        if not failed_dags.empty:
            for _, row in failed_dags.iterrows():
                st.error(f"⚠️ DAG **{row['dag_name']}** failed at **{row['run_time'].strftime('%Y-%m-%d %H:%M:%S')}**")
        else:
            st.success("All systems operational ✅ All active DAGs executed successfully.")
            
        # Display DAG Statuses
        cols = st.columns(4)
        dag_order = ['ingest_and_validate', 'feature_engineering', 'model_training', 'generate_predictions']
        
        for idx, dag_name in enumerate(dag_order):
            dag_status = health_df[health_df['dag_name'] == dag_name]
            
            with cols[idx]:
                if dag_status.empty:
                    st.markdown(f"""
                    <div class="metric-card" style="border-top: 4px solid #64748B;">
                        <div class="metric-label">{dag_name}</div>
                        <div style="font-size: 1.5rem; margin-top: 10px;">⚪ NO RUNS</div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    run = dag_status.iloc[0]
                    status = run['status']
                    color = "#10B981" if status == "SUCCESS" else "#EF4444"
                    icon = "✅ SUCCESS" if status == "SUCCESS" else "❌ FAILED"
                    
                    st.markdown(f"""
                    <div class="metric-card" style="border-top: 4px solid {color};">
                        <div class="metric-label">{dag_name}</div>
                        <div style="font-size: 1.4rem; font-weight: bold; color: {color}; margin-top: 10px;">{icon}</div>
                        <div style="font-size: 0.8rem; color: #64748B; margin-top: 5px;">
                            Processed: {run['rows_processed']:,} rows<br>
                            Run Time: {run['run_time'].strftime('%I:%M %p')}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
        # Detailed runs history log
        st.subheader("Pipeline Execution Log")
        all_logs = run_query("SELECT run_time, dag_name, status, rows_processed FROM pipeline_runs ORDER BY run_time DESC LIMIT 20")
        st.dataframe(all_logs, use_container_width=True)
