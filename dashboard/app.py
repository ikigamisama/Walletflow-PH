import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import time
from kafka import KafkaConsumer
import json


# Page Configuration
st.set_page_config(
    page_title="WalletFlow-PH Dashboard",
    page_icon="🇵🇭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .fraud-alert {
        background-color: #ff4444;
        color: white;
        padding: 1rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
    .success-metric {
        color: #00c851;
        font-weight: bold;
        font-size: 1.2rem;
    }
    .warning-metric {
        color: #ffbb33;
        font-weight: bold;
        font-size: 1.2rem;
    }
    .danger-metric {
        color: #ff4444;
        font-weight: bold;
        font-size: 1.2rem;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_db_connection():
    """Create PostgreSQL connection to pipeline database"""
    try:
        conn = psycopg2.connect(
            host="postgres-pipeline",  # ← Changed from "postgres"
            database="walletflow",
            user="pipeline_user",      # ← Changed from "admin"
            password="pipeline_password",  # ← Changed from "admin"
            port=5432
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        return None


# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================

def fetch_realtime_stats(conn):
    """Fetch real-time statistics from last hour"""
    query = """
    SELECT 
        COUNT(*) as total_transactions,
        SUM(amount_php) as total_volume,
        AVG(amount_php) as avg_transaction,
        SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END) as fraud_count,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
        COUNT(DISTINCT user_id) as active_users,
        COUNT(DISTINCT merchant_id) as active_merchants
    FROM silver.fact_transactions
    WHERE event_timestamp >= NOW() - INTERVAL '1 hour';
    """
    df = pd.read_sql(query, conn)
    return df.iloc[0] if not df.empty else None


def fetch_hourly_velocity(conn, hours=24):
    """Fetch hourly transaction velocity"""
    query = f"""
    SELECT 
        DATE_TRUNC('hour', event_timestamp) as hour,
        COUNT(*) as tx_count,
        SUM(amount_php) as volume_php,
        AVG(amount_php) as avg_amount,
        SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END) as fraud_count
    FROM silver.fact_transactions
    WHERE event_timestamp >= NOW() - INTERVAL '{hours} hours'
    GROUP BY DATE_TRUNC('hour', event_timestamp)
    ORDER BY hour DESC;
    """
    return pd.read_sql(query, conn)


def fetch_fraud_alerts(conn, limit=20):
    """Fetch recent fraud alerts"""
    query = f"""
    SELECT 
        fa.alert_id,
        fa.transaction_id,
        fa.user_id,
        fa.alert_type,
        fa.fraud_score,
        fa.amount_php,
        fa.province,
        fa.event_timestamp,
        fa.status,
        u.full_name as user_name,
        m.merchant_name
    FROM gold.mart_fraud_alerts fa
    LEFT JOIN silver.dim_users u ON fa.user_id = u.user_id
    LEFT JOIN silver.dim_merchants m ON fa.merchant_id = m.merchant_id
    WHERE fa.status = 'NEW'
    ORDER BY fa.created_at DESC
    LIMIT {limit};
    """
    return pd.read_sql(query, conn)


def fetch_category_breakdown(conn):
    """Fetch spending by category"""
    query = """
    SELECT 
        m.category,
        COUNT(*) as tx_count,
        SUM(t.amount_php) as total_volume,
        AVG(t.amount_php) as avg_amount
    FROM silver.fact_transactions t
    JOIN silver.dim_merchants m ON t.merchant_id = m.merchant_id
    WHERE t.event_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY m.category
    ORDER BY total_volume DESC;
    """
    return pd.read_sql(query, conn)


def fetch_provincial_heatmap(conn):
    """Fetch transaction volume by province"""
    query = """
    SELECT 
        province,
        COUNT(*) as tx_count,
        SUM(amount_php) as total_volume,
        SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END) as fraud_count
    FROM silver.fact_transactions
    WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY province
    ORDER BY total_volume DESC
    LIMIT 10;
    """
    return pd.read_sql(query, conn)


def fetch_top_merchants(conn, limit=10):
    """Fetch top merchants by volume"""
    query = f"""
    SELECT 
        m.merchant_name,
        m.category,
        COUNT(*) as tx_count,
        SUM(t.amount_php) as total_volume
    FROM silver.fact_transactions t
    JOIN silver.dim_merchants m ON t.merchant_id = m.merchant_id
    WHERE t.event_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY m.merchant_name, m.category
    ORDER BY total_volume DESC
    LIMIT {limit};
    """
    return pd.read_sql(query, conn)


def fetch_channel_distribution(conn):
    """Fetch transaction distribution by channel"""
    query = """
    SELECT 
        channel,
        COUNT(*) as tx_count,
        SUM(amount_php) as total_volume
    FROM silver.fact_transactions
    WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY channel
    ORDER BY tx_count DESC;
    """
    return pd.read_sql(query, conn)


def fetch_live_transactions(conn, limit=100):
    """Fetch latest transactions"""
    query = f"""
    SELECT 
        t.transaction_id,
        t.event_timestamp,
        u.full_name as user_name,
        m.merchant_name,
        t.amount_php,
        t.channel,
        t.province,
        t.fraud_flag,
        t.fraud_score
    FROM silver.fact_transactions t
    LEFT JOIN silver.dim_users u ON t.user_id = u.user_id
    LEFT JOIN silver.dim_merchants m ON t.merchant_id = m.merchant_id
    ORDER BY t.event_timestamp DESC
    LIMIT {limit};
    """
    return pd.read_sql(query, conn)


# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def plot_hourly_velocity(df):
    """Create hourly velocity chart"""
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Transaction Count', 'Transaction Volume (₱)'),
        vertical_spacing=0.15
    )

    # Transaction count
    fig.add_trace(
        go.Scatter(
            x=df['hour'],
            y=df['tx_count'],
            name='Transactions',
            line=dict(color='#1f77b4', width=3),
            fill='tozeroy'
        ),
        row=1, col=1
    )

    # Fraud overlay
    fig.add_trace(
        go.Scatter(
            x=df['hour'],
            y=df['fraud_count'],
            name='Fraud Alerts',
            line=dict(color='#ff4444', width=2, dash='dot')
        ),
        row=1, col=1
    )

    # Volume
    fig.add_trace(
        go.Scatter(
            x=df['hour'],
            y=df['volume_php'],
            name='Volume',
            line=dict(color='#2ca02c', width=3),
            fill='tozeroy'
        ),
        row=2, col=1
    )

    fig.update_layout(
        height=600,
        showlegend=True,
        hovermode='x unified'
    )

    return fig


def plot_category_pie(df):
    """Create category breakdown pie chart"""
    fig = px.pie(
        df,
        values='total_volume',
        names='category',
        title='Spending by Category (24h)',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


def plot_provincial_bar(df):
    """Create provincial heatmap bar chart"""
    fig = px.bar(
        df,
        x='province',
        y='total_volume',
        color='fraud_count',
        title='Transaction Volume by Province',
        labels={'total_volume': 'Volume (₱)', 'fraud_count': 'Fraud Count'},
        color_continuous_scale='Reds'
    )
    fig.update_layout(xaxis_tickangle=-45)
    return fig


def plot_channel_distribution(df):
    """Create channel distribution chart"""
    fig = px.bar(
        df,
        x='channel',
        y='tx_count',
        title='Transaction Distribution by Channel',
        color='channel',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    return fig


def plot_top_merchants(df):
    """Create top merchants chart"""
    fig = px.bar(
        df,
        y='merchant_name',
        x='total_volume',
        orientation='h',
        title='Top 10 Merchants by Volume',
        color='category',
        labels={'total_volume': 'Volume (₱)'}
    )
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    return fig


# ============================================================================
# MAIN DASHBOARD
# ============================================================================

def main():
    # Header
    st.markdown('<h1 class="main-header">🇵🇭 WalletFlow-PH Real-Time Dashboard</h1>',
                unsafe_allow_html=True)

    # Sidebar
    st.sidebar.title("⚙️ Dashboard Controls")
    refresh_rate = st.sidebar.slider("Auto-refresh (seconds)", 5, 60, 10)
    show_fraud_only = st.sidebar.checkbox(
        "Show Fraud Alerts Only", value=False)

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Statistics Period")
    time_range = st.sidebar.selectbox(
        "Select Time Range",
        ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"]
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔄 Dashboard Status")
    status_placeholder = st.sidebar.empty()

    # Database connection
    conn = get_db_connection()
    if not conn:
        st.error("❌ Cannot connect to database. Please check your configuration.")
        return

    # Auto-refresh logic
    placeholder = st.empty()

    with placeholder.container():
        try:
            # Fetch real-time stats
            stats = fetch_realtime_stats(conn)

            if stats is not None:
                # Key Metrics Row
                st.markdown("### 📈 Real-Time Metrics (Last Hour)")
                col1, col2, col3, col4, col5 = st.columns(5)

                with col1:
                    st.metric(
                        "Total Transactions",
                        f"{int(stats['total_transactions']):,}",
                        delta=None
                    )

                with col2:
                    st.metric(
                        "Total Volume",
                        f"₱{stats['total_volume']:,.2f}",
                        delta=None
                    )

                with col3:
                    success_rate = (stats['success_count'] / stats['total_transactions']
                                    * 100) if stats['total_transactions'] > 0 else 0
                    st.metric(
                        "Success Rate",
                        f"{success_rate:.1f}%",
                        delta=None
                    )

                with col4:
                    fraud_rate = (stats['fraud_count'] / stats['total_transactions']
                                  * 100) if stats['total_transactions'] > 0 else 0
                    st.metric(
                        "Fraud Rate",
                        f"{fraud_rate:.2f}%",
                        delta="⚠️" if fraud_rate > 1 else None
                    )

                with col5:
                    st.metric(
                        "Active Users",
                        f"{int(stats['active_users']):,}",
                        delta=None
                    )

                st.markdown("---")

                # Row 1: Hourly Velocity + Fraud Alerts
                col1, col2 = st.columns([2, 1])

                with col1:
                    st.markdown("### 📊 Hourly Transaction Velocity")
                    velocity_df = fetch_hourly_velocity(conn, hours=24)
                    if not velocity_df.empty:
                        fig = plot_hourly_velocity(velocity_df)
                        st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.markdown("### 🚨 Recent Fraud Alerts")
                    fraud_df = fetch_fraud_alerts(conn, limit=10)
                    if not fraud_df.empty:
                        for _, alert in fraud_df.iterrows():
                            st.markdown(f"""
                            <div class="fraud-alert">
                                <strong>{alert['alert_type']}</strong><br>
                                Score: {alert['fraud_score']:.0f} | Amount: ₱{alert['amount_php']:,.2f}<br>
                                User: {alert['user_name']} | {alert['province']}<br>
                                <small>{alert['event_timestamp']}</small>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        st.success("✅ No fraud alerts in the last hour!")

                st.markdown("---")

                # Row 2: Category + Provincial Analysis
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("### 🏪 Spending by Category")
                    category_df = fetch_category_breakdown(conn)
                    if not category_df.empty:
                        fig = plot_category_pie(category_df)
                        st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.markdown("### 🗺️ Provincial Transaction Heatmap")
                    province_df = fetch_provincial_heatmap(conn)
                    if not province_df.empty:
                        fig = plot_provincial_bar(province_df)
                        st.plotly_chart(fig, use_container_width=True)

                st.markdown("---")

                # Row 3: Channel Distribution + Top Merchants
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("### 📱 Channel Distribution")
                    channel_df = fetch_channel_distribution(conn)
                    if not channel_df.empty:
                        fig = plot_channel_distribution(channel_df)
                        st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.markdown("### 🏆 Top Merchants")
                    merchants_df = fetch_top_merchants(conn, limit=10)
                    if not merchants_df.empty:
                        fig = plot_top_merchants(merchants_df)
                        st.plotly_chart(fig, use_container_width=True)

                st.markdown("---")

                # Row 4: Live Transaction Feed
                st.markdown("### 🔴 Live Transaction Feed")
                live_df = fetch_live_transactions(conn, limit=50)
                if not live_df.empty:
                    # Color code fraud transactions
                    def highlight_fraud(row):
                        if row['fraud_flag']:
                            return ['background-color: #ffcccc'] * len(row)
                        return [''] * len(row)

                    if show_fraud_only:
                        live_df = live_df[live_df['fraud_flag'] == True]

                    styled_df = live_df.style.apply(
                        highlight_fraud, axis=1)
                    st.dataframe(
                        styled_df,
                        use_container_width=True,
                        height=400
                    )

                # Update status
                status_placeholder.success(
                    f"✅ Last updated: {datetime.now().strftime('%H:%M:%S')}")

            else:
                st.warning(
                    "⚠️ No data available. Start the transaction generator!")

        except Exception as e:
            st.error(f"❌ Error fetching data: {e}")
            status_placeholder.error("❌ Error")


if __name__ == "__main__":
    main()
