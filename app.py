import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import io
from db import save_data_to_table, create_table_for_file, get_db_connection, get_data_from_table

st.set_page_config(page_title="COVID-19数据分析", layout="wide", 
                   initial_sidebar_state="expanded")

def load_data(file):
    df = pd.read_csv(file)
    # 将dateId转换为datetime格式
    df['dateId'] = pd.to_datetime(df['dateId'].astype(str), format='%Y%m%d')
    return df.sort_values('dateId')

def plot_trends(df):
    # 创建主图表
    fig = go.Figure()
    
    metrics = {
        'confirmedCount': ['累计确诊', '#FF4B4B'],
        'currentConfirmedCount': ['现存确诊', '#36A2EB'],
        'deadCount': ['累计死亡', '#4B4B4B']
    }
    
    for col, (name, color) in metrics.items():
        fig.add_trace(go.Scatter(
            x=df['dateId'],
            y=df[col],
            name=name,
            line=dict(color=color, width=2),
            fill='tonexty' if col == 'currentConfirmedCount' else None
        ))
    
    fig.update_layout(
        title={
            'text': '疫情趋势分析',
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': dict(size=24)
        },
        xaxis_title='日期',
        yaxis_title='人数',
        template='plotly_white',
        hovermode='x unified',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor='rgba(255, 255, 255, 0.8)'
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    
    return fig

def plot_daily_increase(df):
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['dateId'],
        y=df['confirmedIncr'],
        name='新增确诊',
        marker_color='#FF4B4B'
    ))
    
    fig.update_layout(
        title={
            'text': '每日新增病例',
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': dict(size=24)
        },
        xaxis_title='日期',
        yaxis_title='新增人数',
        template='plotly_white',
        showlegend=True,
        margin=dict(l=20, r=20, t=60, b=20)
    )
    
    return fig

def plot_risk_areas(df, threshold):
    risk_areas = df[df['currentConfirmedCount'] > threshold].sort_values('currentConfirmedCount', ascending=True)
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=risk_areas['currentConfirmedCount'],
        y=risk_areas['dateId'].dt.strftime('%Y-%m-%d'),
        orientation='h',
        marker_color='#FF4B4B'
    ))
    
    fig.update_layout(
        title={
            'text': f'高风险日期(现存确诊>{threshold}人)',
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': dict(size=24)
        },
        xaxis_title='现存确诊人数',
        yaxis_title='日期',
        template='plotly_white',
        height=max(400, len(risk_areas) * 30),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    
    return fig

def get_table_list():
    """获取数据库中的所有表"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            # 使用 DictCursor 时需要获取字典中的值
            results = cursor.fetchall()
            # 提取表���（第一个值）
            return [list(table.values())[0] for table in results]

def load_data_from_db(table_name):
    df = get_data_from_table(table_name)
    if 'dateId' in df.columns:
        # 确保日期格式正确
        if not pd.api.types.is_datetime64_any_dtype(df['dateId']):
            df['dateId'] = pd.to_datetime(df['dateId'].astype(str).str.replace('-', ''), 
                                        format='%Y%m%d', 
                                        errors='coerce')
    return df.sort_values('dateId')

def main():
    st.title('COVID-19疫情数据分析平台')
    
    # 添加数据来源选择
    data_source = st.radio(
        "选择数据来源",
        ["上传新文件", "使用已有数据"],
        horizontal=True
    )
    
    df = None
    
    if data_source == "上传新文件":
        uploaded_file = st.file_uploader("上传CSV文件", type='csv')
        if uploaded_file:
            df = load_data(uploaded_file)
            # 保存到数据库
            table_name = create_table_for_file(uploaded_file.name)
            save_data_to_table(df, table_name)
            
    else:
        # 从数据库中选择表
        tables = get_table_list()
        if tables:
            selected_table = st.selectbox(
                "选择数据文件",
                tables,
                format_func=lambda x: x.replace('_', ' ').title()
            )
            if selected_table:
                df = load_data_from_db(selected_table)
        else:
            st.warning("数据库中没有可用的数据表")
            return
    
    if df is None:
        st.info('请选择或上传数据文件')
        return
        
    # 预设时间范围（最近30天）
    default_end_date = df['dateId'].max()
    default_start_date = default_end_date - timedelta(days=30)
    
    # 用户交互区
    col1, col2 = st.columns(2)
    with col1:
        dates = st.date_input(
            "选择日期范围",
            value=(default_start_date, default_end_date),
            min_value=df['dateId'].min(),
            max_value=df['dateId'].max()
        )
    with col2:
        threshold = st.slider(
            "设置风险阈值（现存确诊人数）",
            min_value=0,
            max_value=int(df['currentConfirmedCount'].max()),
            value=1000,
            format="%d人"
        )

    if df is not None and 'dateId' in df.columns:
        # 确保dateId是datetime类型
        if not pd.api.types.is_datetime64_any_dtype(df['dateId']):
            df['dateId'] = pd.to_datetime(df['dateId'].astype(str).str.replace('-', ''), 
                                        format='%Y%m%d', 
                                        errors='coerce')
        
        # 日期过滤
        if isinstance(dates, tuple) and len(dates) == 2:
            mask = (df['dateId'].dt.date >= dates[0]) & (df['dateId'].dt.date <= dates[1])
            filtered_df = df[mask]
        else:
            filtered_df = df

    # 展示图表
    st.plotly_chart(plot_trends(filtered_df), use_container_width=True)
    st.plotly_chart(plot_daily_increase(filtered_df), use_container_width=True)
    
    risk_areas = df[df['currentConfirmedCount'] > threshold]
    if not risk_areas.empty:
        st.plotly_chart(plot_risk_areas(filtered_df, threshold), use_container_width=True)

    # 数据导出
    if st.button("导出分析结果"):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            filtered_df.to_excel(writer, sheet_name='趋势分析', index=False)
            risk_areas.to_excel(writer, sheet_name='高风险地区', index=False)
        
        st.download_button(
            "📥 下载Excel报告",
            output.getvalue(),
            "covid19_analysis.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

if __name__ == "__main__":
    main()