"""
Streamlit UI for Complex Event Processing Agent
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import time
from cep_agent import (
    CEPAgent, Event, EventType, EventPriority, 
    generate_sample_event, create_example_patterns
)

st.set_page_config(
    page_title="CEP Agent Dashboard",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem;
    }
    .event-card {
        border-left: 4px solid #1f77b4;
        padding: 1rem;
        margin: 0.5rem 0;
        background-color: #f8f9fa;
        border-radius: 0.3rem;
    }
    .critical { border-left-color: #dc3545; }
    .high { border-left-color: #fd7e14; }
    .medium { border-left-color: #ffc107; }
    .low { border-left-color: #28a745; }
    </style>
""", unsafe_allow_html=True)

if 'agent' not in st.session_state:
    st.session_state.agent = CEPAgent()
    for pattern in create_example_patterns():
        st.session_state.agent.register_pattern(pattern)

if 'event_counter' not in st.session_state:
    st.session_state.event_counter = 0

if 'auto_generate' not in st.session_state:
    st.session_state.auto_generate = False

def add_manual_event(event_type, priority, source, temperature, humidity):
    event = Event(
        event_id=f"MAN_{st.session_state.event_counter:04d}",
        event_type=EventType(event_type),
        timestamp=datetime.now(),
        source=source,
        data={"temperature": temperature, "humidity": humidity, "status": "manual"},
        priority=EventPriority[priority]
    )
    st.session_state.agent.add_event(event)
    st.session_state.event_counter += 1
    st.success(f"Event {event.event_id} added successfully!")

def generate_auto_event():
    event = generate_sample_event(st.session_state.event_counter)
    st.session_state.agent.add_event(event)
    st.session_state.event_counter += 1

def render_dashboard():
    st.header("System Overview")
    stats = st.session_state.agent.get_statistics()
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Events", stats['total_events'])
    col2.metric("Patterns Detected", stats['patterns_detected'])
    col3.metric("Buffer Size", len(st.session_state.agent.event_buffer))
    col4.metric("Active Patterns", len(st.session_state.agent.patterns))
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Events by Type")
        if stats['events_by_type']:
            df = pd.DataFrame(list(stats['events_by_type'].items()), 
                            columns=['Event Type', 'Count'])
            fig = px.pie(df, values='Count', names='Event Type')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No events yet")
    
    with col2:
        st.subheader("Events by Priority")
        if stats['events_by_priority']:
            df = pd.DataFrame(list(stats['events_by_priority'].items()), 
                            columns=['Priority', 'Count'])
            fig = px.bar(df, x='Priority', y='Count', color='Priority')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No events yet")

def render_event_stream():
    st.header("Event Stream")
    
    col1, col2, col3 = st.columns(3)
    filter_type = col1.selectbox("Filter by Type", ["All"] + [e.value for e in EventType])
    filter_priority = col2.selectbox("Filter by Priority", ["All"] + [p.name for p in EventPriority])
    limit = col3.number_input("Number of events", 10, 1000, 50)
    
    event_type_filter = None if filter_type == "All" else EventType(filter_type)
    priority_filter = None if filter_priority == "All" else EventPriority[filter_priority]
    
    events = st.session_state.agent.get_events(
        event_type=event_type_filter,
        priority=priority_filter,
        limit=limit
    )
    
    st.subheader(f"Showing {len(events)} events")
    
    for event in reversed(events):
        priority_class = event.priority.name.lower()
        st.markdown(f"""
        <div class="event-card {priority_class}">
            <strong>ID:</strong> {event.event_id} | 
            <strong>Type:</strong> {event.event_type.value} | 
            <strong>Priority:</strong> {event.priority.name} | 
            <strong>Source:</strong> {event.source}<br>
            <strong>Time:</strong> {event.timestamp.strftime('%Y-%m-%d %H:%M:%S')}<br>
            <strong>Data:</strong> {event.data}
        </div>
        """, unsafe_allow_html=True)

def render_pattern_detection():
    st.header("Pattern Detection")
    detected = st.session_state.agent.get_detected_patterns()
    st.subheader(f"Detected Patterns: {len(detected)}")
    
    if detected:
        for pd_item in reversed(detected):
            with st.expander(f"🎯 {pd_item['pattern_name']} - {pd_item['timestamp']}"):
                st.write(f"**Pattern ID:** {pd_item['pattern_id']}")
                st.write(f"**Description:** {pd_item['description']}")
                st.write(f"**Matching Events:** {len(pd_item['matching_events'])}")
                st.write("**Event IDs:**")
                for event_id in pd_item['matching_events']:
                    st.text(f"  • {event_id}")
    else:
        st.info("No patterns detected yet")
    
    if detected:
        pattern_counts = {}
        for d in detected:
            name = d['pattern_name']
            pattern_counts[name] = pattern_counts.get(name, 0) + 1
        
        df = pd.DataFrame(list(pattern_counts.items()), columns=['Pattern', 'Detections'])
        fig = px.bar(df, x='Pattern', y='Detections', color='Detections')
        st.plotly_chart(fig, use_container_width=True)

def render_analytics():
    st.header("Analytics & Insights")
    events = st.session_state.agent.get_events(limit=1000)
    
    if not events:
        st.info("No data available for analytics")
        return
    
    df = pd.DataFrame([{
        'timestamp': e.timestamp,
        'event_type': e.event_type.value,
        'priority': e.priority.name,
        'temperature': e.data.get('temperature', 0),
        'humidity': e.data.get('humidity', 0)
    } for e in events])
    
    st.subheader("Event Timeline")
    df_timeline = df.groupby([pd.Grouper(key='timestamp', freq='1min'), 
                              'event_type']).size().reset_index(name='count')
    fig = px.line(df_timeline, x='timestamp', y='count', color='event_type')
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Temperature Trends")
        fig = px.scatter(df, x='timestamp', y='temperature', color='priority')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Humidity Trends")
        fig = px.scatter(df, x='timestamp', y='humidity', color='priority')
        st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Statistical Summary")
    st.dataframe(df[['temperature', 'humidity']].describe())

def main():
    st.markdown('<div class="main-header">🔄 Complex Event Processing Agent</div>', 
                unsafe_allow_html=True)
    
    with st.sidebar:
        st.header("⚙️ Control Panel")
        
        st.subheader("Event Generation")
        auto_gen = st.checkbox("Auto-generate events", value=st.session_state.auto_generate)
        st.session_state.auto_generate = auto_gen
        
        if auto_gen:
            interval = st.slider("Generation interval (seconds)", 1, 10, 3)
        
        st.subheader("Create Manual Event")
        with st.form("event_form"):
            event_type = st.selectbox("Event Type", [e.value for e in EventType])
            priority = st.selectbox("Priority", [p.name for p in EventPriority])
            source = st.text_input("Source", "manual_input")
            temperature = st.slider("Temperature", 0, 50, 25)
            humidity = st.slider("Humidity", 0, 100, 50)
            
            if st.form_submit_button("Add Event"):
                add_manual_event(event_type, priority, source, temperature, humidity)
        
        st.subheader("Buffer Management")
        if st.button("Clear Event Buffer", type="secondary"):
            st.session_state.agent.clear_buffer()
            st.success("Buffer cleared!")
        
        st.subheader("Registered Patterns")
        patterns = st.session_state.agent.patterns
        st.info(f"Total patterns: {len(patterns)}")
        for pattern in patterns.values():
            st.text(f"• {pattern.name}")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "📋 Event Stream", 
                                       "🎯 Pattern Detection", "📈 Analytics"])
    
    with tab1:
        render_dashboard()
    with tab2:
        render_event_stream()
    with tab3:
        render_pattern_detection()
    with tab4:
        render_analytics()
    
    if st.session_state.auto_generate:
        time.sleep(interval)
        generate_auto_event()
        st.rerun()

if __name__ == "__main__":
    main()
