import streamlit as st
import threading
import os
import sys
import time
import hdfs
import boto3
from pyspark.sql import SparkSession
from streamlit.runtime.scriptrunner import add_script_run_ctx
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# 1. Environment Config (From your PyQt6 logic)
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

services = {
    "spark": {"name": "PySpark", "type": ""},
    "hadoop": {"name": "Hadoop", "type": ""},
    "cloud_local": {"name": "LocalStack (S3)", "type": "Simulator"},
    "cloud_aws": {"name": "AWS Production", "type": "Cloud"},
    "cloud_minio": {"name": "MinIO Server", "type": "Local"}
}

for key, info in services.items():
    if f"{key}_status" not in st.session_state:
        st.session_state[f"{key}_status"] = f"Run {info['name']}"
    if f"{key}_label" not in st.session_state:
        st.session_state[f"{key}_label"] = f"Idle ({info['type']}) 💤"
    if f"{key}_ready" not in st.session_state:
        st.session_state[f"{key}_ready"] = False
    if f"{key}_running" not in st.session_state:
        st.session_state[f"{key}_running"] = False
    

if "system_logs" not in st.session_state:
    st.session_state["system_logs"] = []

def log_event(message):
    timestamp = time.strftime("%H:%M:%S")
    st.session_state["system_logs"].append(f"[{timestamp}] {message}")


# --- THE WORKER FUNCTION ---
def spark_worker():
    try:
        # Note: We don't use st.write here because threads can't render widgets
        spark = SparkSession.builder.appName("StreamlitSpark").getOrCreate()
        
        # Update State
        st.session_state.spark_session = spark
        st.session_state.spark_ready = True
        st.session_state.spark_status = "Stop PySpark"
        st.session_state.spark_label = "Active ✅"
    except Exception as e:
        st.session_state.spark_label = f"Error: {str(e)}"
        st.session_state.spark_status = "Run PySpark"
    
    # Trigger a rerun so the UI sees the 'Active' state
    st.rerun()


def verify_aws_identity():
    try:
        # STS is the service for identity verification
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        
        user_id = identity.get('UserId')
        account = identity.get('Account')
        arn = identity.get('Arn')
        
        log_event(f"AWS VERIFIED: Account {account}")
        log_event(f"User ARN: {arn}")
        
        return True
    except Exception as e:
        log_event(f"AWS IDENTITY VERIFICATION FAILED: {str(e)}")
        return False

def generic_session(name):
    if name == "PySpark":
        return SparkSession.builder.appName("StreamlitSpark").getOrCreate()
    elif name == "Hadoop":
        return hdfs.InsecureClient('http://localhost:9871', user='justi')
    if name == "cloud_aws":
        try:
            # Initialize the S3 client
            # Note: boto3 looks for credentials in ~/.aws/credentials automatically
            s3 = boto3.client('s3')
            sts = boto3.client('sts')
            
            # Verify Identity
            identity = sts.get_caller_identity()
            log_event(f"Logged into AWS as: {identity['Arn']}")

            # Verify Access (List Buckets)
            buckets = s3.list_buckets()
            count = len(buckets.get('Buckets', []))
            log_event(f"S3 Connection Stable. Found {count} buckets.")
            return s3
        except NoCredentialsError:
            log_event("ERROR: AWS Credentials not found.")
            raise
        except Exception as e:
            log_event(f"ERROR: Cloud connection failed: {e}")
            raise
    else:
        return "nothing"

def generic_worker(tool):
    log_event(f"Initializing {tool} thread...")
    try:
        # Generic Session
        log_event(f"Connecting to {tool} backend services...")
        session = generic_session(tool)

        st.session_state[f"{tool}_session"] = session
        st.session_state[f"{tool}_ready"] = True
        st.session_state[f"{tool}_status"] = f"Stop {tool}"
        st.session_state[f"{tool}_label"] = "Active ✅"
        log_event(f"SUCCESS: {tool} is now online.")
    except Exception as e:
        log_event(f"CRITICAL ERROR in {tool}: {str(e)}")
        st.session_state[f"{tool}_label"] = f"Error: {str(e)}"
        st.session_state[f"{tool}_status"] = f"Run {tool}"


def close_session(sid):
    """Safely shuts down the connection for a specific tool."""
    session_key = f"{sid}_session"
    
    if session_key in st.session_state and st.session_state[session_key] is not None:
        try:
            session = st.session_state[session_key]
            
            # Specific closing logic based on tool type
            if "spark" in sid.lower():
                session.stop()
            elif "cloud" in sid.lower():
                # For AWS/Boto3, you usually just nullify the client
                pass 
            
            st.session_state[session_key] = None
            log_event(f"SUCCESS: {sid.upper()} session closed.")
        except Exception as e:
            log_event(f"ERROR closing {sid}: {str(e)}")

@st.fragment(run_every=1)
def control_panel(sid):
    """The UI logic for each tab."""
    btn_label = st.session_state.get(f"{sid}_status", f"Run {sid}")
    
    if st.button(btn_label, key=f"btn_{sid}"):
        if "Run" in btn_label:
            # START LOGIC
            st.session_state[f"{sid}_running"] = True # The Kill Switch flag
            st.session_state[f"{sid}_label"] = "Initializing... ⚙️"
            st.session_state[f"{sid}_status"] = f"Stop {sid}"
            
            t = threading.Thread(target=generic_worker, args=(sid,))
            add_script_run_ctx(t)
            t.start()
        else:
            # STOP LOGIC
            st.session_state[f"{sid}_running"] = False # Signal thread to stop
            close_session(sid) # Kill the connection
            st.session_state[f"{sid}_label"] = "Idle 💤"
            st.session_state[f"{sid}_status"] = f"Run {sid}"
            st.session_state[f"{sid}_ready"] = False
        
        st.rerun()


@st.fragment(run_every=2)
def log_monitor():
    # Use .get() with a default empty list [] so it NEVER fails to iterate
    logs = st.session_state.get("system_logs", [])
    for entry in reversed(logs[-15:]):
        # Check if the log is a dictionary
        if isinstance(entry, dict):
            st.write(f"{entry.get('time', '00:00')} - {entry.get('msg', '')}")
        else:
            # If it's just a string, print it as is
            st.write(str(entry))

@st.fragment(run_every=1)
def global_status_monitor():
    st.header("Hub Monitor")
    st.divider()
    
    # Loop through the registry so it always matches the tabs
    for sid, info in services.items():
        current_name = info['name']
        st.subheader(current_name)
        
        # Use .get() with the dynamic key name
        current_label = st.session_state.get(f"{current_name}_label", "Idle 💤")
        st.write(f"Status: {current_label}")
        
        # Check the 'ready' or 'success' flag
        if st.session_state.get(f"{sid}_ready"):
            st.success(f"{current_name} Ready")
            
        st.divider()



# --- MAIN UI ---
st.title("Data Migration Hub")

service_list = ["PySpark", "Hadoop", "System Logs", "Cloud Local", "Cloud AWS", "Cloud MinIO"]

# Create the Tabs dynamically
tabs = st.tabs([s for s in service_list])


with st.sidebar:
    global_status_monitor()
    st.divider()
    log_monitor()

#with tab_spark:
#    st.header("PySpark Management")
#    # Spark Control Panel
#    spark_control_panel()


# Loop through and fill the tabs
for i, service_id in enumerate(service_list):
    with tabs[i]:
        st.header(f"{service_id} Control")
        # Just call the function we defined above
        control_panel(service_id)
