from airflow import DAG
import pedulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

local_tz = pendulum.timezone("Asia/Seoul")


from datetime import datetime, timedelta

default_args = {
    # DAG 소유자 (담당자 이름)
    "owner": "khj",
    
    # 이메일 알림 받을 주소
    # "email": ["your_email@example.com"],
    
    # 실패 시 이메일 알림 여부
    # "email_on_failure": False,
    
    # 재시도 시 이메일 알림 여부
    # "email_on_retry": False,
    
    # 실패 시 재시도 횟수
    # "retries": 1,
    
    # 재시도 간격 (5분)
    # "retry_delay": timedelta(minutes=5),
    
    # DAG 시작 날짜 (이 날짜부터 스케줄링 시작)
    "start_date": datetime(2026, 1, 1),
    
    # 과거 실행 건 자동 실행 여부
    "catchup": False,
    
    # Task 최대 실행 시간 (타임아웃)
    "execution_timeout": timedelta(hours=1),
    
    # 이전 DAG Run이 완료될 때까지 대기
    "depends_on_past": False,
    
    # Task 실행 대기 시간 제한
    "dagrun_timeout": timedelta(hours=2),
}

with DAG(
    dag_id = "produce_json",
    default_args = default_args,
    description = 'DAG to produce JSON fine with raw data',
    schedule = '0 14 * * *',
) as dag:
    
    #define tasks
    playlist_id = get_playlist_id()
    video_ids =  get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    #define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task
