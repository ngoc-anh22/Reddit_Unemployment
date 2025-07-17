from datetime import datetime
import json
import time
import praw
import os
import boto3

# Nơi định nghĩa các biến
# Lấy thông tin xác thực từ biến môi trường
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USERNAME = os.getenv("REDDIT_USERNAME")
PASSWORD = os.getenv("REDDIT_PASSWORD")

print(CLIENT_ID)
print(CLIENT_SECRET)
print(USERNAME)
print(PASSWORD)

# Khởi tạo đối tượng reddit (kết nối với API)
# User-Agent giúp Reddit nhận diện ứng dụng của bạn và tránh bị chặn.
USER_AGENT = "Unemployment_Data_Pipeline_by_Dazzling-Shame6194_v1.0"

# Danh sách các subreddit mục tiêu đã tinh chỉnh
SUBREDDITS = ["cscareerquestions", "techjobs", "ITCareerQuestions", "unemployment", "jobs"]

# Bộ từ khóa để tìm kiếm
# Chúng ta sẽ tìm kiếm các bài đăng có chứa ít nhất một từ khóa ngành nghề/cấp bậc
# Và ít nhất một từ khóa ngữ cảnh thất nghiệp.
TECH_KEYWORDS = [
    "developer", "engineer", "software engineer", "data scientist", "machine learning engineer",
    "devops", "cloud engineer", "analyst", "architect", "manager", "admin",
    "security specialist", "qa", "tester", "ui/ux", "programmer",
    "entry level", "junior", "mid-level", "senior", "lead", "staff", "principal", "director", "executive", "intern", "fresher"
]
UNEMPLOYMENT_KEYWORDS = [
    "unemployment", "laid off", "job search", "fired", "job market", "recession",
    "hiring freeze", "downsizing", "pink slip", "redundancy"
]

collected_posts = [] # Danh sách để lưu trữ các bài đăng đã thu thập
S3_BUCKET_NAME = "reddit-unemployment-raw-data-ngocanh"
S3_PREFIX = "raw/reddit_tech_unemployment/"

# Hàm extraction
def extraction():
    # Check biến môi trường
    if not all([CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD]):
        print("Lỗi: Vui lòng thiết lập đầy đủ 4 biến môi trường")
        exit()

    # Check Reddit connection
    try:
        reddit = praw.Reddit(
            client_id = CLIENT_ID,
            client_secret = CLIENT_SECRET,
            username = USERNAME,
            password = PASSWORD,
            user_agent = USER_AGENT
        )
        print(f"Đã kết nối thành công với Reddit với tư cách: u/{reddit.user.me()}")
    
    except Exception as e:
        print(f"Đã xảy ra lỗi khi kết nối với Reddit: {e}")
        print("Vui lòng kiểm tra lại Client ID, Client Secret, tên người dùng và mật khẩu của bạn.")
        exit()

    print("\nKết nối thành công! Bắt đầu thu thập dữ liệu bài đăng.")
    print("\nBắt đầu thu thập dữ liệu bài đăng:")

    for subreddit_name in SUBREDDITS:
        print(f"\n--- Đang xử lý subreddit: r/{subreddit_name} ---")
        subreddit = reddit.subreddit(subreddit_name)

        # Query truy vấn
        query_string = " OR ".join(UNEMPLOYMENT_KEYWORDS)

        try:
            for submission in subreddit.search(query=query_string, limit = 100, sort="new", time_filter="all"):
                post_content = (submission.title + " " + submission.selftext).lower()
                if any(tech_kw.lower() in post_content for tech_kw in TECH_KEYWORDS):
                    post_data = {
                        "id": submission.id,
                        "title": submission.title,
                        "selftext": submission.selftext,
                        "author": submission.author.name if submission.author else "[deleted]",
                        "created_utc": submission.created_utc,
                        "score": submission.score,
                        "subreddit": submission.subreddit.display_name,
                        "url": submission.url,
                        "num_comments": submission.num_comments
                    }
                    collected_posts.append(post_data)
                    #print(f"  - Đã thu thập: {submission.title[:70]}... (từ r/{submission.subreddit.display_name})")
                else:
                    pass
            time.sleep(2)

        except praw.exceptions.RedditAPIException as e:
            print(f"Lỗi API khi tìm kiếm trong r/{subreddit_name}: {e}")
            print("Đang chờ 5 giây trước khi tiếp tục...")
            time.sleep(5)
        except Exception as e:
            print(f"Lỗi không xác định khi tìm kiếm trong r/{subreddit_name}: {e}")

    # Kết thúc truy vấn - In kết quả
    print(f"\nHoàn tất thu thập. Đã thu thập tổng cộng {len(collected_posts)} bài đăng liên quan.")
    if collected_posts:
        print("\nChi tiết 3 bài đăng đầu tiên đã thu thập:")
        for i, post in enumerate(collected_posts[:3]):
            print(f"\n--- Bài đăng số {i+1} ---")
            for key, value in post.items():
                if key == "selftext" and len(str(value)) > 200:
                    print(f"{key}: {str(value)[:200]}...")
                else:
                    print(f"{key}: {value}")
    else:
        print("Không tìm thấy bài đăng nào phù hợp với tiêu chí của bạn.")

    # Lưu trữ data vào S3
    print(f"\nBắt đầu lưu trữ dữ liệu vào S3 bucket: {S3_BUCKET_NAME}")

    try: 
        s3 = boto3.client('s3')

        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = f"data_{current_timestamp}.jsonl"

        current_date = datetime.now()
        s3_path = f"{S3_PREFIX}year={current_date.year}/month={current_date.month:02}/day={current_date.day:02}/{file_name}"

        jsonl_data = ""
        for post in collected_posts:
            jsonl_data += json.dumps(post) + "\n"

        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_path, Body=jsonl_data.encode('utf-8'))

        print(f"Đã lưu trữ thành công {len(collected_posts)} bài đăng vào s3://{S3_BUCKET_NAME}/{s3_path}")

    except Exception as e:
        print(f"Đã xảy ra lỗi khi lưu trữ dữ liệu lên S3: {e}")
        print("Vui lòng kiểm tra lại cấu hình AWS CLI/biến môi trường, tên S3 bucket và quyền truy cập của bạn.")

