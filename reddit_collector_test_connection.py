import praw # Thư viện để tương tác với Reddit API
import os   # Thư viện để đọc biến môi trường

# Lấy thông tin xác thực từ biến môi trường
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USERNAME = os.getenv("REDDIT_USERNAME")
PASSWORD = os.getenv("REDDIT_PASSWORD")

print(CLIENT_ID)
print(CLIENT_SECRET)
print(USERNAME)
print(PASSWORD)

if not all([CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD]):
    print("Lỗi: Vui lòng thiết lập đầy đủ 4 biến môi trường")
    exit()

# Khởi tạo đối tượng reddit (kết nối với API)
# Đặt một tên user_agent độc đáo cho ứng dụng của bạn
# User-Agent giúp Reddit nhận diện ứng dụng của bạn và tránh bị chặn.
USER_AGENT = "Unemployment_Data_Pipeline_by_Dazzling-Shame6194_v1.0"

reddit = praw.Reddit(
    client_id = CLIENT_ID,
    client_secret = CLIENT_SECRET,
    username = USERNAME,
    password = PASSWORD,
    user_agent = USER_AGENT
)

# Kiểm tra kết nối thành công
try:
    print(f"Đã kết nối thành công với Reddit với tư cách: u/{reddit.user.me()}")

except Exception as e:
    print(f"Đã xảy ra lỗi khi kết nối với Reddit: {e}")
    print("Vui lòng kiểm tra lại Client ID, Client Secret, tên người dùng và mật khẩu của bạn.")
    exit()

print("\nKết nối thành công! Bạn có thể tiếp tục với các bước thu thập dữ liệu.")

# Danh sách các subreddit mục tiêu đã tinh chỉnh
SUBREDDITS = ["cscareerquestions", "techjobs", "ITCareerQuestions", "unemployment", "jobs"]

# Bộ từ khóa để tìm kiếm (có thể tùy chỉnh)
# Chúng ta sẽ tìm kiếm các bài đăng có chứa ít nhất một từ khóa ngành nghề/cấp bậc
# VÀ ít nhất một từ khóa ngữ cảnh thất nghiệp.
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

print("\nBắt đầu thu thập dữ liệu bài đăng:")

collected_posts = [] # Danh sách để lưu trữ các bài đăng đã thu thập

for subreddit_name in SUBREDDITS:
    print(f"\n--- Đang xử lý subreddit: r/{subreddit_name} ---")
    subreddit = reddit.subreddit(subreddit_name)

    # Xây dựng truy vấn tìm kiếm
    query_string = " OR ".join(UNEMPLOYMENT_KEYWORDS)

    try:
        for submission in subreddit.search(query=query_string, limit=100, sort='new', time_filter='month'):
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
    except Exception as e:
        print(f"Lỗi khi tìm kiếm trong r/{subreddit_name}: {e}")


print(f"\nĐã thu thập tổng cộng {len(collected_posts)} bài đăng.")
# Bạn có thể in ra một vài bài đăng đầu tiên để kiểm tra
if collected_posts:
    print("\nMột vài bài đăng đã thu thập (đầu tiên):")
    for i, post in enumerate(collected_posts[:3]):
        print(f"Bài {i+1}: Tiêu đề: {post['title']} | Subreddit: {post['subreddit']}")