from urllib import response

import streamlit as st
import requests
import pandas as pd

# restore token from query params
params = st.query_params
if "logout" in params:
    st.session_state.token = None
    st.session_state.username = None
    st.session_state.show_user_menu = False
    st.query_params.clear()
    st.rerun()

if "symbol" in params:
    st.session_state.selected_company = params["symbol"]
    st.session_state.page = "Company Explorer"
    del st.query_params["symbol"]
    st.rerun()
    

if "folder" in params:
    st.session_state.selected_folder = params["folder"]
    st.session_state.page = "Portfolio"
    del st.query_params["folder"]
    st.rerun()

if "show_account_popup" not in st.session_state:
    st.session_state.show_account_popup = False
API_URL = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="StockX AI",
    page_icon="📈",
    layout="wide"
)

st.markdown("""
<style>

/* ONLY history buttons */
div[data-testid="stSidebar"] button[data-key^="history_"] {
    background-color: #f5f7fb;
    border: 1px solid #e0e0e0;
    border-radius: 0px;
    text-align: left;
    padding: 10px;
    font-size: 13px;
    margin-bottom: 8px;

    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

/* hover */
div[data-testid="stSidebar"] button[data-key^="history_"]:hover {
    background-color: #e8f0fe;
    border-color: #4a90e2;
    color: #4a90e2;
}

</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>

/*  RED ACTIVE BUTTON */
div.stButton > button[kind="primary"] {
    background-color: #ff4b4b !important;
    color: white !important;
    border-radius: 10px !important;
    border: none !important;
    font-weight: 600;
}

/*  NORMAL BUTTON */
div.stButton > button[kind="secondary"] {
    background-color: #f5f5f5 !important;
    color: black !important;
    border-radius: 10px !important;
    border: 1px solid #ddd !important;
}

</style>
""", unsafe_allow_html=True)

query_params = st.query_params

# EDIT CLICK
if "edit_folder" in query_params:
    st.session_state.rename_folder = query_params["edit_folder"]

def format_query(q, max_len=20):
    if len(q) > max_len:
        return q[:max_len] + "..."
    return q




# SAFE REQUEST HANDLER


def safe_request(method, url, json=None, data=None, headers=None):

    try:
        if headers is None:
            headers = {}

        #  HANDLE BOTH CASES
        if json is not None:
            headers["Content-Type"] = "application/json"

        response = requests.request(
            method,
            url,
            json=json,
            data=data,  
            headers=headers
        )

        try:
            response_data = response.json()
        except:
            response_data = response.text

        return response.status_code, response_data

    except Exception as e:
        return 500, {"error": str(e)}


def show_error(data, default_msg="Something went wrong"):
    if isinstance(data, dict):
        if "error" in data and "message" in data["error"]:
            st.error(data["error"]["message"])
        elif "detail" in data:
            st.error(data["detail"])
        else:
            st.error(default_msg)
    else:
        st.error(default_msg)
def get_initials(name):
    if not name:
        return "U"   # default avatar letter

    parts = name.split()
    initials = ""

    for p in parts:
        initials += p[0]

    return initials.upper()
def get_all_companies():
    status, data = safe_request(
        "GET",
        f"{API_URL}/companies"
    )

    if status == 200:
        return data.get("data", [])
    
    return []
        
def get_sector(symbol):
    status, data = safe_request(
        "GET",
        f"{API_URL}/company/{symbol}/full-details",
        headers=headers
    )

    if status == 200:
        return data["data"].get("sector", "N/A")

    return "N/A"
# SESSION STATE INIT


if "token" not in st.session_state:
    st.session_state.token = None

if "username" not in st.session_state:
    st.session_state.username = None

if "token" in params and st.session_state.token is None:
    st.session_state.token = params["token"]

if "username" in params and st.session_state.username is None:
    st.session_state.username = params["username"]

if "sidebar_collapsed" not in st.session_state:
    st.session_state.sidebar_collapsed = False
if "search_results" not in st.session_state:
    st.session_state.search_results = None
    
if "show_create_folder" not in st.session_state:
    st.session_state.show_create_folder = False

if "show_stock_modal" not in st.session_state:
    st.session_state.show_stock_modal = False

if "portfolio_folders" not in st.session_state:
    st.session_state.portfolio_folders = {}

if "selected_folder" not in st.session_state:
    st.session_state.selected_folder = None
if "folders" not in st.session_state:
    st.session_state.folders = []
if "show_login" not in st.session_state:
    st.session_state.show_login = False


# AUTH FUNCTIONS


def login(username, password):
    return safe_request(
        "POST",
        f"{API_URL}/auth/login",
        data={
            "username": username,
            "password": password
        }
    )


def register(username, email, password):
    return safe_request(
        "POST",
        f"{API_URL}/auth/register",
        json={
            "username": username,
            "email": email,
            "password": password
        }
    )

def require_login():
    if st.session_state.token is None:
        st.session_state.show_login = True
        st.rerun()

def get_current_price(symbol):

    status, data = safe_request(
        "GET",
        f"{API_URL}/company/{symbol}/price",
        headers=headers
    )

    if status == 200:
        return data["data"]["price"]

    return 0

def format_query_title(q, max_len=35):
    if not q:
        return ""

    # make it cleaner 
    q = q.replace("_", " ").capitalize()

    # trim long queries
    if len(q) > max_len:
        return q[:max_len] + "..."

    return q

st.markdown("""
<style>
/*  Only primary buttons (your login button) */
div.stButton > button[kind="primary"] {
    background-color: #ff4b4b;
    color: white;
    border-radius: 8px;
    border: none;
    font-weight: 600;
}

/* Hover */
div.stButton > button[kind="primary"]:hover {
    background-color: #e63b3b;
    color: white;
}
</style>
""", unsafe_allow_html=True)
                        
 # LOGIN POPUP
if st.session_state.show_login:

    left, right = st.columns([3, 1])
    

    with left:
        st.image("images/login.jpg", width='stretch')

    #  RIGHT SIDE → LOGIN FORM
    with right:
        st.markdown("###  Welcome Back")
        st.caption("Login to access your AI stock dashboard")

        mode = st.radio(
            "Mode",
            ["Login", "Register"],
            horizontal=True,
            label_visibility="collapsed"
        )

        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if mode == "Register":
            email = st.text_input("Email")

        col1, col2 = st.columns(2)

        with col1:
            if st.button(mode,width='stretch', type="primary"):

                if username == "" or password == "":
                    st.warning("Fill required fields")
                else:

                    if mode == "Login":
                        status, data = login(username, password)

                        if status == 200:
                            st.session_state.token = data["access_token"]
                            st.session_state.username = username
                            st.session_state.show_login = False

                            #  persist in browser (important)
                            st.query_params["token"] = data["access_token"]
                            st.query_params["username"] = username

                            st.rerun()
                        else:
                            show_error(data)

                    else:
                        if email == "":
                            st.warning("Email required")
                        else:
                            status, data = register(username, email, password)

                            if status == 200:
                                st.success("Registered! Now login")
                            else:
                                show_error(data)

        with col2:
            if st.button("Cancel", width='stretch'):
                st.session_state.show_login = False
                st.rerun()

    st.stop()                    


# MAIN APP


headers = {}

if st.session_state.token:
    headers = {
        "Authorization": f"Bearer {st.session_state.token}"
    }

st.sidebar.title(" AI Stock Platform")

# -------------------------
# PAGE STATE
# -------------------------
if "page" not in st.session_state:
    st.session_state.page = "AI Screener"

params = st.query_params  

if "page" in params and "page" not in st.session_state:
    st.session_state.page = params["page"]

# HANDLE REDIRECT
if "go_to_company" in st.session_state:
    st.session_state.page = "Company Explorer"
    st.session_state.selected_company = st.session_state.go_to_company
    del st.session_state["go_to_company"]
    st.rerun()

# =========================
#  NAVIGATION (UPGRADED)
# =========================


st.markdown("""
<style>

/* remove sidebar spacing */
section[data-testid="stSidebar"] .block-container {
    padding-top: 1rem !important;
    padding-bottom: 0rem !important;
}

/* nav item */
.nav-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 10px;
    border-radius: 8px;
    font-size: 14px;
    cursor: pointer;
    margin-bottom: 2px;
    color: black !important;
}

/* hover */
.nav-item:hover {
    background-color: #f1f3f5;
    color: black !important;
}

/* active */
.nav-active {
    background-color: #e8f0fe;
    font-weight: 600;
    color: black !important;
}

/* remove link style COMPLETELY */
.nav-link {
    text-decoration: none !important;
    color: black !important;
}

.nav-link:visited,
.nav-link:hover,
.nav-link:active {
    color: black !important;
    text-decoration: none !important;
}

</style>
""", unsafe_allow_html=True)

pages = [
    ("AI Screener", "🔍"),
    ("Portfolio", "📂"),
    ("Watchlist", "⭐"),
    ("Alerts", "🚨"),
    ("Company Explorer", "🏢"),
]

for page_name, icon in pages:

    is_active = st.session_state.page == page_name

    btn_type = "primary" if is_active else "secondary"

    if st.sidebar.button(
        f"{icon}  {page_name}",
        key=f"nav_{page_name}",
        use_container_width=True,
        type=btn_type
    ):
        st.session_state.page = page_name
        st.rerun()
# =========================
# HISTORY (UNCHANGED)
# =========================

st.sidebar.markdown("# History")

if st.session_state.token:

    status, data = safe_request(
        "GET",
        f"{API_URL}/history",
        headers=headers
    )

    if status == 200:
        history_list = data.get("data", [])
    else:
        history_list = []

    if len(history_list) == 0:
        st.sidebar.caption("No searches yet")

    else:
        for i, q in enumerate(history_list):

            title = q.lower()

            if "pe" in title:
                title = "Low PE Stocks"
            elif "eps" in title:
                title = "High EPS Stocks"
            elif "growth" in title:
                title = "High Growth Stocks"
            elif "revenue" in title:
                title = "Revenue Based Stocks"
            elif "profit" in title:
                title = "Profit Based Stocks"
            else:
                title = title.capitalize()

            if len(title) > 15:
                title = title[:15] + "..."

            if st.sidebar.button(title, key=f"history_{i}", use_container_width=True):
                st.session_state.last_query = q

else:
    st.sidebar.caption("Login to see history")

# =========================
# HEADER (UNCHANGED)
# =========================

header_left, header_right = st.columns([9,1])

with header_left:
    if st.session_state.page == "AI Screener":
        st.title("StockX AI")
    elif st.session_state.page == "Portfolio":
        st.title("📂 Portfolio")
    elif st.session_state.page == "Watchlist":
        st.title("Watchlist")
    elif st.session_state.page == "Alerts":
        st.title("🚨 Alerts")
    elif st.session_state.page == "Company Explorer":
        st.title("🏢 Company Explorer")

with header_right:

    if st.session_state.token is None:
        if st.button("Login / Signup"):
            st.session_state.show_login = True
            st.rerun()
    else:
        if st.button("Logout"):
            st.session_state.token = None
            st.session_state.username = None
            st.query_params.clear()
            st.rerun()

if st.session_state.username:
    st.caption(f"Welcome, {st.session_state.username}")
 








#  SCREENER


if st.session_state.page == "AI Screener":
    st.session_state.show_login = False
    st.markdown("""
    Ask AI to find stocks based on financial metrics, growth indicators, or any custom criteria you have in mind.
    """)
    st.markdown("### Quick Queries")
    st.markdown("""
<style>
div.stButton > button {
    border-radius: 25px !important;
    padding: 8px 16px !important;
    border: 1px solid #ddd !important;
    background-color: #f9f9f9 !important;
    font-size: 14px !important;
}

div.stButton > button:hover {
    background-color: #e6f0ff !important;
    border-color: #4a90e2 !important;
    color: #4a90e2 !important;
}
</style>
""", unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    if col1.button("Low PE Stocks"):
        st.session_state.last_query = "companies where pe_ratio < 20"

    if col2.button("High EPS Stocks"):
        st.session_state.last_query = "companies where eps > 5"

    if col3.button("High Growth Stocks"):
        st.session_state.last_query = "companies where revenue_growth > 10"

    query = st.text_input(
        " Ask about stocks",
        value=st.session_state.get("last_query", ""),
        placeholder="Example: companies where pe_ratio < 20"
    )
    search_clicked = st.button(" Search Stocks")
    
    
    if search_clicked:

        #  NOT LOGGED IN
        if st.session_state.token is None:
            st.session_state.show_login = True
            st.rerun()

        # LOGGED IN
        if query.strip() == "":
            st.warning("Enter a query.")
        else:

            with st.spinner("AI analyzing stocks..."):
                status, data = safe_request(
                    "POST",
                    f"{API_URL}/screener",
                    json={"query": query},
                    headers=headers
                )

            if status == 200:

                results = data.get("data", [])

                st.session_state.search_results = results
                st.session_state.last_query = query
                st.session_state.page_number = 1
                st.success(f"Found {len(results)} companies")
                st.rerun()

            elif status == 401:
                st.error("Session expired. Please login again.")
                st.session_state.token = None
                st.rerun()

            else:
                show_error(data)

    if st.session_state.search_results is not None:

        if len(st.session_state.search_results) == 0:
            st.info("No companies matched your query.")

        else:

            df = pd.DataFrame(st.session_state.search_results)
            #  REMOVE DUPLICATES BASED ON SYMBOL
            df = df.drop_duplicates(subset=["symbol"], keep="first")

            st.markdown(f"### {len(df)} companies match your query")
            
            
            
            st.markdown("### Top 3 Companies")

            #  make sure values are numeric
            df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")

            #  remove missing values
            clean_df = df.dropna(subset=["market_cap"])

            #  now sort ALL companies and take top 3
            top = clean_df.sort_values(by="market_cap", ascending=False).head(3)

            for _, row in top.iterrows():
                st.markdown(f"""
                <div style="
                    padding:10px;
                    border-radius:10px;
                    background-color:#e6f9ec;
                    margin-bottom:8px;
                ">
                    <span style="
                        color:#16a34a;
                        font-weight:700;
                        font-size:16px;
                    ">
                        {row['symbol']}
                    </span>
                    <br>
                    <span style="color:#333;">
                        Market Cap: ₹{round(row['market_cap']/1_000_000_000,2)}B
                    </span>
                </div>
                """, unsafe_allow_html=True)

            st.subheader(" Screening Results")

            # ---------- Pagination ----------
            rows_per_page = 10

            total_rows = len(df)
            total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)

            # store current page
            if "page_number" not in st.session_state:
                st.session_state.page_number = 1
            start = (st.session_state.page_number - 1) * rows_per_page
            end = start + rows_per_page

            paginated_df = df.iloc[start:end].copy()

           # ========= FORMAT FUNCTIONS =========

            # =========================
            #  FORMAT FUNCTION (ONLY K / M / B / T)
            # =========================

            def format_number(x):
                if pd.isna(x):
                    return ""

                x = float(x)

                if x >= 1_000_000_000_000:
                    return f"{x/1_000_000_000_000:.2f}T"
                elif x >= 1_000_000_000:
                    return f"{x/1_000_000_000:.2f}B"
                elif x >= 1_000_000:
                    return f"{x/1_000_000:.2f}M"
                elif x >= 1_000:
                    return f"{x/1_000:.2f}K"
                else:
                    return f"{x:.2f}"


            # =========================
            # 🔥 FORCE NUMERIC (IMPORTANT)
            # =========================

            for col in ["market_cap", "revenue", "debt", "price_change_1y", "revenue_growth", "eps", "pe_ratio", "score"]:
                if col in paginated_df.columns:
                    paginated_df[col] = pd.to_numeric(paginated_df[col], errors="coerce")


            # =========================
            #  APPLY FORMATTING
            # =========================

            # 💰 BIG MONEY VALUES
            for col in ["market_cap", "revenue", "debt"]:
                if col in paginated_df.columns:
                    paginated_df[col] = paginated_df[col].apply(lambda x: f"₹{format_number(x)}")

            # 📈 REMOVE % → convert to readable numbers
            for col in ["price_change_1y", "revenue_growth"]:
                if col in paginated_df.columns:
                    paginated_df[col] = paginated_df[col].apply(format_number)

            # 🔢 SMALL NUMBERS
            for col in ["eps", "pe_ratio"]:
                if col in paginated_df.columns:
                    paginated_df[col] = paginated_df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "")

            # ⭐ SCORE (already big → convert to M)
            if "score" in paginated_df.columns:
                paginated_df["score"] = paginated_df["score"].apply(
                    lambda x: format_number(x) if pd.notna(x) else ""
                )


            # =========================
            #  DISPLAY
            # =========================

            st.data_editor(paginated_df, width='stretch', disabled=True)
            

            
            
            

            

            # ---------- Compact Pagination ----------
            if total_pages > 1:

                left_space, nav = st.columns([9,1])

                with nav:

                    prev_col, page_col, next_col = st.columns([1,1,1], gap="small")

                    if prev_col.button("◀", key="prev_page"):
                        if st.session_state.page_number > 1:
                            st.session_state.page_number -= 1

                    page_col.markdown(
                        f"""
                        <div style="
                            display:flex;
                            align-items:center;
                            justify-content:center;
                            font-size:14px;
                            margin-top:4px;
                        ">
                            {st.session_state.page_number}/{total_pages}
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    if next_col.button("▶", key="next_page"):
                        if st.session_state.page_number < total_pages:
                            st.session_state.page_number += 1
                            
                            
            #  EPS COMPARISON (AFTER RESULTS)
            st.markdown("### 📊 EPS Comparison")

            if "eps" in df.columns:
                clean_eps_df = df.dropna(subset=["eps"])

                if not clean_eps_df.empty:
                    st.bar_chart(clean_eps_df.set_index("symbol")["eps"])
                else:
                    st.info("No EPS data available for comparison")

            

            
            # ================= SAVE COMPANY =================
            st.markdown("### Add to WatchList")

            col1, col2 = st.columns([4,1], gap="small")

            with col1:
                save_symbol = st.selectbox(
                    "Select Company",
                    df["symbol"].tolist(),
                    key="save_symbol"
                )

            with col2:
                st.markdown("<br>", unsafe_allow_html=True)  # align vertically

                if st.button(" Add", width='stretch'):

                    if st.session_state.token is None:
                        st.session_state.show_login = True
                        st.rerun()

                    if not save_symbol or save_symbol.strip() == "":
                        st.warning("Select a valid company")
                        st.stop()

                    clean_symbol = save_symbol.strip().upper()

                    status, data = safe_request(
                        "POST",
                        f"{API_URL}/watchlist",
                        json={"stock_symbol": clean_symbol},
                        headers=headers
                    )

                    if status == 200:
                        st.success(f"{save_symbol} added ")
                    else:
                        show_error(data)


           
            # ================= ADD TO PORTFOLIO =================
            st.markdown("### 📂 Add to Portfolio")

            #  get all companies from database
            companies = get_all_companies()

            #  extract symbols
            symbols = [c["symbol"] for c in companies]

            #  safety check
            if not symbols:
                st.warning("No companies found in database")
                st.stop()

            #  selectbox
            portfolio_symbol = st.selectbox(
                "Select Company",
                symbols,
                key="portfolio_symbol"
            )

            #  CURRENT PRICE
            current_price = 0
            if portfolio_symbol:
                current_price = get_current_price(portfolio_symbol)

                if current_price > 0:
                    st.info(f"Current Price: ₹{round(current_price,2)}")
                else:
                    st.warning("Price not available")


            #  LOAD FOLDERS (FIXED CLEAN LOGIC)
            folder_options = []

            if st.session_state.token:
                status, data = safe_request(
                    "GET",
                    f"{API_URL}/portfolio",
                    headers=headers
                )

                if status == 200:
                    portfolio_data = data.get("data", [])

                    folder_set = set()

                    for item in portfolio_data:
                        if "folder_name" in item and item["folder_name"]:
                            folder_set.add(item["folder_name"])

                    folder_options = sorted(list(folder_set))

            # fallback
            if not folder_options:
                folder_options = ["My Stocks"]


            #  INLINE INPUTS (MAIN CHANGE)
            col1, col2, col3, col4 = st.columns([1,1,1,1], gap="small")

            with col1:
                quantity = st.number_input("Qty", min_value=1, value=1)

            with col2:
                buy_price = st.number_input("Buy ₹", min_value=0.0, value=0.0)

            with col3:
                selected_folder = st.selectbox("Folder", folder_options)

            with col4:
                st.markdown("<br>", unsafe_allow_html=True)  # 🔥 pushes button down
                add_clicked = st.button("➕ Add", use_container_width=True)


            #  ADD BUTTON LOGIC
            if add_clicked:

                if st.session_state.token is None:
                    st.session_state.show_login = True
                    st.rerun()

                status, data = safe_request(
                    "POST",
                    f"{API_URL}/portfolio",
                    json={
                        "stock_symbol": portfolio_symbol,
                        "quantity": int(quantity),
                        "buy_price": float(buy_price),
                        "folder_name": selected_folder
                    },
                    headers=headers
                )

                if status == 200:
                    st.success(f"{portfolio_symbol} added to Portfolio ✅")
                else:
                    show_error(data)


            #  REMOVE DUPLICATES + CLEAN LIST
            symbols = list(dict.fromkeys(df["symbol"].tolist()))

            #  INLINE VIEW COMPANY (LIKE SEARCH BAR)
            col1, col2 = st.columns([3,1], gap="small")

            with col1:
                selected_symbol = st.selectbox(
                    "Select Company",
                    symbols,
                    key="view_company_symbol",
                    label_visibility="collapsed"
                )

            with col2:
                view_clicked = st.button("🔍 View company", use_container_width=True)

            if view_clicked:
                st.session_state["go_to_company"] = selected_symbol
                st.rerun()

            

            st.markdown("### AI Insight")

            st.info(
                f"The screener found {len(df)} companies matching your filters. "
                "These companies satisfy the financial conditions you provided."
            )
    

#  PORTFOLIO
elif st.session_state.page == "Portfolio":


    # ---------- SESSION STATE ----------
    

    if "selected_folder" not in st.session_state:
        st.session_state.selected_folder = None

    if "show_create_folder" not in st.session_state:
        st.session_state.show_create_folder = False

    if "show_add_stock" not in st.session_state:
        st.session_state.show_add_stock = False


    # ---------- CREATE FOLDER BUTTON ----------
    if st.session_state.selected_folder is None:
        col1, col2 = st.columns([10,2])

        with col2:
            if st.button("➕ Create Folder"):

                if st.session_state.token is None:
                    st.session_state.show_login = True
                    st.rerun()

                st.session_state.show_create_folder = True
        
        # ---------- CREATE FOLDER MODAL ----------
        if st.session_state.show_create_folder:

            st.subheader("Create Folder")

            folder_name = st.text_input("Folder Name")

            col1, col2 = st.columns(2)

            with col1:
                if st.button("Create"):

                    if st.session_state.token is None:
                        st.session_state.show_login = True
                        st.rerun()

                    if folder_name.strip() == "":
                        st.warning("Enter folder name")
                        st.stop()

                    clean_name = folder_name.strip()

                    folders = []   #  ALWAYS DEFINE FIRST

                    if st.session_state.token:
                        status, data = safe_request(
                            "GET",
                            f"{API_URL}/folders",
                            headers=headers
                        )

                        if status == 200:
                            folders = data.get("data", [])

                    if clean_name.lower() in [f.lower() for f in folders]:
                        st.warning("Folder already exists")
                        st.stop()

                    status, data = safe_request(
                        "POST",
                        f"{API_URL}/folders",
                        json={"folder_name": clean_name},
                        headers={
                            "Authorization": f"Bearer {st.session_state.token}"
                        }
                    )

                    if status == 200:
                        st.success("Folder created")
                        st.session_state.show_create_folder = False
                        st.rerun()
                    else:
                        st.error("Folder already exists")

            with col2:
                if st.button("Cancel"):
                    st.session_state.show_create_folder = False
                    st.rerun()


    # ---------- SHOW FOLDERS ----------
    # ---------- SHOW FOLDERS ----------
    if st.session_state.selected_folder is None:

        folders = []

        if st.session_state.token is None:
            st.info("Login to view your portfolio")

        else:
            status, data = safe_request(
                "GET",
                f"{API_URL}/folders",
                headers=headers
            )

            if status == 200:
                folders = list(reversed(data.get("data", [])))

        #  GET FULL PORTFOLIO DATA ONCE
        portfolio_data = []
        status_p, data_p = safe_request(
            "GET",
            f"{API_URL}/portfolio",
            headers=headers
        )

        if status_p == 200:
            portfolio_data = data_p.get("data", [])

        df_all = pd.DataFrame(portfolio_data) if portfolio_data else pd.DataFrame()

        if len(folders) == 0:
            st.info("No folders yet")

        else:

            #  HEADER
            h1, h2, h3, h4 = st.columns([2,1,1,1])
            h1.markdown("**Folder Name**")
            h2.markdown("**Value**")
            h3.markdown("**Change**")
            h4.markdown("**Change %**")

            st.markdown(
                "<hr style='margin:4px 0; border:0.5px solid #ddd;'>",
                unsafe_allow_html=True
            )

            #  GLOBAL STYLES (CLEAN + COMPACT)
            st.markdown("""
            <style>

            /*  compact button (folder name) */
            div.stButton > button {
                background: none !important;
                border: none !important;
                padding: 2px 4px !important;
                margin: 0 !important;
                font-size: 15px !important;
                font-weight: 600 !important;
                text-align: left !important;
            }

            /* hover effect on folder name */
            div.stButton > button:hover {
                color: #4a90e2 !important;
                cursor: pointer;
            }

            /* remove extra space below buttons */
            div.stButton {
                margin-bottom: 0px !important;
            }

            /* reduce vertical spacing between rows */
            .block-container {
                padding-top: 1rem !important;
                padding-bottom: 0rem !important;
            }

            </style>
            """, unsafe_allow_html=True)

            #  LOOP FOLDERS
            for i, folder in enumerate(folders):

                folder_value = 0
                folder_invested = 0
                stock_count = 0

                if not df_all.empty and "folder_name" in df_all.columns:
                    f_df = df_all[df_all["folder_name"] == folder]
                    if not f_df.empty:
                        folder_value = f_df["current_value"].sum()
                        folder_invested = f_df["invested"].sum()
                        stock_count = len(f_df)

                folder_change = folder_value - folder_invested
                folder_percent = (folder_change / folder_invested * 100) if folder_invested > 0 else 0

                color = "green" if folder_change >= 0 else "red"
                arrow = "▲" if folder_change >= 0 else "▼"

                #  ROW (NO EXTRA SPACE)
                c1, c2, c3, c4 = st.columns([2,1,1,1])

                # 📁 FOLDER NAME (CLICKABLE)
                with c1:
                    if st.button(f"📁 {folder}", key=f"folder_{i}"):
                        st.session_state.selected_folder = folder
                        st.rerun()

                    st.markdown(
                        f"<span style='font-size:11px; color:gray;'>{stock_count} Stocks</span>",
                        unsafe_allow_html=True
                    )

                #  VALUE
                with c2:
                    st.write(f"₹{round(folder_value,2)}")

                #  CHANGE
                with c3:
                    st.markdown(
                        f"<span style='color:{color}; font-weight:500;'>{arrow} {round(folder_change,2)}</span>",
                        unsafe_allow_html=True
                    )

                #  PERCENT
                with c4:
                    st.markdown(
                        f"<span style='color:{color}; font-weight:500;'>{round(folder_percent,2)}%</span>",
                        unsafe_allow_html=True
                    )

                #  THIN DIVIDER (NO BIG GAP)
                st.markdown(
                    "<hr style='margin:2px 0; border:0.3px solid #eee;'>",
                    unsafe_allow_html=True
                )

                    
                
                
                
                

                


    # ---------- OPEN FOLDER ----------
    if st.session_state.selected_folder is not None:

        folder = st.session_state.selected_folder

        col1, col2,col3 = st.columns([1,8,2])

        with col1:
            if st.button("⬅"):
                st.session_state.selected_folder = None
                st.rerun()

        with col2:
            st.markdown(f"### {folder}")
        with col3:
            if st.button("➕ Add Stock"):
                st.session_state.show_add_stock = True
    # -------- ADD STOCK UI INSIDE FOLDER --------
        if st.session_state.show_add_stock:

            st.markdown(f"### Add Stock to {folder}")

            symbol = st.text_input("Enter Symbol").upper()
            col1, col2 = st.columns(2)

            with col1:
                quantity = st.number_input("Quantity", min_value=1, value=1)

            with col2:
                buy_price = st.number_input("Buy Price", min_value=0.0, value=0.0)

            current_price = 0

            if symbol:
                current_price = get_current_price(symbol)

                if current_price > 0:
                    st.success(f"Current Price: ₹{round(current_price,2)}")
                    st.info(f"Total Value: ₹{round(current_price * quantity,2)}")
                else:
                    st.warning("Invalid symbol")

            colA, colB = st.columns(2)

            with colA:
                if st.button("Add Now"):

                    #  LOGIN CHECK (ADD THIS)
                    if st.session_state.token is None:
                        st.session_state.show_login = True
                        st.rerun()

                    if symbol == "":
                        st.warning("Enter symbol")
                        st.stop()

                    status, data = safe_request(
                        "POST",
                        f"{API_URL}/portfolio",
                        json={
                            "stock_symbol": symbol,
                            "quantity": quantity,
                            "buy_price": buy_price,
                            "folder_name": folder
                        },
                        headers=headers
                    )

                    if status == 200:
                        st.success("Stock Added ")
                        st.session_state.show_add_stock = False
                        st.rerun()
                    else:
                        show_error(data)

            with colB:
                if st.button("Cancel"):
                    st.session_state.show_add_stock = False
                    st.rerun()
        # ---------- LOAD STOCKS ----------
        status, data = safe_request(
            "GET",
            f"{API_URL}/portfolio",
            headers=headers
        )

        if status == 200 and "data" in data:

            df = pd.DataFrame(data["data"])
            

            if df.empty:
                st.info("No stocks in portfolio")

            else:

                if "folder_name" in df.columns:
                    folder_df = df[
                        df["folder_name"].fillna("").str.strip()
                        == str(folder).strip()
                    ]
                else:
                    st.warning("No folder data available")
                    folder_df = pd.DataFrame()

                if folder_df.empty:
                    st.info("No stocks in this folder")

                else:

                    st.markdown("### Your Stocks")
                    folder_df["current_price"] = folder_df["symbol"].apply(get_current_price)
                    folder_df["current_value"] = folder_df["current_price"] * folder_df["quantity"]
                    folder_df["invested"] = folder_df["buy_price"] * folder_df["quantity"]
                    folder_df["profit"] = folder_df["current_value"] - folder_df["invested"]
                    folder_df["profit_percent"] = (folder_df["profit"] / folder_df["invested"]) * 100

                    # fallback company name
                    folder_df["sector"] = folder_df["symbol"].apply(
    lambda x: get_sector(x) or "Unknown")

                    display_df = pd.DataFrame({
                        "Symbol": folder_df["symbol"],
                        "Sector": folder_df["sector"],
                        "Quantity": folder_df["quantity"],
                        "Buy Price": folder_df["buy_price"],
                        "Current Price": folder_df["current_price"].round(2),
                        "Value": folder_df["current_value"].round(2),
                        "Change %": folder_df["profit_percent"].round(2)
                    })


                    # ---------- AI SCREENER STYLE TABLE ----------

                    display_df = pd.DataFrame({
                        "Symbol": folder_df["symbol"],
                        "Company": folder_df["company_name"],
                        "Quantity": folder_df['quantity'],
                        "Buy Price": folder_df["buy_price"],
                        "Current Price": folder_df["current_price"].round(2),
                        "Value": folder_df["current_value"].round(2),
                        "Change %": folder_df["profit_percent"].round(2),
                    })

                    h1, h2, h3, h4, h5, h6, h7, h8, h9 = st.columns([2,3,2,2,2,2,2,1,1])

                    h1.markdown("**Symbol**")
                    h2.markdown("**Sector**")
                    h3.markdown("**Qty**")
                    h4.markdown("**Buy Price**")
                    h5.markdown("**Current**")
                    h6.markdown("**Value**")
                    h7.markdown("**Change %**")
                    h8.markdown("**Edit**")
                    h9.markdown("**Delete**")
                    
                    st.markdown(
    "<hr style='margin:1px 0 4px 0; border:0.5px solid #ddd;'>",
    unsafe_allow_html=True
)
                    # ROWS
                    for _, row in folder_df.iterrows():

                        cols = st.columns([2,3,2,2,2,2,2,1,1])

                        with cols[0]:

                            token = st.session_state.token
                            username = st.session_state.username
                            symbol = row["symbol"]

                            st.markdown(
                                f"""
                                <a href="?symbol={symbol}&token={token}&username={username}" 
                                style="text-decoration:none; font-size:16px;">
                                {symbol}
                                </a>
                                """,
                                unsafe_allow_html=True
                            )

                        with cols[1]:
                            st.write(row["sector"])

                        with cols[2]:
                            st.markdown(f"{row['quantity']}")

                        with cols[3]:
                            st.markdown(f"₹{round(row['buy_price'], 2)}")

                        with cols[4]:
                            st.markdown(f"₹{round(row['current_price'], 2)}")

                        with cols[5]:
                            st.markdown(f"₹{round(row['current_value'], 2)}")

                        with cols[6]:

                            percent = round(row["profit_percent"], 2)

                            if percent >= 0:
                                st.markdown(
                                    f"<span style='color:green; font-weight:600;'>▲ {percent}%</span>",
                                    unsafe_allow_html=True
                                )
                            else:
                                st.markdown(
                                    f"<span style='color:red; font-weight:600;'>▼ {abs(percent)}%</span>",
                                    unsafe_allow_html=True
                                )

                        with cols[7]:
                            if st.button("✏️", key=f"edit_{row['id']}"):
                                st.session_state.edit_id = row["id"]
                                st.session_state.edit_qty = row["quantity"]
                                st.session_state.edit_price = row["buy_price"]
                        with cols[8]:
                            if st.button("🗑️", key=f"delete_{row['id']}"):

                                st.session_state.delete_id = row["id"]
                                st.session_state.delete_symbol = row["symbol"]  # optional (nice UX)

                                st.rerun()
                        st.markdown(
                            "<hr style='margin:1px 0; border:0.5px solid #ddd;'>",
                            unsafe_allow_html=True
                        )
                    
                    
                    # -------- DELETE CONFIRMATION --------
                    if "delete_id" in st.session_state:

                        st.warning(f" Are you sure you want to delete {st.session_state.delete_symbol}?")

                        col1, col2 = st.columns(2)

                        with col1:
                            if st.button("Confirm Delete"):

                                status, data = safe_request(
                                    "DELETE",
                                    f"{API_URL}/portfolio/{st.session_state.delete_id}",
                                    headers=headers
                                )

                                if status == 200:
                                    st.success("Stock deleted 🗑️")
                                    del st.session_state.delete_id
                                    del st.session_state.delete_symbol
                                    st.rerun()
                                else:
                                    show_error(data)

                        with col2:
                            if st.button("Cancel", key="cancel_stock_delete"):
                                del st.session_state.delete_id
                                del st.session_state.delete_symbol
                                st.rerun()

                    
                    
                    if "edit_id" in st.session_state:

                        st.markdown("### ✏️ Update Stock")

                        #  ACTION SELECT
                        action = st.radio(
                            "Action",
                            ["Buy ➕", "Sell ➖"]
                        )

                        #  INPUT
                        qty = st.number_input("Quantity", min_value=1)

                        price = 0
                        if action == "Buy ➕":
                            price = st.number_input("Buy Price", min_value=0.0)

                        col1, col2 = st.columns(2)

                        with col1:
                            if st.button("Confirm"):

                                current_qty = st.session_state.edit_qty
                                current_price = st.session_state.edit_price

                                # -------- BUY --------
                                if action == "Buy ➕":

                                    new_qty = current_qty + qty

                                    new_price = (
                                        (current_qty * current_price + qty * price)
                                        / new_qty
                                    )

                                # -------- SELL --------
                                else:

                                    new_qty = current_qty - qty

                                    if new_qty <= 0:
                                        # DELETE STOCK
                                        safe_request(
                                            "DELETE",
                                            f"{API_URL}/portfolio/{st.session_state.edit_id}",
                                            headers=headers
                                        )

                                        st.success("Stock removed 🗑️")
                                        del st.session_state.edit_id
                                        st.rerun()

                                    new_price = current_price

                                # -------- UPDATE --------
                                status, data = safe_request(
                                    "PUT",
                                    f"{API_URL}/portfolio/{st.session_state.edit_id}",
                                    json={
                                        "quantity": int(new_qty),
                                        "buy_price": float(new_price)
                                    },
                                    headers=headers
                                )

                                if status == 200:
                                    st.success("Updated ")
                                    del st.session_state.edit_id
                                    st.rerun()
                                else:
                                    show_error(data)

                        with col2:
                            if st.button("Cancel"):
                                del st.session_state.edit_id
                                st.rerun()
                    
        


#WATCHLIST  
              
elif st.session_state.page == "Watchlist":

    import matplotlib.pyplot as plt
    import numpy as np

    # ---------- MINI GRAPH ----------
    def get_mini_chart(symbol, headers):

        status, data = safe_request(
            "GET",
            f"{API_URL}/company/{symbol}/price-history?period=1M",
            headers=headers
        )

        if status != 200:
            return None

        prices = data.get("data", [])
        if not prices:
            return None

        y = np.array([p["close"] for p in prices])

        color = "#00C853" if y[-1] >= y[0] else "#FF3D00"

        fig, ax = plt.subplots(figsize=(1.8, 0.5))
        ax.plot(y, color=color, linewidth=1.5)
        ax.fill_between(range(len(y)), y, y.min(), color=color, alpha=0.1)
        ax.axis("off")

        return fig


    # ---------------- ADD STOCK ----------------
    st.subheader("Add Stock")

    # keep input (no red lines), but not used
    user_input = st.text_input("Enter company symbol")


    # ================= ADD TO WATCHLIST =================

    if st.button("➕ Add to Watchlist"):

        if st.session_state.token is None:
            st.session_state.show_login = True
            st.rerun()

        stock_symbol = user_input.strip().upper()

        if not stock_symbol:
            st.warning("Enter a stock symbol")
            st.stop()

        #  LOADING SPINNER
        with st.spinner("Adding to watchlist..."):
            status, data = safe_request(
                "POST",
                f"{API_URL}/watchlist",
                json={"stock_symbol": stock_symbol},
                headers=headers
            )

        #  STORE MESSAGE (IMPORTANT)
        if status == 200:
            st.session_state.added_msg = f"{stock_symbol} added ✅"

        elif status == 400:

            msg = ""

            if isinstance(data, dict):
                msg = (
                    data.get("error", {}).get("message")
                    or data.get("detail")
                    or str(data)
                )
            else:
                msg = str(data)

            if "Invalid symbol" in msg:
                st.session_state.added_msg = "Enter correct symbol ❌"

            elif "Already" in msg:
                st.session_state.added_msg = "Already exists in watchlist ⚠️"

            else:
                st.session_state.added_msg = msg

        else:
            st.session_state.added_msg = "Something went wrong ❌"


    # ================= SHOW MESSAGE =================

    if "added_msg" in st.session_state:

        msg = st.session_state.added_msg

        if "added" in msg:
            st.success(msg)

        elif "Already" in msg:
            st.warning(msg)

        else:
            st.error(msg)

        import time
        time.sleep(1.5)   #  makes it visible

        del st.session_state["added_msg"]
        st.rerun()


    st.divider()

    # ---------------- WATCHLIST ----------------
    st.subheader("Your Watchlist")

    if st.session_state.token is None:
        st.info("Login to view your watchlist")
        st.stop()

    status, data = safe_request(
        "GET",
        f"{API_URL}/watchlist",
        headers=headers
    )

    if status != 200:
        show_error(data)
        st.stop()

    df = pd.DataFrame(data.get("data", []))

    if df.empty:
        st.info("No stocks in watchlist")
        st.stop()

    # ---------------- FETCH DATA ----------------
    rows = []
    def format_number(x):
        if x is None:
            return ""

        x = float(x)

        if x >= 1_000_000_000_000:
            return f"{x/1_000_000_000_000:.2f}T"
        elif x >= 1_000_000_000:
            return f"{x/1_000_000_000:.2f}B"
        elif x >= 1_000_000:
            return f"{x/1_000_000:.2f}M"
        elif x >= 1_000:
            return f"{x/1_000:.2f}K"
        else:
            return f"{x:.2f}"

    for _, row in df.iterrows():

        symbol = row["symbol"]
        watch_id = row["id"]

        status, details = safe_request(
            "GET",
            f"{API_URL}/company/{symbol}/details",
            headers=headers
        )

        if status != 200:
            continue

        d = details["data"]

        rows.append({
            "id": watch_id,
            "Symbol": d["symbol"],
            "Company": d["company_name"],
            "PE": f"{round(d.get('pe_ratio') or 0, 2)}",
            "EPS": f"{round(d.get('eps') or 0, 2)}",
            "Revenue": f"₹{format_number(d.get('revenue'))}",
            "Debt": f"₹{format_number(d.get('debt'))}",
            "Market Cap": f"₹{format_number(d.get('market_cap'))}",
        })

    df_full = pd.DataFrame(rows)

    if df_full.empty:
        st.warning("No data available")
        st.stop()

    # ---------------- HEADER ----------------
    h1, h2, h3, h4, h5, h6, h7, h8 = st.columns([3,1.5,2,2,2,2,2,1])

    h1.markdown("**Company**")
    h2.markdown("**Trend**")
    h3.markdown("**PE**")
    h4.markdown("**EPS**")
    h5.markdown("**Revenue**")
    h6.markdown("**Debt**")
    h7.markdown("**Market Cap**")
    h8.markdown("**Delete**")

    st.divider()

    # ---------------- ROWS ----------------
    for i, row in df_full.iterrows():

        c1, c2, c3, c4, c5, c6, c7, c8 = st.columns([3,1.5,2,2,2,2,2,1])

        symbol = row["Symbol"]
        watch_id = row["id"]

        token = st.session_state.token
        username = st.session_state.username

        c1.markdown(
            f"<a href='?symbol={symbol}&token={token}&username={username}' "
            f"style='text-decoration:none;color:inherit;font-weight:600'>"
            f"{row['Company']}</a>",
            unsafe_allow_html=True
        )

        fig = get_mini_chart(symbol, headers)
        if fig:
            c2.pyplot(fig)

        c3.write(row["PE"])
        c4.write(row["EPS"])
        c5.write(row["Revenue"])
        c6.write(row["Debt"])
        c7.write(row["Market Cap"])

        # DELETE BUTTON
        if c8.button("🗑️", key=f"delete_{watch_id}_{i}"):

            status, data = safe_request(
                "DELETE",
                f"{API_URL}/watchlist/{watch_id}",
                headers=headers
            )

            if status == 200:
                st.success("Removed")
                st.rerun()
            else:
                show_error(data)

        st.markdown(
            "<hr style='margin:4px 0; border:0.5px solid #ddd;'>",
            unsafe_allow_html=True
        )

#  ALERTS


elif st.session_state.page == "Alerts":
    
    
    # DEFAULT VIEW
    if "alert_view" not in st.session_state:
        st.session_state.alert_view = "alerts"

    col_left, col_mid, col_right = st.columns([2,2,1])

    # Alerts button
    with col_left:
        if st.button(
            "Alerts",
            key="alerts_tab",
        ):
            st.session_state.alert_view = "alerts"

    # Triggered button
    with col_mid:
        if st.button(
            "Triggered Alerts",
            key="triggered_tab",
            type="primary" if st.session_state.alert_view == "triggered" else "secondary"
        ):
            st.session_state.alert_view = "triggered"

    # Create button (right side)
    with col_right:
        if st.session_state.alert_view == "alerts":
            if st.button("➕ Create", width='stretch'):
                if st.session_state.token is None:
                    st.session_state.show_login = True
                    st.rerun()
                st.session_state.show_alert_modal = True
    st.markdown("<br>", unsafe_allow_html=True)
    
    
    # ---------------- CREATE ALERT ----------------
    if "show_alert_modal" not in st.session_state:
        st.session_state.show_alert_modal = False


    if st.session_state.show_alert_modal:

        st.markdown("### Create Alert")

        symbol = st.text_input("Symbol").upper()

        status, data = safe_request(
            "GET",
            f"{API_URL}/alerts/metrics",
            headers=headers
        )

        metrics = []

        if status == 200:
            metrics = data.get("data", [])
        else:
            st.warning("Failed to load metrics")

        metrics = [
            "pe_ratio",
            "eps",
            "revenue",
            "debt",
            "market_cap",
            "revenue_growth",
            "price_change_1y"
        ]

        metric = st.selectbox("Metric", metrics)

        condition = st.selectbox(
            "Condition",
            ["<", ">", "<=", ">=", "="]
        )

        threshold = st.number_input("Value", value=0.0)

        col1, col2 = st.columns(2)

        with col1:
            if st.button("Create Alert Confirm"):

                if symbol.strip() == "":
                    st.warning("Enter symbol")
                    st.stop()
                require_login()

                status, data = safe_request(
                    "POST",
                    f"{API_URL}/alerts",
                    json={
                        "stock_symbol": symbol,
                        "metric": metric,
                        "condition": condition,
                        "threshold": threshold
                    },
                    headers=headers
                )

                if status == 200:
                    st.success("Alert created ")
                    st.session_state.show_alert_modal = False
                    st.rerun()
                else:
                    show_error(data)

        with col2:
            if st.button("Cancel"):
                st.session_state.show_alert_modal = False
                st.rerun()

            
    # ---------------- SHOW ALERTS ----------------

    if st.session_state.alert_view == "alerts":

        

        if st.session_state.token is None:
            st.info("Login to view alerts")

        else:
            status, data = safe_request(
                "GET",
                f"{API_URL}/alerts",
                headers=headers
            )

            if status == 200:

                alerts = data.get("data", [])

                if not alerts:
                    st.info("No alerts created")

                else:
                    import pandas as pd

                    df = pd.DataFrame(alerts)

                    df_display = pd.DataFrame({
                    "Symbol": df["stock_symbol"],
                    "Metric": df["metric"],
                    "Condition": df["operator"] + " " + df["threshold"].astype(str),
                    "Active": df["is_active"].apply(
                        lambda x: "✅" if x else "❌"
                    ),
                    "Created At": pd.to_datetime(df["created_at"]).dt.strftime("%d-%m-%Y  %H:%M")
                })

                    st.dataframe(df_display, width='stretch')

            else:
                show_error(data)


    elif st.session_state.alert_view == "triggered":


        if st.session_state.token is None:
            st.info("Login to view alerts")

        else:
            status, data = safe_request(
                "GET",
                f"{API_URL}/alerts/check",
                headers=headers
            )

            if status == 200:

                alerts = data.get("data", [])

                if not alerts:
                    st.info("No alerts triggered")

                else:
                    import pandas as pd

                    df = pd.DataFrame(alerts)

                    df_display = pd.DataFrame({
                        "Symbol": df["symbol"],
                        "Metric": df["metric"],
                        "Current Value": df["current_value"],
                        "Condition": df["condition"]
                    })

                    st.dataframe(df_display, width='stretch')

            else:
                show_error(data)

    
# ================================
#  COMPANY EXPLORER 
# ================================
elif st.session_state.page == "Company Explorer":
    st.markdown("""
<style>

/* Container */
div[role="radiogroup"] {
    display: flex;
    gap: 0px;
    background-color: #f1f3f5;
    padding: 5px;
    border-radius: 10px;
    width: fit-content;
}

/* Each tab */
div[role="radio"] {
    padding: 6px 14px;
    border-radius: 8px;
    font-size: 13px;
    color: #555;
    background: transparent;
    border: none;
}

/* Selected tab */
div[role="radio"][aria-checked="true"] {
    background-color: white;
    color: black;
    font-weight: 600;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}

</style>
""", unsafe_allow_html=True)

    # SEARCH
    col1, col2 = st.columns([4,1])

    with col1:
        symbol_input = st.text_input(
            "",
            placeholder="Enter Company Symbol (e.g., INFY, AAPL)",
            value=st.session_state.get("selected_company", ""),
            key="company_search_box",
            label_visibility="collapsed"
        )

    with col2:
        search_clicked = st.button("Search", use_container_width=True)

    # LOGIC
    if search_clicked:
        if symbol_input.strip() != "":
            st.session_state["selected_company"] = symbol_input.upper()
            st.rerun()

    symbol = st.session_state.get("selected_company")

    # PERIOD STATE
    if "chart_period" not in st.session_state:
        st.session_state.chart_period = "1Y"

    if symbol:

        # FETCH DATA
        detail_status, detail_data = safe_request(
            "GET",
            f"{API_URL}/company/{symbol}/full-details",
            headers=headers
        )

        price_status, price_data = safe_request(
            "GET",
            f"{API_URL}/company/{symbol}/price-history?period={st.session_state.chart_period}",
            headers=headers
        )

        left, right = st.columns([2,1])

        # =========================
        # LEFT SIDE
        # =========================
        with left:

            if price_status == 200:

                df = pd.DataFrame(price_data["data"])

                if not df.empty:

                    df["date"] = pd.to_datetime(df["date"])
                    df = df.sort_values("date")

                    import plotly.graph_objects as go

                    # CALCULATIONS
                    first_price = df["close"].iloc[0]
                    last_price = df["close"].iloc[-1]

                    high = df["high"].max()
                    low = df["low"].min()

                    latest = df.iloc[-1]

                    change = last_price - first_price
                    percent = (change / first_price) * 100

                    volume = df["close"].sum()

                    color = "green" if change >= 0 else "red"
                    arrow = "▲" if change >= 0 else "▼"

                    # TOP PRICE UI
                    st.markdown(f"""
                    <div style="font-size:34px; font-weight:700;">
                        ₹{round(last_price,2)}
                    </div>

                    <div style="color:{color}; font-size:18px; font-weight:600;">
                        {arrow} {round(change,2)} ({round(percent,2)}%)
                    </div>

                    <div style="color:gray; font-size:14px;">
                        Volume: {round(volume,2)}
                    </div>
                    """, unsafe_allow_html=True)

                    st.markdown("<br>", unsafe_allow_html=True)

                    # BUTTONS (ABOVE GRAPH)
                    options = ["1D", "1W", "1M", "1Y", "5Y"]

                    period = st.radio(
                        "",
                        options,
                        horizontal=True,
                        index=options.index(st.session_state.chart_period)
                    )

                    if period != st.session_state.chart_period:
                        st.session_state.chart_period = period
                        st.rerun()

                    # GRAPH
                    fig = go.Figure()

                    fig.add_trace(go.Candlestick(
                        x=df["date"],
                        open=df["open"],
                        high=df["high"],
                        low=df["low"],
                        close=df["close"],
                        increasing_line_color="green",
                        decreasing_line_color="red"
                    ))

                    fig.add_trace(go.Scatter(
                        x=df["date"],
                        y=df["close"],
                        mode="lines",
                        line=dict(color="blue", width=2),
                        visible=False
                    ))

                    fig.add_trace(go.Scatter(
                        x=df["date"],
                        y=df["close"],
                        mode="lines",
                        fill="tozeroy",
                        line=dict(color="green", width=2),
                        fillcolor="rgba(0,255,0,0.1)",
                        visible=False
                    ))

                    fig.update_layout(
                        updatemenus=[
                            dict(
                                type="buttons",
                                direction="left",
                                x=0.98,
                                y=0.98,
                                xanchor="right",
                                yanchor="top",
                                buttons=[
                                    dict(label="🕯️", method="update", args=[{"visible": [True, False, False]}]),
                                    dict(label="📈", method="update", args=[{"visible": [False, True, False]}]),
                                    dict(label="🌊", method="update", args=[{"visible": [False, False, True]}]),
                                ]
                            )
                        ]
                    )

                    fig.update_layout(
                        template="plotly_white",
                        height=450,
                        margin=dict(l=10, r=10, t=10, b=10),
                        xaxis=dict(showgrid=False),
                        yaxis=dict(showgrid=False),
                    )

                    fig.update_layout(xaxis_rangeslider_visible=False)

                    st.plotly_chart(fig, width='stretch')

                    # =========================
                    #  OPEN / HIGH / LOW / CLOSE
                    # =========================
                    c1, c2, c3, c4 = st.columns(4)

                    def box(title, value):
                        return f"""
                        <div style="padding:10px; background:#f5f7fb; border-radius:10px;">
                        <b>{title}</b><br>{round(value,2)}
                        </div>
                        """

                    c1.markdown(box("Open", latest["open"]), unsafe_allow_html=True)
                    c2.markdown(box("High", high), unsafe_allow_html=True)
                    c3.markdown(box("Low", low), unsafe_allow_html=True)
                    c4.markdown(box("Close", latest["close"]), unsafe_allow_html=True)

                else:
                    st.warning("No price data")

            else:
                st.error("Price API failed")

        # =========================
        # RIGHT SIDE
        # =========================
        with right:

            if detail_status == 200:

                d = detail_data["data"]

                st.markdown(f"""
                <div style="padding:15px; border-radius:12px; border:1px solid #eee;">

                <h4>{d["company_name"]}</h4>
                <div style="color:gray;">{d["symbol"]} • {d["sector"]}</div>

                <hr>

                <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">

                <div><b>PE</b><br>{round(d.get("pe_ratio",0),2)}</div>
                <div><b>EPS</b><br>{round(d.get("eps",0),2)}</div>

                <div><b>Market Cap</b><br>₹{d.get("market_cap")}</div>
                <div><b>Revenue</b><br>₹{d.get("revenue")}</div>

                <div><b>Profit</b><br>₹{d.get("profit")}</div>
                <div><b>EBITDA</b><br>₹{d.get("ebitda")}</div>

                <div><b>ROE</b><br>{round(d.get("roe",0),2)}%</div>
                <div><b>ROA</b><br>{round(d.get("roa",0),2)}%</div>

                </div>

                </div>
                """, unsafe_allow_html=True)

            else:
                st.error("Company details failed")