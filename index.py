from flask import Flask, request, jsonify, render_template_string
import asyncio
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import binascii
import aiohttp
import requests
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
import random
import os
import urllib.parse
import jwt
import threading
import pickle

# استيراد ملفات Protobuf (يجب رفعها مع المشروع)
try:
    import like_pb2
    import like_count_pb2
    import uid_generator_pb2
except ImportError:
    print("WARNING: pb2 files are missing!")

# ==================== APP CONFIG ====================
# تعريف التطبيق في المستوى الأعلى ليتعرف عليه Vercel
app = Flask(__name__)

NORMAL_API_KEY = "SHAPPNO_04X"
INFO_API_URL = "https://info-ob55-shappnooo.vercel.app/info?uid={uid}"
JWT_API_URLS = [
    "https://shappno-deploy-hub.lovable.app/token",
    "https://shappno.vercel.app/token",
]

TOKEN_CACHE = {}
FAILED_TOKENS = {}
TOKEN_EXPIRY_HOURS = 6
KEY_LIMIT = 99
tracker = defaultdict(lambda: [0, 0])
liked_cache = defaultdict(set)
TRACKER_LOCK = threading.Lock()
BANGLADESH_OFFSET = 6 * 3600

# Vercel Read-Only Filesystem Fix (استخدام /tmp)
DATA_DIR = "/tmp" if os.environ.get("VERCEL") else os.getcwd()

# ==================== PERSISTENT STORAGE ====================
def save_data():
    try:
        with TRACKER_LOCK:
            with open(os.path.join(DATA_DIR, 'tracker.pkl'), "wb") as f: pickle.dump(dict(tracker), f)
            with open(os.path.join(DATA_DIR, 'liked.pkl'), "wb") as f: pickle.dump(dict(liked_cache), f)
            with open(os.path.join(DATA_DIR, 'tokens.pkl'), "wb") as f: pickle.dump(dict(TOKEN_CACHE), f)
            with open(os.path.join(DATA_DIR, 'failed.pkl'), "wb") as f: pickle.dump(dict(FAILED_TOKENS), f)
    except Exception as e: print(f"Error saving data: {e}")

def load_data():
    try:
        if os.path.exists(os.path.join(DATA_DIR, 'tracker.pkl')):
            with open(os.path.join(DATA_DIR, 'tracker.pkl'), "rb") as f: tracker.update(pickle.load(f))
        if os.path.exists(os.path.join(DATA_DIR, 'liked.pkl')):
            with open(os.path.join(DATA_DIR, 'liked.pkl'), "rb") as f: liked_cache.update(pickle.load(f))
        if os.path.exists(os.path.join(DATA_DIR, 'tokens.pkl')):
            with open(os.path.join(DATA_DIR, 'tokens.pkl'), "rb") as f: TOKEN_CACHE.update(pickle.load(f))
        if os.path.exists(os.path.join(DATA_DIR, 'failed.pkl')):
            with open(os.path.join(DATA_DIR, 'failed.pkl'), "rb") as f: FAILED_TOKENS.update(pickle.load(f))
    except Exception as e: print(f"Error loading data: {e}")

def get_bangladesh_midnight_timestamp():
    now_utc = datetime.utcnow()
    now_bd = now_utc + timedelta(hours=BANGLADESH_OFFSET)
    midnight_bd = datetime(now_bd.year, now_bd.month, now_bd.day)
    midnight_utc = midnight_bd - timedelta(hours=BANGLADESH_OFFSET)
    return midnight_utc.timestamp()

def load_accounts(server_name):
    filename = "account_ind.txt" if server_name in {"IND", "BR", "US", "SAC", "NA"} else "account_bd.txt"
    # البحث عن الملف في /tmp أولاً (لأن Vercel لا يسمح بالكتابة في المجلد الرئيسي)
    filepath = os.path.join(DATA_DIR, filename)
    
    if not os.path.exists(filepath):
        return []
    
    accounts = []
    seen_uids = set()
    with open(filepath, "r", encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or ':' not in line: continue
            uid, password = line.split(':', 1)
            uid, password = uid.strip(), password.strip()
            if uid and password and uid.isdigit() and uid not in seen_uids:
                seen_uids.add(uid)
                accounts.append({"uid": uid, "password": password})
    return accounts

# ==================== ASYNC FUNCTIONS ====================
async def generate_jwt_token_with_api(uid, password, api_url, retry_count=0):
    encoded_password = urllib.parse.quote(password, safe='')
    for attempt in range(retry_count, 3):
        try:
            url = f"{api_url}?uid={uid}&password={encoded_password}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        token = data.get('token') or data.get('access_token') or (data.get('data', {}).get('token') if isinstance(data.get('data'), dict) else None)
                        if token:
                            try:
                                payload = jwt.decode(token, options={"verify_signature": False})
                                exp = payload.get("exp", int(time.time()) + (TOKEN_EXPIRY_HOURS * 3600))
                            except: exp = int(time.time()) + (TOKEN_EXPIRY_HOURS * 3600)
                            
                            TOKEN_CACHE[uid] = {"token": token, "expires_at": datetime.utcfromtimestamp(exp), "api_used": api_url}
                            if uid in FAILED_TOKENS: del FAILED_TOKENS[uid]
                            return token
        except Exception: pass
        if attempt < 2: await asyncio.sleep(1)
    
    FAILED_TOKENS[uid] = {'timestamp': time.time(), 'api_used': api_url}
    return None

async def generate_tokens_batch_parallel(accounts, api_urls):
    if not accounts: return {}
    split_size = len(accounts) // len(api_urls)
    api1_accounts, api2_accounts = accounts[:split_size], accounts[split_size:]
    
    async def generate_for_api(accs, api_url):
        tokens = {}
        semaphore = asyncio.Semaphore(50) 
        async def task(acc):
            async with semaphore:
                # إصلاح الخطأ البرمجي هنا
                token = await generate_jwt_token_with_api(acc['uid'], acc['password'], api_url)
                if token: tokens[acc['uid']] = token
        await asyncio.gather(*(task(acc) for acc in accs), return_exceptions=True)
        return tokens

    results = await asyncio.gather(
        generate_for_api(api1_accounts, api_urls[0]),
        generate_for_api(api2_accounts, api_urls[1]),
        return_exceptions=True
    )
    all_tokens = {}
    for res in results:
        if isinstance(res, dict): all_tokens.update(res)
    return all_tokens

async def send_like(encrypted_uid, token, url):
    try:
        edata = bytes.fromhex(encrypted_uid)
        headers = {
            'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
            'Authorization': f"Bearer {token}", 'Content-Type': "application/x-www-form-urlencoded",
            'X-GA': "v1 1", 'ReleaseVersion': "OB55"
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=edata, headers=headers, timeout=5) as response:
                return response.status
    except: return 500

async def send_likes_batch(target_uid, encrypted_uid, accounts_with_tokens, url):
    if not accounts_with_tokens: return 0, 0
    semaphore = asyncio.Semaphore(50)
    successful, failed = 0, 0
    async def send_for_account(acc, token):
        nonlocal successful, failed
        status = await send_like(encrypted_uid, token, url)
        if status == 200:
            with TRACKER_LOCK: liked_cache[target_uid].add(acc['uid'])
            successful += 1
        else: failed += 1
    async def limited_send(acc, token):
        async with semaphore: await send_for_account(acc, token)
    await asyncio.gather(*(limited_send(acc, token) for acc, token in accounts_with_tokens), return_exceptions=True)
    return successful, failed

async def send_all_likes(target_uid, server_name, url, accounts, encrypted_uid):
    if not accounts: return {'success': 0, 'failed': 0, 'total': 0, 'already_liked': 0}
    with TRACKER_LOCK:
        already_liked = liked_cache.get(target_uid, set())
        fresh_accounts = [acc for acc in accounts if acc['uid'] not in already_liked]
    if not fresh_accounts:
        return {'success': 0, 'failed': 0, 'total': len(accounts), 'already_liked': len(already_liked), 'fresh_used': 0}
    
    random.shuffle(fresh_accounts)
    accounts_needing_tokens, accounts_with_valid_tokens = [], []
    for acc in fresh_accounts:
        if acc['uid'] in TOKEN_CACHE:
            cached = TOKEN_CACHE[acc['uid']]
            if (cached["expires_at"] - datetime.utcnow()).total_seconds() > 0:
                accounts_with_valid_tokens.append((acc, cached["token"]))
                continue
        accounts_needing_tokens.append(acc)
    
    tokens_dict = {}
    if accounts_needing_tokens:
        tokens_dict = await generate_tokens_batch_parallel(accounts_needing_tokens, JWT_API_URLS)
        save_data()
    
    all_accounts_with_tokens = list(accounts_with_valid_tokens)
    for acc in fresh_accounts:
        if acc['uid'] in tokens_dict and tokens_dict[acc['uid']]:
            all_accounts_with_tokens.append((acc, tokens_dict[acc['uid']]))
    
    if not all_accounts_with_tokens:
        return {'success': 0, 'failed': len(fresh_accounts), 'total': len(accounts)}
        
    successful, failed = await send_likes_batch(target_uid, encrypted_uid, all_accounts_with_tokens, url)
    save_data()
    return {'success': successful, 'failed': failed, 'total': len(accounts), 'already_liked': len(already_liked), 'fresh_used': len(all_accounts_with_tokens)}

# ==================== HELPERS ====================
def get_valid_token_sync(uid):
    if uid in TOKEN_CACHE:
        cached = TOKEN_CACHE[uid]
        if (cached["expires_at"] - datetime.utcnow()).total_seconds() > 0: return cached["token"]
    return None

def encrypt_message(plaintext):
    key, iv = b'Yg&tc%DEuh6%Zc^8', b'6oyZDr22E3ychjM%'
    return binascii.hexlify(AES.new(key, AES.MODE_CBC, iv).encrypt(pad(plaintext, AES.block_size))).decode('utf-8')

def enc(uid):
    message = uid_generator_pb2.uid_generator()
    message.krishna_ = int(uid)
    message.teamXdarks = 1
    return encrypt_message(message.SerializeToString())

def decode_protobuf(binary):
    try:
        items = like_count_pb2.Info()
        items.ParseFromString(binary)
        return items
    except: return None

def get_player_info(encrypted_uid, server_name, token):
    if server_name == "IND": url = "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
    elif server_name in {"BR", "US", "SAC", "NA"}: url = "https://client.us.freefiremobile.com/GetPlayerPersonalShow"
    else: url = "https://clientbp.ggpolarbear.com/GetPlayerPersonalShow"
    
    headers = {'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)", 'Authorization': f"Bearer {token}", 'Content-Type': "application/x-www-form-urlencoded", 'X-GA': "v1 1", 'ReleaseVersion': "OB55"}
    try: return decode_protobuf(requests.post(url, data=bytes.fromhex(encrypted_uid), headers=headers, verify=False, timeout=10).content)
    except: return None

# ==================== HTML TEMPLATE (مدمج لتسهيل الرفع) ====================
HTML_PAGE = """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shappno OB55 - لوحة التحكم</title>
    <link href="https://fonts.googleapis.com/css2?family=Cairo:wght@400;700&family=Fira+Code&display=swap" rel="stylesheet">
    <style>
        :root { --bg: #0f172a; --card: #1e293b; --text: #f8fafc; --primary: #3b82f6; --success: #10b981; --danger: #ef4444; }
        body { font-family: 'Cairo', sans-serif; background-color: var(--bg); color: var(--text); margin: 0; padding: 20px; }
        .container { max-width: 1000px; margin: 0 auto; display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .header { grid-column: span 2; text-align: center; margin-bottom: 20px; padding-bottom: 10px; border-bottom: 2px solid var(--primary); }
        .card { background: var(--card); padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
        .card h2 { margin-top: 0; color: var(--primary); font-size: 1.2rem; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        input, select, textarea { width: 100%; padding: 10px; border-radius: 6px; border: 1px solid #334155; background: #0f172a; color: white; box-sizing: border-box; font-family: inherit; }
        textarea { font-family: 'Fira Code', monospace; height: 200px; resize: vertical; direction: ltr; }
        button { width: 100%; padding: 12px; background: var(--primary); color: white; border: none; border-radius: 6px; font-weight: bold; cursor: pointer; transition: 0.3s; font-family: 'Cairo', sans-serif; font-size: 1rem; }
        button:hover { background: #2563eb; }
        button:disabled { background: #475569; cursor: not-allowed; }
        .terminal { grid-column: span 2; background: #000; color: #10b981; font-family: 'Fira Code', monospace; padding: 15px; border-radius: 8px; height: 200px; overflow-y: auto; direction: ltr; text-align: left; font-size: 0.9rem; border: 1px solid #334155; }
        .terminal p { margin: 5px 0; }
        .log-error { color: #ef4444; }
        .log-success { color: #10b981; }
        .log-info { color: #3b82f6; }
        @media (max-width: 768px) { .container { grid-template-columns: 1fr; } .header, .terminal { grid-column: span 1; } }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔥 Shappno OB55 - لوحة التحكم</h1>
            <p>إدارة الحسابات وإرسال الإعجابات</p>
        </div>
        <div class="card">
            <h2>💻 Terminal (إدارة الحسابات)</h2>
            <div class="form-group">
                <label>اختر السيرفر:</label>
                <select id="accountServer">
                    <option value="BD">BD (بنغلاديش / مينا)</option>
                    <option value="IND">IND (الهند)</option>
                    <option value="BR">BR (البرازيل)</option>
                </select>
            </div>
            <div class="form-group">
                <label>الحسابات (UID:Password):</label>
                <textarea id="accountsText" placeholder="123456789:password&#10;987654321:password"></textarea>
            </div>
            <button onclick="saveAccounts()">حفظ الحسابات</button>
        </div>
        <div class="card">
            <h2>❤️ إرسال إعجابات</h2>
            <div class="form-group">
                <label>UID اللاعب المستهدف:</label>
                <input type="text" id="targetUid" placeholder="مثال: 123456789">
            </div>
            <div class="form-group">
                <label>السيرفر:</label>
                <select id="serverName">
                    <option value="ME">ME (مينا)</option>
                    <option value="BD">BD (بنغلاديش)</option>
                    <option value="IND">IND (الهند)</option>
                    <option value="BR">BR (البرازيل)</option>
                    <option value="ID">ID (إندونيسيا)</option>
                </select>
            </div>
            <div class="form-group">
                <label>API Key:</label>
                <input type="text" id="apiKey" value="SHAPPNO_04X">
            </div>
            <button onclick="sendLikes()" id="sendBtn">🚀 إرسال الإعجابات</button>
        </div>
        <div class="terminal" id="terminal">
            <p class="log-info">[SYSTEM] الموقع جاهز...</p>
        </div>
    </div>
    <script>
        function log(msg, type = 'info') {
            const term = document.getElementById('terminal');
            const p = document.createElement('p');
            p.className = `log-${type}`;
            p.innerText = `[${new Date().toLocaleTimeString()}] ${msg}`;
            term.appendChild(p);
            term.scrollTop = term.scrollHeight;
        }
        async function saveAccounts() {
            const server = document.getElementById('accountServer').value;
            const accounts = document.getElementById('accountsText').value;
            log(`جاري حفظ الحسابات لسيرفر ${server}...`);
            try {
                const res = await fetch('/api/accounts', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ server: server, accounts: accounts })
                });
                const data = await res.json();
                if(res.ok) log(data.message, 'success');
                else log(data.error, 'error');
            } catch(e) { log('حدث خطأ أثناء الاتصال بالخادم', 'error'); }
        }
        async function sendLikes() {
            const uid = document.getElementById('targetUid').value;
            const server = document.getElementById('serverName').value;
            const key = document.getElementById('apiKey').value;
            const btn = document.getElementById('sendBtn');
            if(!uid) return alert('أدخل UID اللاعب!');
            btn.disabled = true;
            btn.innerText = 'جاري الإرسال...';
            log(`بدء عملية إرسال الإعجابات للاعب ${uid} على سيرفر ${server}...`);
            try {
                const res = await fetch('/api/send_likes', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ uid: uid, server_name: server, api_key: key })
                });
                const data = await res.json();
                if(res.ok) {
                    log(`✅ نجاح! اللاعب: ${data.PlayerNickname}`, 'success');
                    log(`اللايكات قبل: ${data.LikesbeforeCommand} | بعد: ${data.LikesafterCommand}`);
                    log(`الإعجابات المرسلة: +${data.LikesGivenByAPI}`, 'success');
                    log(`المتبقي لك اليوم: ${data.remains}`, 'info');
                } else {
                    log(`❌ خطأ: ${data.error}`, 'error');
                }
            } catch(e) { log('فشل الاتصال بالخادم.', 'error'); } finally {
                btn.disabled = false;
                btn.innerText = '🚀 إرسال الإعجابات';
            }
        }
        window.onload = async () => {
            try {
                const res = await fetch('/api/accounts?server=BD');
                const data = await res.json();
                if(data.accounts) document.getElementById('accountsText').value = data.accounts.join('\\n');
                log('تم تحميل الحسابات بنجاح');
            } catch(e) {}
        };
    </script>
</body>
</html>
"""

# ==================== ROUTES ====================
@app.route('/')
def index():
    return render_template_string(HTML_PAGE)

@app.route('/api/accounts', methods=['GET', 'POST'])
def manage_accounts():
    if request.method == 'GET':
        server = request.args.get('server', 'BD')
        accounts = load_accounts(server)
        return jsonify({"accounts": [f"{a['uid']}:{a['password']}" for a in accounts]})
    
    if request.method == 'POST':
        data = request.json
        server = data.get('server', 'BD')
        accounts_text = data.get('accounts', '')
        
        filename = "account_ind.txt" if server in {"IND", "BR", "US", "SAC", "NA"} else "account_bd.txt"
        filepath = os.path.join(DATA_DIR, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(accounts_text)
            return jsonify({"status": "success", "message": f"تم حفظ الحسابات في {filename}"})
        except Exception as e:
            return jsonify({"error": f"فشل الحفظ (قد يكون السيرفر محمي): {e}"}), 500

@app.route('/api/send_likes', methods=['POST'])
def api_send_likes():
    data = request.json
    uid = data.get('uid')
    server_name = data.get('server_name', '').upper()
    api_key = data.get('api_key')
    client_ip = request.remote_addr
    
    if api_key != NORMAL_API_KEY: return jsonify({"error": "مفتاح API غير صحيح"}), 403
    if not uid or not server_name: return jsonify({"error": "مطلوب UID والسيرفر"}), 400

    accounts = load_accounts(server_name)
    if not accounts: return jsonify({"error": f"لا توجد حسابات في ملف هذا السيرفر ({server_name})"}), 400
    
    bangladesh_midnight = get_bangladesh_midnight_timestamp()
    with TRACKER_LOCK:
        count, last_reset = tracker[client_ip]
        if last_reset < bangladesh_midnight:
            tracker[client_ip] = [0, bangladesh_midnight]
            count = 0
        if count >= KEY_LIMIT: return jsonify({"error": "تم الوصول للحد الأقصى اليومي (99)"}), 429

    check_token = None
    for account in accounts[:5]:
        check_token = get_valid_token_sync(account['uid'])
        if check_token: break
    
    if not check_token:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            for account in accounts[:5]:
                check_token = loop.run_until_complete(generate_jwt_token_with_api(account['uid'], account['password'], JWT_API_URLS[0]))
                if check_token: break
            loop.close()
        except Exception as e:
            pass
    
    if not check_token: return jsonify({"error": "فشل توليد التوكن، قد تكون APIs معطلة"}), 500

    encrypted_uid = enc(uid)
    before = get_player_info(encrypted_uid, server_name, check_token)
    if before is None: return jsonify({"error": "UID غير صحيح أو السيرفر خطأ"}), 400

    try: before_like = int(json.loads(MessageToJson(before))['AccountInfo'].get('Likes', 0))
    except: return jsonify({"error": "فشل قراءة البيانات"}), 400

    if server_name == "IND": like_url = "https://client.ind.freefiremobile.com/LikeProfile"
    elif server_name in {"BR", "US", "SAC", "NA"}: like_url = "https://client.us.freefiremobile.com/LikeProfile"
    else: like_url = "https://clientbp.ggpolarbear.com/LikeProfile"

    # إصلاح مشكلة asyncio.run() في Vercel
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import nest_asyncio
        nest_asyncio.apply()
        result = asyncio.run(send_all_likes(uid, server_name, like_url, accounts, encrypted_uid))
    else:
        result = asyncio.run(send_all_likes(uid, server_name, like_url, accounts, encrypted_uid))

    after = get_player_info(encrypted_uid, server_name, check_token)
    if after is None: return jsonify({"error": "لم نتمكن من تأكيد الإعجابات"}), 500

    try:
        after_data = json.loads(MessageToJson(after))
        after_like = int(after_data['AccountInfo']['Likes'])
        player_name = str(after_data['AccountInfo']['PlayerNickname'])
        like_given = after_like - before_like
        
        if like_given > 0:
            with TRACKER_LOCK:
                tracker[client_ip][0] += 1
                count = tracker[client_ip][0]
        
        remains = KEY_LIMIT - count
        save_data()

        return jsonify({
            "success": True, "LikesGivenByAPI": like_given, "LikesafterCommand": after_like,
            "LikesbeforeCommand": before_like, "PlayerNickname": player_name, "UID": uid,
            "remains": f"({remains}/{KEY_LIMIT})", "details": result
        })
    except Exception as e: return jsonify({"error": str(e)}), 500

# ==================== INIT ====================
load_data()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
