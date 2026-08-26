from aiohttp import web
import aiohttp_cors
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import os, hashlib, uuid, asyncio

DB_CONFIG = {
    "user": "postgres.ntshrlzpfyvfnkkxckfs",
    "password": "Gourav@123#",
    "host": "aws-1-ap-south-1.pooler.supabase.com",
    "port": 6543,
    "dbname": "postgres",
    "sslmode": "require"
}

ADMIN_EMAIL = "abc@gmail.com"
db_connection = None


def get_db():
    global db_connection
    try:
        if db_connection is None or db_connection.closed:
            db_connection = psycopg2.connect(**DB_CONFIG)
        else:
            # Test connection is alive
            db_connection.cursor().execute("SELECT 1")
        return db_connection
    except Exception:
        try:
            db_connection = psycopg2.connect(**DB_CONFIG)
            return db_connection
        except Exception as e:
            print(f"DB error: {e}")
            return None


def get_ip(request):
    for h in ('X-Forwarded-For', 'X-Real-IP'):
        v = request.headers.get(h)
        if v: return v.split(',')[0].strip()
    p = request.transport.get_extra_info('peername')
    return p[0] if p else 'unknown'


def hash_ip(ip): return hashlib.sha256(ip.encode()).hexdigest()

def extract_vid(url):
    p = urlparse(url)
    if 'youtube.com' in p.netloc: return parse_qs(p.query).get('v', [None])[0]
    if 'youtu.be' in p.netloc: return p.path[1:]
    return None

def embed_url(url):
    vid = extract_vid(url)
    return f"https://www.youtube.com/embed/{vid}" if vid else url

def thumb_url(url):
    vid = extract_vid(url)
    return f"https://img.youtube.com/vi/{vid}/mqdefault.jpg" if vid else ""


# ── KEEP ALIVE ──────────────────────────────────
async def keep_alive_task(app):
    """Ping DB every 4 minutes to prevent cold starts on Render free tier"""
    while True:
        try:
            await asyncio.sleep(240)
            conn = get_db()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                print(f"[{datetime.utcnow().isoformat()}] ✅ Keep-alive ping OK")
        except Exception as e:
            print(f"Keep-alive error: {e}")


async def keep_alive_endpoint(request):
    """GET /ping — external uptime monitors can hit this"""
    try:
        conn = get_db()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            count = cur.fetchone()[0]
            cur.close()
            return web.json_response({'status': 'ok', 'db': 'connected', 'users': count, 'ts': datetime.utcnow().isoformat()})
        return web.json_response({'status': 'degraded', 'db': 'disconnected'}, status=503)
    except Exception as e:
        return web.json_response({'status': 'error', 'detail': str(e)}, status=500)


# ── DB INIT ──────────────────────────────────────
async def init_db(app):
    global db_connection
    try:
        db_connection = psycopg2.connect(**DB_CONFIG)
        c = db_connection.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL, email TEXT UNIQUE NOT NULL,
            is_admin BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW())''')

        c.execute('''CREATE TABLE IF NOT EXISTS videos (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            url TEXT NOT NULL, embed_url TEXT, thumbnail TEXT,
            added_by TEXT NOT NULL,
            user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            library_name TEXT,
            is_public BOOLEAN DEFAULT FALSE,
            views INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW())''')

        c.execute('''CREATE TABLE IF NOT EXISTS video_views (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
            ip_hash TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(video_id, ip_hash))''')

        c.execute('''CREATE TABLE IF NOT EXISTS watch_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
            seconds_watched INTEGER DEFAULT 0,
            session_start TIMESTAMPTZ DEFAULT NOW(),
            watched_at TIMESTAMPTZ DEFAULT NOW())''')

        c.execute('''CREATE TABLE IF NOT EXISTS login_sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            logged_at TIMESTAMPTZ DEFAULT NOW(),
            ip_hash TEXT)''')

        c.execute('''CREATE TABLE IF NOT EXISTS comments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            user_name TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW())''')

        # Chat messages table
        c.execute('''CREATE TABLE IF NOT EXISTS chat_messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            from_user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            to_user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            message TEXT NOT NULL,
            is_read BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW())''')

        for sql in [
            'CREATE INDEX IF NOT EXISTS idx_vid_created ON videos(created_at DESC)',
            'CREATE INDEX IF NOT EXISTS idx_wh_user ON watch_history(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_wh_session ON watch_history(session_start)',
            'CREATE INDEX IF NOT EXISTS idx_ls_user ON login_sessions(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_comments_video ON comments(video_id)',
            'CREATE INDEX IF NOT EXISTS idx_chat_from ON chat_messages(from_user_id)',
            'CREATE INDEX IF NOT EXISTS idx_chat_to ON chat_messages(to_user_id)',
            'CREATE INDEX IF NOT EXISTS idx_chat_created ON chat_messages(created_at DESC)',
        ]:
            try: c.execute(sql)
            except Exception: db_connection.rollback()

        for sql in [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE",
            "ALTER TABLE videos ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE SET NULL",
            "ALTER TABLE videos ADD COLUMN IF NOT EXISTS library_name TEXT",
            "ALTER TABLE videos ADD COLUMN IF NOT EXISTS is_public BOOLEAN DEFAULT FALSE",
            "ALTER TABLE watch_history ADD COLUMN IF NOT EXISTS session_start TIMESTAMPTZ DEFAULT NOW()",
        ]:
            try: c.execute(sql)
            except Exception: db_connection.rollback()

        # Make first user with admin email an admin
        c.execute("UPDATE users SET is_admin=TRUE WHERE email=%s", (ADMIN_EMAIL.lower(),))

        db_connection.commit(); c.close()
        print("✅ DB ready")

        # Start keep-alive background task
        asyncio.create_task(keep_alive_task(app))

    except Exception as e:
        print(f"⚠️ DB init failed: {e}")
        db_connection = None


async def close_db(app):
    global db_connection
    if db_connection and not db_connection.closed:
        db_connection.close()


# ── USERS ────────────────────────────────────────
async def create_or_get_user(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        name = d.get('name', '').strip()
        email = d.get('email', '').strip().lower()
        if not name or not email: return web.json_response({'error': 'name+email required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT * FROM users WHERE email=%s', (email,))
        row = cur.fetchone()
        if row:
            cur.close()
            return web.json_response({'success': True, 'is_new': False, 'user': {
                'id': str(row['id']), 'name': row['name'], 'email': row['email'],
                'is_admin': row['is_admin'], 'created_at': row['created_at'].isoformat()}})
        is_admin = email == ADMIN_EMAIL.lower()
        cur.execute('INSERT INTO users(name,email,is_admin) VALUES(%s,%s,%s) RETURNING *', (name, email, is_admin))
        row = cur.fetchone(); conn.commit(); cur.close()
        return web.json_response({'success': True, 'is_new': True, 'user': {
            'id': str(row['id']), 'name': row['name'], 'email': row['email'],
            'is_admin': row['is_admin'], 'created_at': row['created_at'].isoformat()}})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


async def get_users(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        requester_id = request.rel_url.query.get('requester_id')
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if requester is admin
        is_admin = False
        if requester_id:
            cur.execute('SELECT is_admin FROM users WHERE id=%s', (requester_id,))
            u = cur.fetchone()
            is_admin = u and u['is_admin']

        cur.execute('''SELECT u.id,u.name,u.email,u.is_admin,u.created_at,
            COUNT(v.id) as video_count,
            COALESCE(SUM(wh.seconds_watched),0) as total_watch_seconds
            FROM users u
            LEFT JOIN videos v ON v.user_id=u.id
            LEFT JOIN watch_history wh ON wh.user_id=u.id
            GROUP BY u.id ORDER BY u.created_at DESC''')
        rows = cur.fetchall(); cur.close()

        def fmt(r):
            d = {'id': str(r['id']), 'name': r['name'], 'is_admin': r['is_admin'],
                 'video_count': int(r['video_count']),
                 'total_watch_seconds': int(r['total_watch_seconds']),
                 'created_at': r['created_at'].isoformat()}
            if is_admin:
                d['email'] = r['email']  # Only admin sees emails
            else:
                d['email'] = r['email'][:2] + '***@***' + r['email'].split('@')[-1][-3:]
            return d
        return web.json_response({'success': True, 'users': [fmt(r) for r in rows], 'viewer_is_admin': is_admin})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def update_user(request):
    """Admin: update user name/email/admin status"""
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        admin_id = d.get('admin_id')
        target_id = d.get('user_id')
        new_name = d.get('name')
        new_email = d.get('email')
        new_is_admin = d.get('is_admin')

        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT is_admin FROM users WHERE id=%s', (admin_id,))
        adm = cur.fetchone()
        if not adm or not adm['is_admin']:
            cur.close(); return web.json_response({'error': 'Admin only'}, status=403)

        updates, params = [], []
        if new_name:  updates.append('name=%s');     params.append(new_name.strip())
        if new_email: updates.append('email=%s');    params.append(new_email.strip().lower())
        if new_is_admin is not None: updates.append('is_admin=%s'); params.append(bool(new_is_admin))
        if not updates:
            cur.close(); return web.json_response({'error': 'Nothing to update'}, status=400)

        params.append(target_id)
        cur.execute(f"UPDATE users SET {','.join(updates)} WHERE id=%s RETURNING *", params)
        row = cur.fetchone(); conn.commit(); cur.close()
        if not row: return web.json_response({'error': 'User not found'}, status=404)
        return web.json_response({'success': True, 'user': {
            'id': str(row['id']), 'name': row['name'], 'email': row['email'],
            'is_admin': row['is_admin']}})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


async def delete_user(request):
    """Admin: delete a user and all their data"""
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        admin_id = d.get('admin_id')
        target_id = d.get('user_id')
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT is_admin FROM users WHERE id=%s', (admin_id,))
        adm = cur.fetchone()
        if not adm or not adm['is_admin']:
            cur.close(); return web.json_response({'error': 'Admin only'}, status=403)
        cur.execute('DELETE FROM users WHERE id=%s AND id!=%s', (target_id, admin_id))
        n = cur.rowcount; conn.commit(); cur.close()
        if n == 0: return web.json_response({'error': 'Cannot delete admin or user not found'}, status=400)
        return web.json_response({'success': True})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


# ── SESSIONS ─────────────────────────────────────
async def record_login(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        uid = d.get('user_id')
        if not uid: return web.json_response({'error': 'user_id required'}, status=400)
        ip_hash = hash_ip(get_ip(request))
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('INSERT INTO login_sessions(user_id, ip_hash) VALUES(%s,%s) RETURNING id', (str(uuid.UUID(uid)), ip_hash))
        row = cur.fetchone(); conn.commit(); cur.close()
        return web.json_response({'success': True, 'session_id': str(row['id'])})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


async def get_sessions(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        uid = request.rel_url.query.get('user_id')
        cur = conn.cursor(cursor_factory=RealDictCursor)
        uf = "WHERE u.id=%s" if uid else ""
        p = [uid] if uid else []
        cur.execute(f'''SELECT u.id,u.name,u.email,COUNT(ls.id) as login_count,
            MAX(ls.logged_at) as last_login,MIN(ls.logged_at) as first_login
            FROM users u LEFT JOIN login_sessions ls ON ls.user_id=u.id {uf}
            GROUP BY u.id ORDER BY login_count DESC''', p)
        stats = cur.fetchall()
        cur.execute(f'''SELECT ls.logged_at,u.name as user_name,u.id as user_id
            FROM login_sessions ls JOIN users u ON u.id=ls.user_id {uf}
            ORDER BY ls.logged_at DESC LIMIT 100''', p)
        timeline = cur.fetchall(); cur.close()
        return web.json_response({'success': True,
            'stats': [{'user_id': str(r['id']), 'name': r['name'], 'email': r['email'],
                'login_count': int(r['login_count']),
                'last_login': r['last_login'].isoformat() if r['last_login'] else None,
                'first_login': r['first_login'].isoformat() if r['first_login'] else None} for r in stats],
            'timeline': [{'user_name': r['user_name'], 'user_id': str(r['user_id']),
                'logged_at': r['logged_at'].isoformat()} for r in timeline]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


# ── VIDEOS ───────────────────────────────────────
async def add_video(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        url = d.get('url', '').strip()
        email = d.get('email', '').strip().lower()
        if not url or not email: return web.json_response({'error': 'url+email required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT id,name FROM users WHERE email=%s', (email,))
        u = cur.fetchone()
        uid = str(u['id']) if u else None
        dname = u['name'] if u else email
        lname = u['name'] if u else None
        cur.execute('''INSERT INTO videos(url,embed_url,thumbnail,added_by,user_id,library_name,is_public,views)
            VALUES(%s,%s,%s,%s,%s,%s,FALSE,0) RETURNING *''',
            (url, embed_url(url), thumb_url(url), dname, uid, lname))
        row = cur.fetchone(); conn.commit(); cur.close()
        return web.json_response({'success': True, 'video': {
            '_id': str(row['id']), 'url': row['url'], 'embed_url': row['embed_url'],
            'thumbnail': row['thumbnail'], 'added_by': row['added_by'],
            'user_id': str(row['user_id']) if row['user_id'] else None,
            'is_public': row['is_public'], 'views': row['views'],
            'created_at': row['created_at'].isoformat()}})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


async def get_videos(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        uid = request.rel_url.query.get('user_id')
        public_only = request.rel_url.query.get('public_only') == 'true'
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if uid: cur.execute('SELECT * FROM videos WHERE user_id=%s ORDER BY created_at DESC', (uid,))
        elif public_only: cur.execute('SELECT * FROM videos WHERE is_public=TRUE ORDER BY created_at DESC')
        else: cur.execute('SELECT * FROM videos ORDER BY created_at DESC')
        rows = cur.fetchall(); cur.close()
        return web.json_response({'success': True, 'videos': [{
            '_id': str(r['id']), 'url': r['url'], 'embed_url': r['embed_url'],
            'thumbnail': r['thumbnail'], 'added_by': r['added_by'],
            'user_id': str(r['user_id']) if r['user_id'] else None,
            'is_public': r['is_public'], 'views': r['views'],
            'created_at': r['created_at'].isoformat()} for r in rows]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def toggle_visibility(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        vid = d.get('video_id'); uid = d.get('user_id')
        if not vid or not uid: return web.json_response({'error': 'video_id+user_id required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT user_id, is_public FROM videos WHERE id=%s', (str(uuid.UUID(vid)),))
        row = cur.fetchone()
        if not row: cur.close(); return web.json_response({'error': 'Not found'}, status=404)
        # Allow admin or owner
        cur.execute('SELECT is_admin FROM users WHERE id=%s', (str(uuid.UUID(uid)),))
        u = cur.fetchone()
        if str(row['user_id']) != str(uuid.UUID(uid)) and not (u and u['is_admin']):
            cur.close(); return web.json_response({'error': 'Not authorized'}, status=403)
        new_status = not row['is_public']
        cur.execute('UPDATE videos SET is_public=%s WHERE id=%s', (new_status, str(uuid.UUID(vid))))
        conn.commit(); cur.close()
        return web.json_response({'success': True, 'is_public': new_status})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


async def delete_video(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        vid = d.get('video_id'); email = d.get('email', '').strip().lower()
        uid = d.get('user_id')
        if not vid: return web.json_response({'error': 'video_id required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Check if user is admin or owner
        is_admin = email == ADMIN_EMAIL.lower()
        if not is_admin and uid:
            cur.execute('SELECT is_admin, id FROM users WHERE id=%s', (str(uuid.UUID(uid)),))
            u = cur.fetchone()
            is_admin = u and u['is_admin']
        if not is_admin:
            # Check if owner
            cur.execute('SELECT user_id FROM videos WHERE id=%s', (str(uuid.UUID(vid)),))
            v = cur.fetchone()
            if not v or (uid and str(v['user_id']) != str(uuid.UUID(uid))):
                cur.close(); return web.json_response({'error': 'Unauthorized'}, status=403)
        cur.execute('DELETE FROM videos WHERE id=%s', (str(uuid.UUID(vid)),))
        n = cur.rowcount; conn.commit(); cur.close()
        if n == 0: return web.json_response({'error': 'Not found'}, status=404)
        return web.json_response({'success': True})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


# ── COMMENTS ─────────────────────────────────────
async def add_comment(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        video_id = d.get('video_id'); user_id = d.get('user_id'); text = d.get('text', '').strip()
        if not all([video_id, user_id, text]): return web.json_response({'error': 'Missing fields'}, status=400)
        if len(text) > 1000: return web.json_response({'error': 'Too long'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT name FROM users WHERE id=%s', (str(uuid.UUID(user_id)),))
        u = cur.fetchone()
        if not u: cur.close(); return web.json_response({'error': 'User not found'}, status=404)
        cur.execute('INSERT INTO comments(video_id,user_id,user_name,text) VALUES(%s,%s,%s,%s) RETURNING *',
            (str(uuid.UUID(video_id)), str(uuid.UUID(user_id)), u['name'], text))
        row = cur.fetchone(); conn.commit(); cur.close()
        return web.json_response({'success': True, 'comment': {
            'id': str(row['id']), 'user_id': str(row['user_id']),
            'user_name': row['user_name'], 'text': row['text'],
            'created_at': row['created_at'].isoformat()}})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


async def get_comments(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        video_id = request.rel_url.query.get('video_id')
        if not video_id: return web.json_response({'error': 'video_id required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT * FROM comments WHERE video_id=%s ORDER BY created_at ASC', (str(uuid.UUID(video_id)),))
        rows = cur.fetchall(); cur.close()
        return web.json_response({'success': True, 'comments': [{
            'id': str(r['id']), 'user_id': str(r['user_id']),
            'user_name': r['user_name'], 'text': r['text'],
            'created_at': r['created_at'].isoformat()} for r in rows]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def delete_comment(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        comment_id = d.get('comment_id'); user_id = d.get('user_id')
        if not comment_id: return web.json_response({'error': 'comment_id required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT user_id FROM comments WHERE id=%s', (str(uuid.UUID(comment_id)),))
        row = cur.fetchone()
        if not row: cur.close(); return web.json_response({'error': 'Not found'}, status=404)
        # Check admin or owner
        is_admin_del = False
        if user_id:
            cur.execute('SELECT is_admin FROM users WHERE id=%s', (str(uuid.UUID(user_id)),))
            u = cur.fetchone()
            is_admin_del = u and u['is_admin']
        is_owner = user_id and str(row['user_id']) == str(uuid.UUID(user_id))
        if not is_admin_del and not is_owner:
            cur.close(); return web.json_response({'error': 'Unauthorized'}, status=403)
        cur.execute('DELETE FROM comments WHERE id=%s', (str(uuid.UUID(comment_id)),))
        conn.commit(); cur.close()
        return web.json_response({'success': True})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


# ── CHAT ─────────────────────────────────────────
async def send_message(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        from_id = d.get('from_user_id'); to_id = d.get('to_user_id'); msg = d.get('message', '').strip()
        if not all([from_id, to_id, msg]): return web.json_response({'error': 'Missing fields'}, status=400)
        if len(msg) > 2000: return web.json_response({'error': 'Too long'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('''INSERT INTO chat_messages(from_user_id,to_user_id,message)
            VALUES(%s,%s,%s) RETURNING *''', (str(uuid.UUID(from_id)), str(uuid.UUID(to_id)), msg))
        row = cur.fetchone(); conn.commit()
        # Fetch sender name
        cur.execute('SELECT name FROM users WHERE id=%s', (from_id,))
        sender = cur.fetchone(); cur.close()
        return web.json_response({'success': True, 'message': {
            'id': str(row['id']), 'from_user_id': str(row['from_user_id']),
            'to_user_id': str(row['to_user_id']), 'message': row['message'],
            'from_name': sender['name'] if sender else '?',
            'is_read': row['is_read'], 'created_at': row['created_at'].isoformat()}})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


async def get_messages(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        uid = request.rel_url.query.get('user_id')
        other_id = request.rel_url.query.get('other_id')
        if not uid: return web.json_response({'error': 'user_id required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if other_id:
            cur.execute('''SELECT m.*,
                uf.name as from_name, ut.name as to_name
                FROM chat_messages m
                JOIN users uf ON uf.id=m.from_user_id
                JOIN users ut ON ut.id=m.to_user_id
                WHERE (m.from_user_id=%s AND m.to_user_id=%s)
                   OR (m.from_user_id=%s AND m.to_user_id=%s)
                ORDER BY m.created_at ASC LIMIT 200''', (uid, other_id, other_id, uid))
            msgs = cur.fetchall()
            # Mark as read
            cur.execute('''UPDATE chat_messages SET is_read=TRUE
                WHERE to_user_id=%s AND from_user_id=%s AND is_read=FALSE''', (uid, other_id))
            conn.commit()
        else:
            # Get conversation list: latest message per contact
            cur.execute('''SELECT DISTINCT ON (partner_id) *
                FROM (
                    SELECT m.*, uf.name as from_name, ut.name as to_name,
                        CASE WHEN m.from_user_id=%s THEN m.to_user_id ELSE m.from_user_id END as partner_id,
                        CASE WHEN m.from_user_id=%s THEN ut.name ELSE uf.name END as partner_name
                    FROM chat_messages m
                    JOIN users uf ON uf.id=m.from_user_id
                    JOIN users ut ON ut.id=m.to_user_id
                    WHERE m.from_user_id=%s OR m.to_user_id=%s
                ) sub ORDER BY partner_id, created_at DESC''', (uid, uid, uid, uid))
            msgs = cur.fetchall()
        # Unread count
        cur.execute('SELECT COUNT(*) as cnt FROM chat_messages WHERE to_user_id=%s AND is_read=FALSE', (uid,))
        unread = cur.fetchone()['cnt']; cur.close()
        return web.json_response({'success': True, 'unread_count': int(unread),
            'messages': [{'id': str(m['id']),
                'from_user_id': str(m['from_user_id']),
                'to_user_id': str(m['to_user_id']),
                'from_name': m['from_name'], 'to_name': m['to_name'],
                'message': m['message'], 'is_read': m['is_read'],
                'partner_id': str(m.get('partner_id', m['from_user_id'])),
                'partner_name': m.get('partner_name', m['from_name']),
                'created_at': m['created_at'].isoformat()} for m in msgs]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


# ── WATCH HISTORY & ANALYTICS ────────────────────
async def record_watch(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        uid = d.get('user_id'); vid = d.get('video_id'); secs = int(d.get('seconds_watched', 0))
        ss_iso = d.get('session_start')
        if not uid or not vid: return web.json_response({'error': 'user_id+video_id required'}, status=400)
        try: ss = datetime.fromisoformat(ss_iso.replace('Z', '+00:00'))
        except Exception: ss = datetime.utcnow()
        cur = conn.cursor()
        cur.execute('INSERT INTO watch_history(user_id,video_id,seconds_watched,session_start,watched_at) VALUES(%s,%s,%s,%s,NOW())',
            (str(uuid.UUID(uid)), str(uuid.UUID(vid)), secs, ss))
        cur.execute('UPDATE videos SET views=views+1 WHERE id=%s', (str(uuid.UUID(vid)),))
        conn.commit(); cur.close()
        return web.json_response({'success': True})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


async def get_history(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        uid = request.rel_url.query.get('user_id')
        cur = conn.cursor(cursor_factory=RealDictCursor)
        uf = "WHERE u.id=%s" if uid else ""
        p = [uid] if uid else []
        cur.execute(f'''SELECT u.id,u.name,u.email,
            COALESCE(SUM(wh.seconds_watched),0) as total_seconds,
            COUNT(DISTINCT wh.video_id) as videos_watched,
            MAX(wh.watched_at) as last_watched
            FROM users u LEFT JOIN watch_history wh ON wh.user_id=u.id {uf}
            GROUP BY u.id ORDER BY total_seconds DESC''', p)
        us = cur.fetchall()
        uf2 = "WHERE wh.user_id=%s" if uid else ""
        cur.execute(f'''SELECT wh.user_id,u.name as user_name,wh.video_id,
            v.url,SUM(wh.seconds_watched) as total_seconds,
            COUNT(*) as session_count,MAX(wh.watched_at) as last_watched
            FROM watch_history wh JOIN users u ON u.id=wh.user_id JOIN videos v ON v.id=wh.video_id
            {uf2} GROUP BY wh.user_id,u.name,wh.video_id,v.url ORDER BY last_watched DESC''', p)
        vd = cur.fetchall(); cur.close()
        return web.json_response({'success': True,
            'user_stats': [{'user_id': str(r['id']), 'name': r['name'], 'email': r['email'],
                'total_seconds': int(r['total_seconds']), 'videos_watched': int(r['videos_watched']),
                'last_watched': r['last_watched'].isoformat() if r['last_watched'] else None} for r in us],
            'video_details': [{'user_id': str(r['user_id']), 'user_name': r['user_name'],
                'video_id': str(r['video_id']), 'url': r['url'],
                'total_seconds': int(r['total_seconds']), 'session_count': int(r['session_count']),
                'last_watched': r['last_watched'].isoformat()} for r in vd]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def get_analytics(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        uid = request.rel_url.query.get('user_id')
        days = int(request.rel_url.query.get('days', 30))
        cur = conn.cursor(cursor_factory=RealDictCursor)
        uf = "AND wh.user_id=%s" if uid else ""
        p = [uid] if uid else []
        cur.execute(f'''SELECT u.name as user_name,u.id::text as user_id,
            DATE(wh.session_start AT TIME ZONE 'UTC') as day,
            SUM(wh.seconds_watched) as total_seconds
            FROM watch_history wh JOIN users u ON u.id=wh.user_id
            WHERE wh.session_start >= NOW() - INTERVAL '{days} days' {uf}
            GROUP BY u.id,u.name,DATE(wh.session_start AT TIME ZONE 'UTC') ORDER BY day ASC''', p)
        day_rows = cur.fetchall()
        cur.execute(f'''SELECT u.name as user_name,u.id::text as user_id,
            EXTRACT(HOUR FROM wh.session_start AT TIME ZONE 'UTC')::int as hour,
            SUM(wh.seconds_watched) as total_seconds
            FROM watch_history wh JOIN users u ON u.id=wh.user_id
            WHERE wh.session_start >= NOW() - INTERVAL '{days} days' {uf}
            GROUP BY u.id,u.name,EXTRACT(HOUR FROM wh.session_start AT TIME ZONE 'UTC') ORDER BY hour ASC''', p)
        hour_rows = cur.fetchall()
        cur.execute(f'''SELECT u.name as user_name,u.id::text as user_id,
            wh.session_start,wh.seconds_watched,v.url,v.thumbnail
            FROM watch_history wh JOIN users u ON u.id=wh.user_id JOIN videos v ON v.id=wh.video_id
            WHERE wh.session_start >= NOW() - INTERVAL '{days} days' {uf}
            ORDER BY wh.session_start DESC LIMIT 200''', p)
        sessions = cur.fetchall()
        cur.execute(f'''SELECT v.id::text as video_id,v.url,v.thumbnail,
            COUNT(*) as play_count,SUM(wh.seconds_watched) as total_seconds,u.name as user_name
            FROM watch_history wh JOIN videos v ON v.id=wh.video_id JOIN users u ON u.id=wh.user_id
            WHERE wh.session_start >= NOW() - INTERVAL '{days} days' {uf}
            GROUP BY v.id,v.url,v.thumbnail,u.name ORDER BY play_count DESC LIMIT 20''', p)
        top = cur.fetchall(); cur.close()
        return web.json_response({'success': True,
            'day_data': [{'user_name': r['user_name'], 'user_id': r['user_id'],
                'day': r['day'].isoformat(), 'total_seconds': int(r['total_seconds'])} for r in day_rows],
            'hour_data': [{'user_name': r['user_name'], 'user_id': r['user_id'],
                'hour': r['hour'], 'total_seconds': int(r['total_seconds'])} for r in hour_rows],
            'sessions': [{'user_name': r['user_name'], 'user_id': r['user_id'],
                'session_start': r['session_start'].isoformat(),
                'seconds_watched': r['seconds_watched'], 'url': r['url']} for r in sessions],
            'top_videos': [{'video_id': r['video_id'], 'url': r['url'],
                'play_count': int(r['play_count']), 'total_seconds': int(r['total_seconds']),
                'user_name': r['user_name']} for r in top]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


# ── ADMIN PURGE ──────────────────────────────────
async def purge_all(request):
    conn = get_db()
    if not conn: return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        email = d.get('email', '').strip().lower()
        confirm = d.get('confirm', '')
        if email != ADMIN_EMAIL.lower(): return web.json_response({'error': 'Admin only'}, status=403)
        if confirm != 'DELETE_ALL': return web.json_response({'error': 'Must confirm DELETE_ALL'}, status=400)
        cur = conn.cursor()
        for t in ['chat_messages', 'comments', 'watch_history', 'video_views', 'login_sessions', 'videos', 'users']:
            cur.execute(f'TRUNCATE TABLE {t} CASCADE')
        conn.commit(); cur.close()
        return web.json_response({'success': True, 'message': 'All data purged.'})
    except Exception as e:
        conn.rollback(); return web.json_response({'error': str(e)}, status=500)


async def index(request):
    try:
        with open(os.path.join(os.path.dirname(__file__), 'index.html'), 'r', encoding='utf-8') as f:
            return web.Response(text=f.read(), content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='index.html not found', status=404)


def create_app():
    app = web.Application()
    cors = aiohttp_cors.setup(app, defaults={"*": aiohttp_cors.ResourceOptions(
        allow_credentials=True, expose_headers="*", allow_headers="*", allow_methods="*")})

    app.router.add_get('/', index)
    app.router.add_get('/ping', keep_alive_endpoint)

    app.router.add_post('/api/users/create', create_or_get_user)
    app.router.add_get('/api/users', get_users)
    app.router.add_post('/api/users/update', update_user)
    app.router.add_post('/api/users/delete', delete_user)

    app.router.add_post('/api/sessions/login', record_login)
    app.router.add_get('/api/sessions', get_sessions)

    app.router.add_post('/api/videos/add', add_video)
    app.router.add_get('/api/videos', get_videos)
    app.router.add_post('/api/videos/delete', delete_video)
    app.router.add_post('/api/videos/toggle', toggle_visibility)

    app.router.add_post('/api/comments/add', add_comment)
    app.router.add_get('/api/comments', get_comments)
    app.router.add_post('/api/comments/delete', delete_comment)

    app.router.add_post('/api/chat/send', send_message)
    app.router.add_get('/api/chat/messages', get_messages)

    app.router.add_post('/api/history/record', record_watch)
    app.router.add_get('/api/history', get_history)
    app.router.add_get('/api/analytics', get_analytics)

    app.router.add_post('/api/admin/purge', purge_all)

    for route in list(app.router.routes()):
        cors.add(route)
    app.on_startup.append(init_db)
    app.on_cleanup.append(close_db)
    return app


if __name__ == '__main__':
    print("🚀 VIDLIB v5 — Chat · Admin · Keep-alive · 5-page UI")
    web.run_app(create_app(), host='0.0.0.0', port=9000)
