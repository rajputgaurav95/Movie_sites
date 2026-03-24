from aiohttp import web
import aiohttp_cors
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import os
import hashlib
import uuid

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


def get_db_connection():
    global db_connection
    try:
        if db_connection is None or db_connection.closed:
            db_connection = psycopg2.connect(**DB_CONFIG)
        return db_connection
    except Exception as e:
        print(f"DB error: {e}")
        return None


def get_client_ip(request):
    for h in ('X-Forwarded-For', 'X-Real-IP'):
        v = request.headers.get(h)
        if v:
            return v.split(',')[0].strip()
    p = request.transport.get_extra_info('peername')
    return p[0] if p else 'unknown'


def hash_ip(ip): return hashlib.sha256(ip.encode()).hexdigest()


def extract_video_id(url):
    p = urlparse(url)
    if 'youtube.com' in p.netloc:
        return parse_qs(p.query).get('v', [None])[0]
    if 'youtu.be' in p.netloc:
        return p.path[1:]
    return None


def get_embed_url(url):
    vid = extract_video_id(url)
    return f"https://www.youtube.com/embed/{vid}" if vid else url


def get_thumbnail_url(url):
    vid = extract_video_id(url)
    return f"https://img.youtube.com/vi/{vid}/mqdefault.jpg" if vid else ""


async def init_db(app):
    global db_connection
    try:
        db_connection = psycopg2.connect(**DB_CONFIG)
        c = db_connection.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL, email TEXT UNIQUE NOT NULL,
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

        # Login sessions — one row per login event
        c.execute('''CREATE TABLE IF NOT EXISTS login_sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            logged_at TIMESTAMPTZ DEFAULT NOW(),
            ip_hash TEXT)''')

        # Comments on videos
        c.execute('''CREATE TABLE IF NOT EXISTS comments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            user_name TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW())''')

        # Indexes
        for sql in [
            'CREATE INDEX IF NOT EXISTS idx_vid_created ON videos(created_at DESC)',
            'CREATE INDEX IF NOT EXISTS idx_vv_vid ON video_views(video_id)',
            'CREATE INDEX IF NOT EXISTS idx_wh_user ON watch_history(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_wh_video ON watch_history(video_id)',
            'CREATE INDEX IF NOT EXISTS idx_wh_session ON watch_history(session_start)',
            'CREATE INDEX IF NOT EXISTS idx_ls_user ON login_sessions(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_comments_video ON comments(video_id)',
        ]:
            c.execute(sql)

        # Safe column upgrades
        for sql in [
            "ALTER TABLE videos ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE SET NULL",
            "ALTER TABLE videos ADD COLUMN IF NOT EXISTS library_name TEXT",
            "ALTER TABLE videos ADD COLUMN IF NOT EXISTS is_public BOOLEAN DEFAULT FALSE",
            "ALTER TABLE watch_history ADD COLUMN IF NOT EXISTS session_start TIMESTAMPTZ DEFAULT NOW()",
        ]:
            try:
                c.execute(sql)
            except Exception:
                db_connection.rollback()

        db_connection.commit()
        c.close()
        print("✅ DB ready")
    except Exception as e:
        print(f"⚠️ DB init failed: {e}")
        db_connection = None


async def close_db(app):
    global db_connection
    if db_connection and not db_connection.closed:
        db_connection.close()


# ── USERS ─────────────────────────────────────

async def create_or_get_user(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        name = d.get('name', '').strip()
        email = d.get('email', '').strip().lower()
        if not name or not email:
            return web.json_response({'error': 'name+email required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT * FROM users WHERE email=%s', (email,))
        row = cur.fetchone()
        if row:
            cur.close()
            return web.json_response({'success': True, 'is_new': False, 'user': {
                'id': str(row['id']), 'name': row['name'],
                'email': row['email'], 'created_at': row['created_at'].isoformat()}})
        cur.execute('INSERT INTO users(name,email) VALUES(%s,%s) RETURNING *', (name, email))
        row = cur.fetchone()
        conn.commit(); cur.close()
        return web.json_response({'success': True, 'is_new': True, 'user': {
            'id': str(row['id']), 'name': row['name'],
            'email': row['email'], 'created_at': row['created_at'].isoformat()}})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


async def get_users(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('''SELECT u.id,u.name,u.email,u.created_at,COUNT(v.id) as video_count
            FROM users u LEFT JOIN videos v ON v.user_id=u.id
            GROUP BY u.id ORDER BY u.created_at DESC''')
        rows = cur.fetchall(); cur.close()
        return web.json_response({'success': True, 'users': [
            {'id': str(r['id']), 'name': r['name'], 'email': r['email'],
             'video_count': r['video_count'], 'created_at': r['created_at'].isoformat()}
            for r in rows]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


# ── LOGIN SESSIONS ─────────────────────────────

async def record_login_session(request):
    """Called every time a user successfully logs in."""
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        uid = d.get('user_id')
        if not uid:
            return web.json_response({'error': 'user_id required'}, status=400)
        ip_hash = hash_ip(get_client_ip(request))
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            'INSERT INTO login_sessions(user_id, ip_hash) VALUES(%s,%s) RETURNING id, logged_at',
            (str(uuid.UUID(uid)), ip_hash)
        )
        row = cur.fetchone()
        conn.commit(); cur.close()
        return web.json_response({'success': True, 'session_id': str(row['id'])})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


async def get_session_stats(request):
    """Per-user login session counts and timelines."""
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        uid = request.rel_url.query.get('user_id')
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if uid:
            cur.execute('''SELECT u.id, u.name, u.email,
                COUNT(ls.id) as login_count,
                MAX(ls.logged_at) as last_login,
                MIN(ls.logged_at) as first_login
                FROM users u LEFT JOIN login_sessions ls ON ls.user_id=u.id
                WHERE u.id=%s
                GROUP BY u.id''', (uid,))
        else:
            cur.execute('''SELECT u.id, u.name, u.email,
                COUNT(ls.id) as login_count,
                MAX(ls.logged_at) as last_login,
                MIN(ls.logged_at) as first_login
                FROM users u LEFT JOIN login_sessions ls ON ls.user_id=u.id
                GROUP BY u.id ORDER BY login_count DESC''')
        stats = cur.fetchall()

        # Recent login timeline
        uf = "WHERE ls.user_id=%s" if uid else ""
        p = [uid] if uid else []
        cur.execute(f'''SELECT ls.logged_at, u.name as user_name, u.id as user_id
            FROM login_sessions ls JOIN users u ON u.id=ls.user_id
            {uf}
            ORDER BY ls.logged_at DESC LIMIT 100''', p)
        timeline = cur.fetchall()
        cur.close()

        return web.json_response({'success': True,
            'stats': [{
                'user_id': str(r['id']), 'name': r['name'], 'email': r['email'],
                'login_count': int(r['login_count']),
                'last_login': r['last_login'].isoformat() if r['last_login'] else None,
                'first_login': r['first_login'].isoformat() if r['first_login'] else None
            } for r in stats],
            'timeline': [{
                'user_name': r['user_name'],
                'user_id': str(r['user_id']),
                'logged_at': r['logged_at'].isoformat()
            } for r in timeline]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


# ── VIDEOS ────────────────────────────────────

async def add_video(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        url = d.get('url', '').strip()
        email = d.get('email', '').strip().lower()
        if not url or not email:
            return web.json_response({'error': 'url+email required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT id,name FROM users WHERE email=%s', (email,))
        u = cur.fetchone()
        uid = str(u['id']) if u else None
        lname = u['name'] if u else None
        dname = u['name'] if u else email
        cur.execute('''INSERT INTO videos(url,embed_url,thumbnail,added_by,user_id,library_name,is_public,views,created_at)
            VALUES(%s,%s,%s,%s,%s,%s,FALSE,0,NOW()) RETURNING *''',
            (url, get_embed_url(url), get_thumbnail_url(url), dname, uid, lname))
        row = cur.fetchone(); conn.commit(); cur.close()
        return web.json_response({'success': True, 'video': {
            '_id': str(row['id']), 'url': row['url'], 'embed_url': row['embed_url'],
            'thumbnail': row['thumbnail'], 'added_by': row['added_by'],
            'user_id': str(row['user_id']) if row['user_id'] else None,
            'library_name': row['library_name'],
            'is_public': row['is_public'],
            'views': row['views'],
            'created_at': row['created_at'].isoformat()}})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


async def get_videos(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        uid = request.rel_url.query.get('user_id')
        public_only = request.rel_url.query.get('public_only') == 'true'
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if uid:
            cur.execute('SELECT * FROM videos WHERE user_id=%s ORDER BY created_at DESC', (uid,))
        elif public_only:
            cur.execute('SELECT * FROM videos WHERE is_public=TRUE ORDER BY created_at DESC')
        else:
            cur.execute('SELECT * FROM videos ORDER BY created_at DESC')
        rows = cur.fetchall(); cur.close()
        return web.json_response({'success': True, 'videos': [{
            '_id': str(r['id']), 'url': r['url'], 'embed_url': r['embed_url'],
            'thumbnail': r['thumbnail'], 'added_by': r['added_by'],
            'user_id': str(r['user_id']) if r['user_id'] else None,
            'library_name': r['library_name'],
            'is_public': r['is_public'],
            'views': r['views'],
            'created_at': r['created_at'].isoformat()} for r in rows]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def toggle_video_visibility(request):
    """Toggle is_public for a video. Only the owner can do this."""
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        vid = d.get('video_id')
        uid = d.get('user_id')
        if not vid or not uid:
            return web.json_response({'error': 'video_id+user_id required'}, status=400)
        vid_uuid = str(uuid.UUID(vid))
        uid_uuid = str(uuid.UUID(uid))
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT id, user_id, is_public FROM videos WHERE id=%s', (vid_uuid,))
        row = cur.fetchone()
        if not row:
            cur.close(); return web.json_response({'error': 'Not found'}, status=404)
        if str(row['user_id']) != uid_uuid:
            cur.close(); return web.json_response({'error': 'Not your video'}, status=403)
        new_status = not row['is_public']
        cur.execute('UPDATE videos SET is_public=%s WHERE id=%s', (new_status, vid_uuid))
        conn.commit(); cur.close()
        return web.json_response({'success': True, 'is_public': new_status})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


async def delete_video(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        vid = d.get('video_id')
        email = d.get('email', '').strip().lower()
        if not vid:
            return web.json_response({'error': 'video_id required'}, status=400)
        if email != ADMIN_EMAIL.lower():
            return web.json_response({'error': 'Unauthorized. Admin only.'}, status=403)
        cur = conn.cursor()
        cur.execute('DELETE FROM videos WHERE id=%s', (str(uuid.UUID(vid)),))
        n = cur.rowcount; conn.commit(); cur.close()
        if n == 0:
            return web.json_response({'error': 'Not found'}, status=404)
        return web.json_response({'success': True})
    except ValueError:
        return web.json_response({'error': 'Invalid ID'}, status=400)
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


async def update_views(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        vid = uuid.UUID(d.get('video_id'))
        ip_hash = hash_ip(get_client_ip(request))
        cur = conn.cursor()
        try:
            cur.execute('INSERT INTO video_views(video_id,ip_hash) VALUES(%s,%s)', (str(vid), ip_hash))
            cur.execute('UPDATE videos SET views=views+1 WHERE id=%s', (str(vid),))
            conn.commit(); cur.close()
            return web.json_response({'success': True, 'new_view': True})
        except psycopg2.IntegrityError:
            conn.rollback(); cur.close()
            return web.json_response({'success': True, 'new_view': False})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


async def get_video_stats(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        vid = uuid.UUID(request.match_info.get('video_id'))
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT views FROM videos WHERE id=%s', (str(vid),))
        v = cur.fetchone()
        if not v:
            cur.close(); return web.json_response({'error': 'Not found'}, status=404)
        cur.execute('SELECT COUNT(*) as c FROM video_views WHERE video_id=%s', (str(vid),))
        uv = cur.fetchone()['c']; cur.close()
        return web.json_response({'success': True, 'unique_views': uv, 'total_views': v['views']})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


# ── COMMENTS ──────────────────────────────────

async def add_comment(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        video_id = d.get('video_id')
        user_id = d.get('user_id')
        text = d.get('text', '').strip()
        if not video_id or not user_id or not text:
            return web.json_response({'error': 'video_id, user_id, text required'}, status=400)
        if len(text) > 1000:
            return web.json_response({'error': 'Comment too long (max 1000 chars)'}, status=400)

        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT name FROM users WHERE id=%s', (str(uuid.UUID(user_id)),))
        u = cur.fetchone()
        if not u:
            cur.close(); return web.json_response({'error': 'User not found'}, status=404)

        cur.execute('''INSERT INTO comments(video_id, user_id, user_name, text)
            VALUES(%s,%s,%s,%s) RETURNING *''',
            (str(uuid.UUID(video_id)), str(uuid.UUID(user_id)), u['name'], text))
        row = cur.fetchone(); conn.commit(); cur.close()
        return web.json_response({'success': True, 'comment': {
            'id': str(row['id']),
            'video_id': str(row['video_id']),
            'user_id': str(row['user_id']),
            'user_name': row['user_name'],
            'text': row['text'],
            'created_at': row['created_at'].isoformat()
        }})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


async def get_comments(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        video_id = request.rel_url.query.get('video_id')
        if not video_id:
            return web.json_response({'error': 'video_id required'}, status=400)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('''SELECT * FROM comments WHERE video_id=%s ORDER BY created_at DESC''',
            (str(uuid.UUID(video_id)),))
        rows = cur.fetchall(); cur.close()
        return web.json_response({'success': True, 'comments': [{
            'id': str(r['id']),
            'user_id': str(r['user_id']),
            'user_name': r['user_name'],
            'text': r['text'],
            'created_at': r['created_at'].isoformat()
        } for r in rows]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def delete_comment(request):
    """Delete a comment — owner or admin."""
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        comment_id = d.get('comment_id')
        user_id = d.get('user_id')
        admin_email = d.get('admin_email', '').strip().lower()
        if not comment_id:
            return web.json_response({'error': 'comment_id required'}, status=400)

        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT user_id FROM comments WHERE id=%s', (str(uuid.UUID(comment_id)),))
        row = cur.fetchone()
        if not row:
            cur.close(); return web.json_response({'error': 'Not found'}, status=404)

        is_admin = admin_email == ADMIN_EMAIL.lower()
        is_owner = user_id and str(row['user_id']) == str(uuid.UUID(user_id))
        if not is_admin and not is_owner:
            cur.close(); return web.json_response({'error': 'Unauthorized'}, status=403)

        cur.execute('DELETE FROM comments WHERE id=%s', (str(uuid.UUID(comment_id)),))
        conn.commit(); cur.close()
        return web.json_response({'success': True})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


# ── WATCH HISTORY ─────────────────────────────

async def record_watch(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        uid = d.get('user_id')
        vid = d.get('video_id')
        secs = int(d.get('seconds_watched', 0))
        ss_iso = d.get('session_start')
        if not uid or not vid:
            return web.json_response({'error': 'user_id+video_id required'}, status=400)
        try:
            ss = datetime.fromisoformat(ss_iso.replace('Z', '+00:00'))
        except Exception:
            ss = datetime.utcnow()

        vid_uuid = str(uuid.UUID(vid))
        uid_uuid = str(uuid.UUID(uid))

        cur = conn.cursor()
        cur.execute('''INSERT INTO watch_history(user_id,video_id,seconds_watched,session_start,watched_at)
            VALUES(%s,%s,%s,%s,NOW())''', (uid_uuid, vid_uuid, secs, ss))
        cur.execute('UPDATE videos SET views=views+1 WHERE id=%s', (vid_uuid,))
        conn.commit(); cur.close()
        return web.json_response({'success': True})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


async def get_history(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        uid = request.rel_url.query.get('user_id')
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if uid:
            cur.execute('''SELECT u.id,u.name,u.email,
                COALESCE(SUM(wh.seconds_watched),0) as total_seconds,
                COUNT(DISTINCT wh.video_id) as videos_watched,
                MAX(wh.watched_at) as last_watched
                FROM users u LEFT JOIN watch_history wh ON wh.user_id=u.id
                WHERE u.id=%s
                GROUP BY u.id ORDER BY total_seconds DESC''', (uid,))
        else:
            cur.execute('''SELECT u.id,u.name,u.email,
                COALESCE(SUM(wh.seconds_watched),0) as total_seconds,
                COUNT(DISTINCT wh.video_id) as videos_watched,
                MAX(wh.watched_at) as last_watched
                FROM users u LEFT JOIN watch_history wh ON wh.user_id=u.id
                GROUP BY u.id ORDER BY total_seconds DESC''')
        us = cur.fetchall()

        if uid:
            cur.execute('''SELECT wh.user_id,u.name as user_name,wh.video_id,
                v.url,v.embed_url,v.thumbnail,v.added_by,
                SUM(wh.seconds_watched) as total_seconds,
                COUNT(*) as session_count, MAX(wh.watched_at) as last_watched
                FROM watch_history wh JOIN users u ON u.id=wh.user_id JOIN videos v ON v.id=wh.video_id
                WHERE wh.user_id=%s
                GROUP BY wh.user_id,u.name,wh.video_id,v.url,v.embed_url,v.thumbnail,v.added_by
                ORDER BY last_watched DESC''', (uid,))
        else:
            cur.execute('''SELECT wh.user_id,u.name as user_name,wh.video_id,
                v.url,v.embed_url,v.thumbnail,v.added_by,
                SUM(wh.seconds_watched) as total_seconds,
                COUNT(*) as session_count, MAX(wh.watched_at) as last_watched
                FROM watch_history wh JOIN users u ON u.id=wh.user_id JOIN videos v ON v.id=wh.video_id
                GROUP BY wh.user_id,u.name,wh.video_id,v.url,v.embed_url,v.thumbnail,v.added_by
                ORDER BY last_watched DESC''')
        vd = cur.fetchall(); cur.close()
        return web.json_response({'success': True,
            'user_stats': [{'user_id': str(r['id']), 'name': r['name'], 'email': r['email'],
                'total_seconds': int(r['total_seconds']), 'videos_watched': int(r['videos_watched']),
                'last_watched': r['last_watched'].isoformat() if r['last_watched'] else None}
                for r in us],
            'video_details': [{'user_id': str(r['user_id']), 'user_name': r['user_name'],
                'video_id': str(r['video_id']), 'url': r['url'], 'embed_url': r['embed_url'],
                'thumbnail': r['thumbnail'], 'added_by': r['added_by'],
                'total_seconds': int(r['total_seconds']), 'session_count': int(r['session_count']),
                'last_watched': r['last_watched'].isoformat()} for r in vd]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


async def get_analytics(request):
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        uid = request.rel_url.query.get('user_id')
        days = int(request.rel_url.query.get('days', 30))
        cur = conn.cursor(cursor_factory=RealDictCursor)

        uf = "AND wh.user_id=%s" if uid else ""
        p = [uid] if uid else []

        cur.execute(f'''SELECT u.name as user_name, u.id::text as user_id,
            DATE(wh.session_start AT TIME ZONE 'UTC') as day,
            SUM(wh.seconds_watched) as total_seconds, COUNT(*) as session_count
            FROM watch_history wh JOIN users u ON u.id=wh.user_id
            WHERE wh.session_start >= NOW() - INTERVAL '{days} days' {uf}
            GROUP BY u.id,u.name,DATE(wh.session_start AT TIME ZONE 'UTC')
            ORDER BY day ASC''', p)
        day_rows = cur.fetchall()

        cur.execute(f'''SELECT u.name as user_name, u.id::text as user_id,
            EXTRACT(HOUR FROM wh.session_start AT TIME ZONE 'UTC')::int as hour,
            SUM(wh.seconds_watched) as total_seconds, COUNT(*) as session_count
            FROM watch_history wh JOIN users u ON u.id=wh.user_id
            WHERE wh.session_start >= NOW() - INTERVAL '{days} days' {uf}
            GROUP BY u.id,u.name,EXTRACT(HOUR FROM wh.session_start AT TIME ZONE 'UTC')
            ORDER BY hour ASC''', p)
        hour_rows = cur.fetchall()

        cur.execute(f'''SELECT u.name as user_name, u.id::text as user_id,
            wh.session_start, wh.seconds_watched, v.url, v.thumbnail
            FROM watch_history wh JOIN users u ON u.id=wh.user_id JOIN videos v ON v.id=wh.video_id
            WHERE wh.session_start >= NOW() - INTERVAL '{days} days' {uf}
            ORDER BY wh.session_start DESC LIMIT 200''', p)
        sessions = cur.fetchall()

        # Most watched videos with play count
        uf2 = "AND wh.user_id=%s" if uid else ""
        cur.execute(f'''SELECT v.id::text as video_id, v.url, v.thumbnail,
            COUNT(*) as play_count, SUM(wh.seconds_watched) as total_seconds,
            u.name as user_name
            FROM watch_history wh
            JOIN videos v ON v.id=wh.video_id
            JOIN users u ON u.id=wh.user_id
            WHERE wh.session_start >= NOW() - INTERVAL '{days} days' {uf2}
            GROUP BY v.id, v.url, v.thumbnail, u.name
            ORDER BY play_count DESC LIMIT 20''', p)
        top_videos = cur.fetchall()

        cur.close()

        return web.json_response({'success': True,
            'day_data': [{'user_name': r['user_name'], 'user_id': r['user_id'],
                'day': r['day'].isoformat(), 'total_seconds': int(r['total_seconds']),
                'session_count': int(r['session_count'])} for r in day_rows],
            'hour_data': [{'user_name': r['user_name'], 'user_id': r['user_id'],
                'hour': r['hour'], 'total_seconds': int(r['total_seconds']),
                'session_count': int(r['session_count'])} for r in hour_rows],
            'sessions': [{'user_name': r['user_name'], 'user_id': r['user_id'],
                'session_start': r['session_start'].isoformat(),
                'seconds_watched': r['seconds_watched'],
                'url': r['url'], 'thumbnail': r['thumbnail']} for r in sessions],
            'top_videos': [{'video_id': r['video_id'], 'url': r['url'],
                'thumbnail': r['thumbnail'], 'play_count': int(r['play_count']),
                'total_seconds': int(r['total_seconds']),
                'user_name': r['user_name']} for r in top_videos]})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


# ── ADMIN: PURGE ALL DATA ──────────────────────

async def purge_all_data(request):
    """
    Truncate ALL tables. Requires admin email confirmation.
    DANGEROUS — irreversible!
    """
    conn = get_db_connection()
    if not conn:
        return web.json_response({'error': 'DB not connected'}, status=503)
    try:
        d = await request.json()
        email = d.get('email', '').strip().lower()
        confirm = d.get('confirm', '')
        if email != ADMIN_EMAIL.lower():
            return web.json_response({'error': 'Unauthorized. Admin only.'}, status=403)
        if confirm != 'DELETE_ALL':
            return web.json_response({'error': 'confirm field must be "DELETE_ALL"'}, status=400)

        cur = conn.cursor()
        # Truncate in dependency order (child first)
        tables = ['comments', 'watch_history', 'video_views', 'login_sessions', 'videos', 'users']
        for t in tables:
            cur.execute(f'TRUNCATE TABLE {t} CASCADE')
        conn.commit(); cur.close()
        return web.json_response({'success': True, 'message': 'All data purged.'})
    except Exception as e:
        conn.rollback()
        return web.json_response({'error': str(e)}, status=500)


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

    # Users
    app.router.add_post('/api/users/create', create_or_get_user)
    app.router.add_get('/api/users', get_users)

    # Sessions
    app.router.add_post('/api/sessions/login', record_login_session)
    app.router.add_get('/api/sessions', get_session_stats)

    # Videos
    app.router.add_post('/api/videos/add', add_video)
    app.router.add_get('/api/videos', get_videos)
    app.router.add_post('/api/videos/delete', delete_video)
    app.router.add_post('/api/videos/toggle', toggle_video_visibility)
    app.router.add_post('/api/videos/view', update_views)
    app.router.add_get('/api/videos/{video_id}/stats', get_video_stats)

    # Comments
    app.router.add_post('/api/comments/add', add_comment)
    app.router.add_get('/api/comments', get_comments)
    app.router.add_post('/api/comments/delete', delete_comment)

    # History & analytics
    app.router.add_post('/api/history/record', record_watch)
    app.router.add_get('/api/history', get_history)
    app.router.add_get('/api/analytics', get_analytics)

    # Admin
    app.router.add_post('/api/admin/purge', purge_all_data)

    for route in list(app.router.routes()):
        cors.add(route)
    app.on_startup.append(init_db)
    app.on_cleanup.append(close_db)
    return app


if __name__ == '__main__':
    print("🚀 Video Manager v4 — with comments, sessions, visibility & purge")
    web.run_app(create_app(), host='0.0.0.0', port=9000)
