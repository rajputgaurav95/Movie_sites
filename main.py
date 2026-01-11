from aiohttp import web
import aiohttp_cors
import asyncpg
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import os
import hashlib
import uuid

# PostgreSQL Connection
DB_CONFIG = {
    'user': 'postgres',
    'password': 'Gourav@123#',
    'host': 'db.ntshrlzpfyvfnkkxckfs.supabase.co',
    'port': 5432,
    'database': 'postgres'
}

# Admin email
ADMIN_EMAIL = "abc@gmail.com"

# Global database pool
db_pool = None


def get_client_ip(request):
    """Get client IP address"""
    forwarded = request.headers.get('X-Forwarded-For')
    if forwarded:
        return forwarded.split(',')[0].strip()
    
    real_ip = request.headers.get('X-Real-IP')
    if real_ip:
        return real_ip
    
    peername = request.transport.get_extra_info('peername')
    if peername:
        return peername[0]
    
    return 'unknown'


def hash_ip(ip_address):
    """Hash IP address for privacy"""
    return hashlib.sha256(ip_address.encode()).hexdigest()


def extract_video_id(url):
    """Extract video ID from YouTube URL"""
    parsed = urlparse(url)
    if 'youtube.com' in parsed.netloc:
        query = parse_qs(parsed.query)
        return query.get('v', [None])[0]
    elif 'youtu.be' in parsed.netloc:
        return parsed.path[1:]
    return None


def get_embed_url(url):
    """Convert YouTube URL to embed URL"""
    video_id = extract_video_id(url)
    if video_id:
        return f"https://www.youtube.com/embed/{video_id}"
    return url


def get_thumbnail_url(url):
    """Get YouTube thumbnail"""
    video_id = extract_video_id(url)
    if video_id:
        return f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg"
    return ""


async def init_db(app):
    """Initialize database connection pool"""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(**DB_CONFIG, min_size=1, max_size=10)
        print("✅ Connected to PostgreSQL successfully")
        
        # Create tables if they don't exist
        async with db_pool.acquire() as conn:
            # Videos table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    url TEXT NOT NULL,
                    embed_url TEXT,
                    thumbnail TEXT,
                    added_by TEXT NOT NULL,
                    views INTEGER DEFAULT 0,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            ''')
            
            # Video views table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS video_views (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
                    ip_hash TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE(video_id, ip_hash)
                )
            ''')
            
            # Create indexes
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_created ON videos(created_at DESC)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_views_video ON video_views(video_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_views_created ON video_views(created_at)')
            
        print("✅ Database tables and indexes ready")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("⚠️ App will start but database operations may fail")


async def close_db(app):
    """Close database connection pool"""
    global db_pool
    if db_pool:
        await db_pool.close()
        print("❌ Closed PostgreSQL connection")


async def add_video(request):
    """Add a new video URL"""
    try:
        data = await request.json()
        url = data.get('url', '').strip()
        user_email = data.get('email', '').strip()
        
        if not url:
            return web.json_response({'error': 'URL is required'}, status=400)
        
        if not user_email:
            return web.json_response({'error': 'Email is required'}, status=400)
        
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow('''
                INSERT INTO videos (url, embed_url, thumbnail, added_by, views, created_at)
                VALUES ($1, $2, $3, $4, 0, NOW())
                RETURNING id, url, embed_url, thumbnail, added_by, views, created_at
            ''', url, get_embed_url(url), get_thumbnail_url(url), user_email)
            
            video = {
                '_id': str(row['id']),
                'url': row['url'],
                'embed_url': row['embed_url'],
                'thumbnail': row['thumbnail'],
                'added_by': row['added_by'],
                'views': row['views'],
                'created_at': row['created_at'].isoformat()
            }
            
            return web.json_response({
                'success': True,
                'video': video
            })
    
    except Exception as e:
        print(f"Error adding video: {e}")
        return web.json_response({'error': str(e)}, status=500)


async def get_videos(request):
    """Get all videos"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT id, url, embed_url, thumbnail, added_by, views, created_at
                FROM videos
                ORDER BY created_at DESC
            ''')
            
            videos = []
            for row in rows:
                videos.append({
                    '_id': str(row['id']),
                    'url': row['url'],
                    'embed_url': row['embed_url'],
                    'thumbnail': row['thumbnail'],
                    'added_by': row['added_by'],
                    'views': row['views'],
                    'created_at': row['created_at'].isoformat()
                })
            
            return web.json_response({
                'success': True,
                'videos': videos
            })
    
    except Exception as e:
        print(f"Error getting videos: {e}")
        return web.json_response({'error': str(e)}, status=500)


async def delete_video(request):
    """Delete a video (admin only)"""
    try:
        data = await request.json()
        video_id = data.get('video_id')
        user_email = data.get('email', '').strip()
        
        if not video_id:
            return web.json_response({'error': 'Video ID is required'}, status=400)
        
        if user_email.lower() != ADMIN_EMAIL.lower():
            return web.json_response({'error': 'Unauthorized. Admin only.'}, status=403)
        
        try:
            video_uuid = uuid.UUID(video_id)
        except ValueError:
            return web.json_response({'error': 'Invalid video ID'}, status=400)
        
        async with db_pool.acquire() as conn:
            result = await conn.execute('DELETE FROM videos WHERE id = $1', video_uuid)
            
            if result == 'DELETE 0':
                return web.json_response({'error': 'Video not found'}, status=404)
            
            return web.json_response({'success': True, 'message': 'Video deleted'})
    
    except Exception as e:
        print(f"Error deleting video: {e}")
        return web.json_response({'error': str(e)}, status=500)


async def update_views(request):
    """Update view count based on IP address (one view per IP per video)"""
    try:
        data = await request.json()
        video_id = data.get('video_id')
        
        if not video_id:
            return web.json_response({'error': 'Video ID is required'}, status=400)
        
        try:
            video_uuid = uuid.UUID(video_id)
        except ValueError:
            return web.json_response({'error': 'Invalid video ID'}, status=400)
        
        client_ip = get_client_ip(request)
        ip_hash = hash_ip(client_ip)
        
        async with db_pool.acquire() as conn:
            try:
                # Try to insert view record
                await conn.execute('''
                    INSERT INTO video_views (video_id, ip_hash, created_at)
                    VALUES ($1, $2, NOW())
                ''', video_uuid, ip_hash)
                
                # Increment view count
                await conn.execute('''
                    UPDATE videos SET views = views + 1 WHERE id = $1
                ''', video_uuid)
                
                return web.json_response({
                    'success': True,
                    'new_view': True,
                    'message': 'View counted'
                })
                
            except asyncpg.UniqueViolationError:
                # This IP already viewed this video
                return web.json_response({
                    'success': True,
                    'new_view': False,
                    'message': 'Already viewed from this IP'
                })
    
    except Exception as e:
        print(f"Error updating views: {e}")
        return web.json_response({'error': str(e)}, status=500)


async def get_video_stats(request):
    """Get detailed stats for a video"""
    try:
        video_id = request.match_info.get('video_id')
        
        try:
            video_uuid = uuid.UUID(video_id)
        except ValueError:
            return web.json_response({'error': 'Invalid video ID'}, status=400)
        
        async with db_pool.acquire() as conn:
            video = await conn.fetchrow('SELECT views FROM videos WHERE id = $1', video_uuid)
            
            if not video:
                return web.json_response({'error': 'Video not found'}, status=404)
            
            unique_views = await conn.fetchval(
                'SELECT COUNT(*) FROM video_views WHERE video_id = $1', 
                video_uuid
            )
            
            return web.json_response({
                'success': True,
                'video_id': video_id,
                'unique_views': unique_views,
                'total_views': video['views']
            })
    
    except Exception as e:
        print(f"Error getting video stats: {e}")
        return web.json_response({'error': str(e)}, status=500)


async def index(request):
    """Serve the frontend HTML"""
    try:
        html_path = os.path.join(os.path.dirname(__file__), 'index.html')
        with open(html_path, 'r', encoding='utf-8') as f:
            return web.Response(text=f.read(), content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='index.html not found', status=404)


def create_app():
    """Create and configure the application"""
    app = web.Application()
    
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*"
        )
    })
    
    app.router.add_get('/', index)
    app.router.add_post('/api/videos/add', add_video)
    app.router.add_get('/api/videos', get_videos)
    app.router.add_post('/api/videos/delete', delete_video)
    app.router.add_post('/api/videos/view', update_views)
    app.router.add_get('/api/videos/{video_id}/stats', get_video_stats)
    
    for route in list(app.router.routes()):
        cors.add(route)
    
    app.on_startup.append(init_db)
    app.on_cleanup.append(close_db)
    
    return app


if __name__ == '__main__':
    print("🚀 Starting Video Manager Server...")
    print("🐘 Using PostgreSQL (Supabase)")
    print("📊 IP-based view tracking enabled")
    print("🌐 Server will run on http://localhost:8080")
    print("-" * 50)
    app = create_app()
    web.run_app(app, host='0.0.0.0', port=8080)
