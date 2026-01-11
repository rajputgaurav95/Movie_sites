# from aiohttp import web
# import aiohttp_cors
# from motor.motor_asyncio import AsyncIOMotorClient
# from bson import ObjectId
# from datetime import datetime
# from urllib.parse import urlparse, parse_qs
# import os

# # MongoDB Connection
# MONGO_URI = "mongodb+srv://dataxbsqhjg_db_user:7cYqz2EatkTZZCII@cluster0.iyah27z.mongodb.net/"
# DB_NAME = "video_manager"
# COLLECTION_NAME = "video_collection"

# # Admin email
# ADMIN_EMAIL = "abc@gmail.com"

# # Global MongoDB client
# client = None
# db = None
# collection = None

# def extract_video_id(url):
#     """Extract video ID from YouTube URL"""
#     parsed = urlparse(url)
#     if 'youtube.com' in parsed.netloc:
#         query = parse_qs(parsed.query)
#         return query.get('v', [None])[0]
#     elif 'youtu.be' in parsed.netloc:
#         return parsed.path[1:]
#     return None

# def get_embed_url(url):
#     """Convert YouTube URL to embed URL"""
#     video_id = extract_video_id(url)
#     if video_id:
#         return f"https://www.youtube.com/embed/{video_id}"
#     return url

# def get_thumbnail_url(url):
#     """Get YouTube thumbnail"""
#     video_id = extract_video_id(url)
#     if video_id:
#         return f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg"
#     return ""

# async def init_db(app):
#     """Initialize database connection"""
#     global client, db, collection
#     try:
#         client = AsyncIOMotorClient(MONGO_URI)
#         db = client[DB_NAME]
#         collection = db[COLLECTION_NAME]
#         # Test connection
#         await client.admin.command('ping')
#         print("✅ Connected to MongoDB successfully")
#     except Exception as e:
#         print(f"❌ Failed to connect to MongoDB: {e}")
#         raise

# async def close_db(app):
#     """Close database connection"""
#     global client
#     if client:
#         client.close()
#         print("❌ Closed MongoDB connection")

# async def add_video(request):
#     """Add a new video URL"""
#     try:
#         data = await request.json()
#         url = data.get('url', '').strip()
#         user_email = data.get('email', '').strip()
        
#         if not url:
#             return web.json_response({'error': 'URL is required'}, status=400)
        
#         if not user_email:
#             return web.json_response({'error': 'Email is required'}, status=400)
        
#         # Create video document
#         video_doc = {
#             'url': url,
#             'embed_url': get_embed_url(url),
#             'thumbnail': get_thumbnail_url(url),
#             'added_by': user_email,
#             'created_at': datetime.utcnow(),
#             'views': 0
#         }
        
#         result = await collection.insert_one(video_doc)
#         video_doc['_id'] = str(result.inserted_id)
#         video_doc['created_at'] = video_doc['created_at'].isoformat()
        
#         return web.json_response({
#             'success': True,
#             'video': video_doc
#         })
    
#     except Exception as e:
#         print(f"Error adding video: {e}")
#         return web.json_response({'error': str(e)}, status=500)

# async def get_videos(request):
#     """Get all videos"""
#     try:
#         videos = []
#         cursor = collection.find().sort('created_at', -1)
        
#         async for doc in cursor:
#             doc['_id'] = str(doc['_id'])
#             doc['created_at'] = doc['created_at'].isoformat()
#             videos.append(doc)
        
#         return web.json_response({
#             'success': True,
#             'videos': videos
#         })
    
#     except Exception as e:
#         print(f"Error getting videos: {e}")
#         return web.json_response({'error': str(e)}, status=500)

# async def delete_video(request):
#     """Delete a video (admin only)"""
#     try:
#         data = await request.json()
#         video_id = data.get('video_id')
#         user_email = data.get('email', '').strip()
        
#         if not video_id:
#             return web.json_response({'error': 'Video ID is required'}, status=400)
        
#         # Check if user is admin
#         if user_email.lower() != ADMIN_EMAIL.lower():
#             return web.json_response({'error': 'Unauthorized. Admin only.'}, status=403)
        
#         # Validate ObjectId
#         if not ObjectId.is_valid(video_id):
#             return web.json_response({'error': 'Invalid video ID'}, status=400)
        
#         result = await collection.delete_one({'_id': ObjectId(video_id)})
        
#         if result.deleted_count > 0:
#             return web.json_response({'success': True, 'message': 'Video deleted'})
#         else:
#             return web.json_response({'error': 'Video not found'}, status=404)
    
#     except Exception as e:
#         print(f"Error deleting video: {e}")
#         return web.json_response({'error': str(e)}, status=500)

# async def update_views(request):
#     """Update view count"""
#     try:
#         data = await request.json()
#         video_id = data.get('video_id')
        
#         if not video_id:
#             return web.json_response({'error': 'Video ID is required'}, status=400)
        
#         # Validate ObjectId
#         if not ObjectId.is_valid(video_id):
#             return web.json_response({'error': 'Invalid video ID'}, status=400)
        
#         await collection.update_one(
#             {'_id': ObjectId(video_id)},
#             {'$inc': {'views': 1}}
#         )
        
#         return web.json_response({'success': True})
    
#     except Exception as e:
#         print(f"Error updating views: {e}")
#         return web.json_response({'error': str(e)}, status=500)

# async def index(request):
#     """Serve the frontend HTML"""
#     try:
#         html_path = os.path.join(os.path.dirname(__file__), 'index.html')
#         with open(html_path, 'r', encoding='utf-8') as f:
#             return web.Response(text=f.read(), content_type='text/html')
#     except FileNotFoundError:
#         return web.Response(text='index.html not found', status=404)

# def create_app():
#     """Create and configure the application"""
#     app = web.Application()
    
#     # Setup CORS
#     cors = aiohttp_cors.setup(app, defaults={
#         "*": aiohttp_cors.ResourceOptions(
#             allow_credentials=True,
#             expose_headers="*",
#             allow_headers="*",
#             allow_methods="*"
#         )
#     })
    
#     # Add routes
#     app.router.add_get('/', index)
#     app.router.add_post('/api/videos/add', add_video)
#     app.router.add_get('/api/videos', get_videos)
#     app.router.add_post('/api/videos/delete', delete_video)
#     app.router.add_post('/api/videos/view', update_views)
    
#     # Configure CORS for all routes
#     for route in list(app.router.routes()):
#         cors.add(route)
    
#     # Setup startup/cleanup
#     app.on_startup.append(init_db)
#     app.on_cleanup.append(close_db)
    
#     return app

# if __name__ == '__main__':
#     print("🚀 Starting Video Manager Server...")
#     app = create_app()
#     web.run_app(app, host='0.0.0.0', port=8080)
#     print("🚀 Server running on http://localhost:8080")



from aiohttp import web
import aiohttp_cors
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import os
import hashlib

# MongoDB Connection
MONGO_URI = "mongodb+srv://dataxbsqhjg_db_user:7cYqz2EatkTZZCII@cluster0.iyah27z.mongodb.net/"
DB_NAME = "video_manager"
COLLECTION_NAME = "video_collection"
VIEWS_COLLECTION = "video_views"

# Admin email
ADMIN_EMAIL = "abc@gmail.com"

# Global MongoDB client
client = None
db = None
collection = None
views_collection = None


def get_client_ip(request):
    """Get client IP address"""
    # Check for X-Forwarded-For header (for proxies/load balancers)
    forwarded = request.headers.get('X-Forwarded-For')
    if forwarded:
        return forwarded.split(',')[0].strip()
    
    # Check for X-Real-IP header
    real_ip = request.headers.get('X-Real-IP')
    if real_ip:
        return real_ip
    
    # Fall back to remote address
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
    """Initialize database connection"""
    global client, db, collection, views_collection
    try:
        client = AsyncIOMotorClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        views_collection = db[VIEWS_COLLECTION]
        
        # Create indexes for better performance
        await views_collection.create_index([("video_id", 1), ("ip_hash", 1)], unique=True)
        await views_collection.create_index("created_at")
        
        # Test connection
        await client.admin.command('ping')
        print("✅ Connected to MongoDB successfully")
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        raise


async def close_db(app):
    """Close database connection"""
    global client
    if client:
        client.close()
        print("❌ Closed MongoDB connection")


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
        
        # Create video document
        video_doc = {
            'url': url,
            'embed_url': get_embed_url(url),
            'thumbnail': get_thumbnail_url(url),
            'added_by': user_email,
            'created_at': datetime.utcnow(),
            'views': 0
        }
        
        result = await collection.insert_one(video_doc)
        video_doc['_id'] = str(result.inserted_id)
        video_doc['created_at'] = video_doc['created_at'].isoformat()
        
        return web.json_response({
            'success': True,
            'video': video_doc
        })
    
    except Exception as e:
        print(f"Error adding video: {e}")
        return web.json_response({'error': str(e)}, status=500)


async def get_videos(request):
    """Get all videos"""
    try:
        videos = []
        cursor = collection.find().sort('created_at', -1)
        
        async for doc in cursor:
            doc['_id'] = str(doc['_id'])
            doc['created_at'] = doc['created_at'].isoformat()
            videos.append(doc)
        
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
        
        # Check if user is admin
        if user_email.lower() != ADMIN_EMAIL.lower():
            return web.json_response({'error': 'Unauthorized. Admin only.'}, status=403)
        
        # Validate ObjectId
        if not ObjectId.is_valid(video_id):
            return web.json_response({'error': 'Invalid video ID'}, status=400)
        
        # Delete video
        result = await collection.delete_one({'_id': ObjectId(video_id)})
        
        # Also delete all view records for this video
        await views_collection.delete_many({'video_id': video_id})
        
        if result.deleted_count > 0:
            return web.json_response({'success': True, 'message': 'Video deleted'})
        else:
            return web.json_response({'error': 'Video not found'}, status=404)
    
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
        
        # Validate ObjectId
        if not ObjectId.is_valid(video_id):
            return web.json_response({'error': 'Invalid video ID'}, status=400)
        
        # Get client IP and hash it for privacy
        client_ip = get_client_ip(request)
        ip_hash = hash_ip(client_ip)
        
        # Try to insert view record (will fail if IP already viewed this video)
        view_doc = {
            'video_id': video_id,
            'ip_hash': ip_hash,
            'created_at': datetime.utcnow()
        }
        
        try:
            await views_collection.insert_one(view_doc)
            
            # Only increment view count if this is a new view
            await collection.update_one(
                {'_id': ObjectId(video_id)},
                {'$inc': {'views': 1}}
            )
            
            return web.json_response({
                'success': True,
                'new_view': True,
                'message': 'View counted'
            })
            
        except Exception as duplicate_error:
            # Duplicate key error means this IP already viewed this video
            if 'duplicate key' in str(duplicate_error).lower():
                return web.json_response({
                    'success': True,
                    'new_view': False,
                    'message': 'Already viewed from this IP'
                })
            else:
                raise duplicate_error
    
    except Exception as e:
        print(f"Error updating views: {e}")
        return web.json_response({'error': str(e)}, status=500)


async def get_video_stats(request):
    """Get detailed stats for a video"""
    try:
        video_id = request.match_info.get('video_id')
        
        if not ObjectId.is_valid(video_id):
            return web.json_response({'error': 'Invalid video ID'}, status=400)
        
        # Get video
        video = await collection.find_one({'_id': ObjectId(video_id)})
        if not video:
            return web.json_response({'error': 'Video not found'}, status=404)
        
        # Get unique view count
        unique_views = await views_collection.count_documents({'video_id': video_id})
        
        # Get total views from video document
        total_views = video.get('views', 0)
        
        return web.json_response({
            'success': True,
            'video_id': video_id,
            'unique_views': unique_views,
            'total_views': total_views
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
    
    # Setup CORS
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*"
        )
    })
    
    # Add routes
    app.router.add_get('/', index)
    app.router.add_post('/api/videos/add', add_video)
    app.router.add_get('/api/videos', get_videos)
    app.router.add_post('/api/videos/delete', delete_video)
    app.router.add_post('/api/videos/view', update_views)
    app.router.add_get('/api/videos/{video_id}/stats', get_video_stats)
    
    # Configure CORS for all routes
    for route in list(app.router.routes()):
        cors.add(route)
    
    # Setup startup/cleanup
    app.on_startup.append(init_db)
    app.on_cleanup.append(close_db)
    
    return app


if __name__ == '__main__':
    print("🚀 Starting Video Manager Server...")
    print("📊 IP-based view tracking enabled")
    print("🌐 Server will run on http://localhost:8080")
    print("-" * 50)
    app = create_app()
    web.run_app(app, host='0.0.0.0', port=8080)
