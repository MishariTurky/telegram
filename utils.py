"""
utils.py - دوال مساعدة متكاملة مع معالجة الأخطاء
"""

import asyncio
import logging
import os
import shutil
import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable, Union
from functools import wraps
from telethon.errors import (
    FloodWaitError, RPCError, ChatAdminRequiredError,
    UserPrivacyRestrictedError, ChannelInvalidError
)

logger = logging.getLogger(__name__)

# ============== ديكورات معالجة الأخطاء ==============

def retry_on_error(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    ديكور لإعادة المحاولة عند الفشل مع تأخير متزايد
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except FloodWaitError as e:
                    wait_time = e.seconds
                    logger.warning(f"FloodWait: انتظار {wait_time} ثانية")
                    await asyncio.sleep(min(wait_time, 60))
                    retries += 1
                except (RPCError, ConnectionError, TimeoutError) as e:
                    logger.warning(f"خطأ مؤقت: {e}, إعادة المحاولة {retries + 1}/{max_retries}")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
                    retries += 1
                except Exception as e:
                    logger.error(f"خطأ غير متوقع: {e}")
                    raise
            
            raise Exception(f"فشل بعد {max_retries} محاولات")
        
        return wrapper
    return decorator


def handle_errors(default_return=None, log_error: bool = True):
    """
    ديكور لمعالجة الأخطاء بشكل موحد
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if log_error:
                    logger.error(f"خطأ في {func.__name__}: {e}")
                return default_return
        return wrapper
    return decorator


# ============== دوال مساعدة للقيم ==============

def safe_value(value, default=0, cast_type=float):
    """تحويل القيمة بأمان مع معالجة None"""
    if value is None:
        return default
    try:
        return cast_type(value)
    except (TypeError, ValueError):
        return default


# ============== دوال مساعدة للوقت ==============

def format_duration(seconds: Union[int, float, None]) -> str:
    """تنسيق المدة الزمنية مع معالجة القيم الفارغة"""
    seconds = safe_value(seconds, 0, float)
    
    if seconds <= 0:
        return "0 ثانية"
    
    if seconds < 60:
        return f"{seconds:.0f} ثانية"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        if secs > 0:
            return f"{minutes} دقيقة {secs} ثانية"
        return f"{minutes} دقيقة"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        if minutes > 0:
            return f"{hours} ساعة {minutes} دقيقة"
        return f"{hours} ساعة"
    else:
        days = int(seconds / 86400)
        hours = int((seconds % 86400) / 3600)
        if hours > 0:
            return f"{days} يوم {hours} ساعة"
        return f"{days} يوم"


def estimate_time(total_items: int, items_per_minute: int) -> str:
    """تقدير الوقت المتبقي"""
    total_items = safe_value(total_items, 0, int)
    items_per_minute = safe_value(items_per_minute, 0, int)
    
    if items_per_minute <= 0 or total_items <= 0:
        return "غير معروف"
    
    minutes_needed = total_items / items_per_minute
    return format_duration(minutes_needed * 60)


def get_timestamp() -> str:
    """الحصول على طابع زمني منسق"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_date() -> str:
    """الحصول على التاريخ الحالي"""
    return datetime.now().strftime('%Y-%m-%d')


def get_time() -> str:
    """الحصول على الوقت الحالي"""
    return datetime.now().strftime('%H:%M:%S')


# ============== دوال مساعدة للتنسيق ==============

def create_progress_bar(percentage: Union[int, float, None], width: int = 20) -> str:
    """
    إنشاء شريط تقدم مع معالجة القيم الفارغة
    هذه هي النسخة الوحيدة - تم حذف التكرار
    """
    # التأكد من أن النسبة رقم صحيح
    percentage = safe_value(percentage, 0, float)
    
    # التأكد من أن النسبة بين 0 و 100
    percentage = max(0.0, min(100.0, percentage))
    
    filled = int(width * percentage / 100)
    empty = width - filled
    return "█" * filled + "░" * empty


def format_bytes(size: Union[int, float, None]) -> str:
    """تنسيق حجم الملفات مع معالجة القيم الفارغة"""
    size = safe_value(size, 0, float)
    
    if size <= 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    
    return f"{size:.1f} PB"


def format_number(number: Union[int, float, None]) -> str:
    """تنسيق الأرقام مع معالجة القيم الفارغة"""
    number = safe_value(number, 0, float)
    
    if number < 1000:
        return f"{number:.0f}" if number == int(number) else f"{number:.1f}"
    elif number < 1000000:
        return f"{number/1000:.1f}K"
    else:
        return f"{number/1000000:.1f}M"


# ============== دوال مساعدة للملفات ==============

def ensure_directory(path: str) -> bool:
    """إنشاء مجلد إذا لم يكن موجوداً"""
    try:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            logger.debug(f"تم إنشاء المجلد: {path}")
        return True
    except Exception as e:
        logger.error(f"فشل إنشاء المجلد {path}: {e}")
        return False


def get_file_size(file_path: str) -> int:
    """حجم الملف بالبايت"""
    try:
        if os.path.exists(file_path):
            return os.path.getsize(file_path)
        return 0
    except Exception:
        return 0


def safe_delete_file(file_path: str) -> bool:
    """حذف ملف بأمان"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"تم حذف الملف: {file_path}")
        return True
    except Exception as e:
        logger.error(f"فشل حذف الملف {file_path}: {e}")
        return False


def get_directory_size(directory: str) -> int:
    """حجم المجلد بالبايت"""
    total = 0
    try:
        if not os.path.exists(directory):
            return 0
        
        for dirpath, dirnames, filenames in os.walk(directory):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    total += os.path.getsize(fp)
                except:
                    continue
    except Exception as e:
        logger.error(f"خطأ في حساب حجم المجلد: {e}")
    return total


# ============== دوال مساعدة للأعضاء ==============

def split_members(members: List[Dict], chunk_size: int) -> List[List[Dict]]:
    """تقسيم قائمة الأعضاء إلى أجزاء"""
    if not members:
        return []
    
    chunk_size = safe_value(chunk_size, 100, int)
    if chunk_size <= 0:
        chunk_size = 100
    
    return [members[i:i + chunk_size] for i in range(0, len(members), chunk_size)]


def get_member_display_name(member: Dict) -> str:
    """الحصول على اسم العضو للتنسيق"""
    try:
        if member.get('first_name'):
            name = member['first_name']
            if member.get('last_name'):
                name += f" {member['last_name']}"
            return name
        elif member.get('username'):
            return f"@{member['username']}"
        else:
            return str(member.get('user_id', 'غير معروف'))
    except:
        return 'غير معروف'


def is_valid_user(member: Dict) -> bool:
    """التحقق من صحة العضو"""
    try:
        if member.get('is_deleted'):
            return False
        if member.get('is_bot'):
            return False
        if not member.get('user_id'):
            return False
        return True
    except:
        return False


# ============== دوال مساعدة للتحقق ==============

async def check_admin_rights(client, chat_id: int) -> Dict:
    """التحقق من صلاحيات البوت في المجموعة"""
    try:
        me = await client.get_me()
        permissions = await client.get_permissions(chat_id, me)
        
        return {
            'is_admin': getattr(permissions, 'is_admin', False),
            'can_invite': getattr(permissions, 'invite_users', False),
            'can_add_admins': getattr(permissions, 'add_admins', False),
            'can_change_info': getattr(permissions, 'change_info', False),
            'can_delete_messages': getattr(permissions, 'delete_messages', False),
            'can_pin_messages': getattr(permissions, 'pin_messages', False),
            'can_ban_users': getattr(permissions, 'ban_users', False)
        }
    except Exception as e:
        logger.error(f"فشل التحقق من الصلاحيات: {e}")
        return {'is_admin': False}


def validate_group_id(group_id: int) -> bool:
    """التحقق من صحة معرف المجموعة"""
    if group_id is None:
        return False
    try:
        group_id = int(group_id)
    except:
        return False
    return group_id < 0


# ============== دوال مساعدة للاستئناف ==============

class ProgressTracker:
    """متتبع التقدم للعمليات الطويلة"""
    
    def __init__(self, total: int, update_interval: int = 100):
        self.total = safe_value(total, 0, int)
        self.processed = 0
        self.update_interval = update_interval
        self.start_time = None
        self.last_update = None
        self.callbacks = []
    
    def start(self):
        """بدء التتبع"""
        self.start_time = time.time()
        self.last_update = self.start_time
    
    def update(self, count: int = 1):
        """تحديث التقدم"""
        self.processed += count
        current_time = time.time()
        
        if self.processed % self.update_interval == 0:
            self._notify_callbacks()
    
    def get_progress(self) -> Dict:
        """الحصول على معلومات التقدم"""
        if self.start_time is None or self.total == 0:
            return {
                'processed': self.processed,
                'total': self.total,
                'percentage': 0,
                'remaining': 'غير معروف',
                'rate': 0
            }
        
        elapsed = time.time() - self.start_time
        
        if self.processed == 0:
            return {
                'processed': 0,
                'total': self.total,
                'percentage': 0,
                'remaining': 'غير معروف',
                'rate': 0
            }
        
        percentage = (self.processed / self.total * 100)
        rate = self.processed / elapsed if elapsed > 0 else 0
        
        if rate > 0:
            remaining_seconds = (self.total - self.processed) / rate
            remaining = format_duration(remaining_seconds)
        else:
            remaining = "غير معروف"
        
        return {
            'processed': self.processed,
            'total': self.total,
            'percentage': percentage,
            'remaining': remaining,
            'rate': rate
        }
    
    def on_progress(self, callback: Callable):
        """إضافة دالة استدعاء عند التقدم"""
        if callback not in self.callbacks:
            self.callbacks.append(callback)
    
    def _notify_callbacks(self):
        """إشعار الدوال المسجلة"""
        progress = self.get_progress()
        for callback in self.callbacks:
            try:
                callback(progress)
            except Exception as e:
                logger.error(f"خطأ في دالة الاستدعاء: {e}")
    
    def get_progress_bar(self, width: int = 20) -> str:
        """الحصول على شريط التقدم"""
        progress = self.get_progress()
        return create_progress_bar(progress['percentage'], width)


# ============== دوال مساعدة للنسخ الاحتياطي ==============

class SimpleBackupManager:
    """مدير النسخ الاحتياطي البسيط"""
    
    def __init__(self, backup_path: str = "backups"):
        self.backup_path = backup_path
        ensure_directory(backup_path)
        ensure_directory(f"{backup_path}/photos")
        ensure_directory(f"{backup_path}/media")
        ensure_directory(f"{backup_path}/messages")
        ensure_directory(f"{backup_path}/compressed")
        ensure_directory(f"{backup_path}/temp")
        ensure_directory(f"{backup_path}/metadata")
    
    def save_photo(self, group_id: int, photo_data: bytes) -> Optional[str]:
        """حفظ صورة المجموعة"""
        try:
            if not photo_data:
                return None
            photo_path = f"{self.backup_path}/photos/group_{group_id}_photo.jpg"
            with open(photo_path, 'wb') as f:
                f.write(photo_data)
            return photo_path
        except Exception as e:
            logger.error(f"فشل حفظ الصورة: {e}")
            return None
    
    def save_media(self, message_id: int, media_data: bytes, media_type: str) -> Optional[str]:
        """حفظ وسائط"""
        try:
            if not media_data:
                return None
            
            extensions = {
                'photo': 'jpg',
                'video': 'mp4',
                'document': 'file',
                'audio': 'mp3',
                'animation': 'gif',
                'sticker': 'webp'
            }
            ext = extensions.get(media_type, 'bin')
            media_path = f"{self.backup_path}/media/msg_{message_id}.{ext}"
            with open(media_path, 'wb') as f:
                f.write(media_data)
            return media_path
        except Exception as e:
            logger.error(f"فشل حفظ الوسائط: {e}")
            return None
    
    def save_message_text(self, group_id: int, message_id: int, text: str, metadata: Dict = None) -> Optional[str]:
        """حفظ نص الرسالة"""
        try:
            if not text:
                return None
            
            message_path = f"{self.backup_path}/messages/group_{group_id}_msg_{message_id}.json"
            data = {
                'message_id': message_id,
                'group_id': group_id,
                'text': text,
                'timestamp': datetime.now().isoformat(),
                'metadata': metadata or {}
            }
            with open(message_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return message_path
        except Exception as e:
            logger.error(f"فشل حفظ النص: {e}")
            return None
    
    def get_backup_size(self) -> int:
        """حجم النسخ الاحتياطية"""
        return get_directory_size(self.backup_path)
    
    def get_backup_stats(self) -> Dict:
        """إحصائيات النسخ الاحتياطية"""
        stats = {
            'total_size': self.get_backup_size(),
            'folders': {},
            'groups': {}
        }
        
        for folder_name in ['photos', 'media', 'messages', 'compressed', 'metadata']:
            folder_path = f"{self.backup_path}/{folder_name}"
            if os.path.exists(folder_path):
                size = get_directory_size(folder_path)
                try:
                    count = len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
                except:
                    count = 0
                stats['folders'][folder_name] = {
                    'size': size,
                    'count': count
                }
        
        return stats
    
    def cleanup_old_backups(self, days: int = 30) -> Dict:
        """تنظيف النسخ القديمة"""
        result = {
            'deleted_files': 0,
            'freed_space': 0,
            'errors': []
        }
        
        try:
            cutoff = time.time() - (days * 86400)
            for root, dirs, files in os.walk(self.backup_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        if os.path.getmtime(file_path) < cutoff:
                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            result['deleted_files'] += 1
                            result['freed_space'] += file_size
                    except Exception as e:
                        result['errors'].append(str(e))
            
            logger.info(f"تم تنظيف {result['deleted_files']} ملف، وفر {format_bytes(result['freed_space'])}")
        except Exception as e:
            logger.error(f"فشل تنظيف النسخ القديمة: {e}")
            result['errors'].append(str(e))
        
        return result


# ============== دوال مساعدة للإشعارات ==============

def format_notification(template: str, **kwargs) -> str:
    """تنسيق رسالة الإشعار"""
    try:
        return template.format(**kwargs)
    except Exception as e:
        logger.error(f"فشل تنسيق الإشعار: {e}")
        return template


def create_restore_report(result: Dict, duration: Union[int, float, None]) -> str:
    """إنشاء تقرير الاستعادة"""
    try:
        total = safe_value(result.get('total'), 1, int)
        restored = safe_value(result.get('restored'), 0, int)
        success_rate = (restored / total * 100) if total > 0 else 0
        
        admins_restored = safe_value(result.get('admins_restored'), 0, int)
        
        report = f"""
✅ **تم استعادة المجموعة بنجاح**

📊 **إحصائيات الاستعادة:**
👥 الأعضاء المستعادين: {format_number(restored)} / {format_number(total)}
👑 المشرفين: {format_number(admins_restored)}
📈 نسبة النجاح: {success_rate:.1f}%
⏱️ المدة: {format_duration(duration)}

🎉 تم الاستعادة بنجاح!
"""
        return report
    except Exception as e:
        logger.error(f"فشل إنشاء تقرير الاستعادة: {e}")
        return "✅ تم استعادة المجموعة بنجاح!"