"""
database.py - قاعدة البيانات المحسنة مع معالجة الأخطاء
"""

import sqlite3
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from contextlib import contextmanager
from config import PerformanceConfig

logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """استثناء مخصص لقاعدة البيانات"""
    pass

class UltimateDatabase:
    """
    قاعدة بيانات متكاملة مع:
    - معالجة الأخطاء الشاملة
    - إدارة الاتصالات التلقائية
    - تحسين الأداء للمجموعات الكبيرة
    - نظام استئناف ذكي
    """
    
    def __init__(self, db_path: str = "ultimate_backup.db"):
        self.db_path = db_path
        self.config = PerformanceConfig()
        self._init_database()
    
    @contextmanager
    def get_connection(self):
        """إدارة اتصال قاعدة البيانات مع معالجة الأخطاء"""
        conn = None
        try:
            conn = sqlite3.connect(
                self.db_path,
                timeout=getattr(self.config, 'DB_TIMEOUT', 30),
                check_same_thread=False
            )
            conn.row_factory = sqlite3.Row
            # إعدادات الأداء
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA cache_size=-2000")  # 2MB cache
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            yield conn
        except sqlite3.Error as e:
            logger.error(f"خطأ في قاعدة البيانات: {e}")
            raise DatabaseError(f"فشل الاتصال بقاعدة البيانات: {e}")
        finally:
            if conn:
                conn.close()
    
    def _init_database(self):
        """تهيئة قاعدة البيانات مع جميع الجداول والفهارس"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # جدول المجموعات (موجود)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS groups (
                        original_id INTEGER PRIMARY KEY,
                        title TEXT NOT NULL,
                        username TEXT,
                        about TEXT,
                        photo_path TEXT,
                        photo_file_id TEXT,
                        participants_count INTEGER DEFAULT 0,
                        is_megagroup BOOLEAN DEFAULT 0,
                        created_date TIMESTAMP,
                        backup_date TIMESTAMP,
                        backup_status TEXT DEFAULT 'pending',
                        total_members_backup INTEGER DEFAULT 0,
                        total_messages_backup INTEGER DEFAULT 0,
                        total_media_backup INTEGER DEFAULT 0,
                        backup_size INTEGER DEFAULT 0,
                        checkpoint INTEGER DEFAULT 0,
                        settings TEXT,
                        invite_link TEXT,
                        pinned_message_id INTEGER
                    )
                ''')
                
                # جدول الأعضاء (محسن مع إضافة restore_status)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS members (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id INTEGER NOT NULL,
                        user_id INTEGER NOT NULL,
                        username TEXT,
                        first_name TEXT,
                        last_name TEXT,
                        phone TEXT,
                        is_admin BOOLEAN DEFAULT 0,
                        is_creator BOOLEAN DEFAULT 0,
                        is_bot BOOLEAN DEFAULT 0,
                        is_deleted BOOLEAN DEFAULT 0,
                        added_status TEXT DEFAULT 'pending',
                        added_date TIMESTAMP,
                        error TEXT,
                        admin_rights TEXT,
                        join_date TIMESTAMP,
                        restore_status TEXT DEFAULT 'pending',
                        restore_date TIMESTAMP,
                        FOREIGN KEY (group_id) REFERENCES groups (original_id) ON DELETE CASCADE,
                        UNIQUE(group_id, user_id)
                    )
                ''')
                
                # إضافة الأعمدة المفقودة في الجداول الموجودة (إذا كانت موجودة)
                try:
                    # محاولة إضافة عمود restore_status إذا لم يكن موجوداً
                    cursor.execute('ALTER TABLE members ADD COLUMN restore_status TEXT DEFAULT "pending"')
                    logger.info("تم إضافة عمود restore_status")
                except sqlite3.OperationalError:
                    pass  # العمود موجود بالفعل
                
                try:
                    # محاولة إضافة عمود restore_date إذا لم يكن موجوداً
                    cursor.execute('ALTER TABLE members ADD COLUMN restore_date TIMESTAMP')
                    logger.info("تم إضافة عمود restore_date")
                except sqlite3.OperationalError:
                    pass  # العمود موجود بالفعل
                
                # باقي الجداول...
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id INTEGER NOT NULL,
                        message_id INTEGER NOT NULL,
                        sender_id INTEGER,
                        sender_name TEXT,
                        text TEXT,
                        media_type TEXT,
                        media_path TEXT,
                        media_file_id TEXT,
                        reply_to INTEGER,
                        date TIMESTAMP,
                        is_pinned BOOLEAN DEFAULT 0,
                        entities TEXT,
                        FOREIGN KEY (group_id) REFERENCES groups (original_id) ON DELETE CASCADE
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS invite_links (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id INTEGER NOT NULL,
                        link TEXT NOT NULL,
                        creator_id INTEGER,
                        usage_limit INTEGER,
                        expire_date TIMESTAMP,
                        is_permanent BOOLEAN DEFAULT 0,
                        usage_count INTEGER DEFAULT 0,
                        created_date TIMESTAMP,
                        FOREIGN KEY (group_id) REFERENCES groups (original_id) ON DELETE CASCADE
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        group_id INTEGER PRIMARY KEY,
                        operation_type TEXT,
                        processed_count INTEGER DEFAULT 0,
                        total_count INTEGER DEFAULT 0,
                        last_offset INTEGER DEFAULT 0,
                        batch_offset TEXT,
                        last_update TIMESTAMP,
                        status TEXT DEFAULT 'running',
                        details TEXT
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS operations_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id INTEGER,
                        operation_type TEXT,
                        status TEXT,
                        details TEXT,
                        progress INTEGER DEFAULT 0,
                        timestamp TIMESTAMP,
                        duration INTEGER DEFAULT 0
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS notifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id INTEGER,
                        user_id INTEGER,
                        user_type TEXT,
                        message TEXT,
                        sent BOOLEAN DEFAULT 0,
                        sent_date TIMESTAMP,
                        created_date TIMESTAMP,
                        error TEXT
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS statistics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id INTEGER,
                        date DATE,
                        members_count INTEGER,
                        messages_count INTEGER,
                        media_count INTEGER,
                        restore_success_rate REAL,
                        created_at TIMESTAMP
                    )
                ''')
                
                # إنشاء الفهارس (مع تجنب الأخطاء)
                indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_members_group ON members(group_id)",
                    "CREATE INDEX IF NOT EXISTS idx_members_status ON members(added_status)",
                    "CREATE INDEX IF NOT EXISTS idx_members_user ON members(user_id)",
                    "CREATE INDEX IF NOT EXISTS idx_members_creator ON members(is_creator)",
                    "CREATE INDEX IF NOT EXISTS idx_members_admin ON members(is_admin)",
                    "CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(group_id)",
                    "CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date)",
                    "CREATE INDEX IF NOT EXISTS idx_checkpoints_group ON checkpoints(group_id)",
                    "CREATE INDEX IF NOT EXISTS idx_logs_group ON operations_log(group_id)",
                    "CREATE INDEX IF NOT EXISTS idx_logs_date ON operations_log(timestamp)",
                    "CREATE INDEX IF NOT EXISTS idx_notifications_sent ON notifications(sent)",
                ]
                
                # إضافة فهارس restore_status فقط إذا كان العمود موجوداً
                try:
                    cursor.execute("SELECT restore_status FROM members LIMIT 1")
                    indexes.append("CREATE INDEX IF NOT EXISTS idx_members_restore_status ON members(restore_status)")
                except:
                    pass  # العمود غير موجود، تخطي الفهرس
                
                for index in indexes:
                    try:
                        cursor.execute(index)
                    except sqlite3.Error as e:
                        logger.warning(f"فشل إنشاء فهرس {index}: {e}")
                
                conn.commit()
                logger.info("✅ تم تهيئة قاعدة البيانات بنجاح")
                
        except Exception as e:
            logger.error(f"فشل تهيئة قاعدة البيانات: {e}")
            raise DatabaseError(f"فشل تهيئة قاعدة البيانات: {e}")
        
    def _create_indexes(self, cursor):
        """إنشاء الفهارس لتحسين سرعة الاستعلامات"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_members_group ON members(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_members_status ON members(added_status)",
            "CREATE INDEX IF NOT EXISTS idx_members_restore_status ON members(restore_status)",
            "CREATE INDEX IF NOT EXISTS idx_members_user ON members(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_members_creator ON members(is_creator)",
            "CREATE INDEX IF NOT EXISTS idx_members_admin ON members(is_admin)",
            "CREATE INDEX IF NOT EXISTS idx_members_bot ON members(is_bot)",
            "CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date)",
            "CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id)",
            "CREATE INDEX IF NOT EXISTS idx_checkpoints_group ON checkpoints(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_logs_group ON operations_log(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_logs_date ON operations_log(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_logs_type ON operations_log(operation_type)",
            "CREATE INDEX IF NOT EXISTS idx_notifications_sent ON notifications(sent)",
            "CREATE INDEX IF NOT EXISTS idx_statistics_group ON statistics(group_id)",
            "CREATE INDEX IF NOT EXISTS idx_statistics_date ON statistics(date)",
        ]
        
        for index in indexes:
            try:
                cursor.execute(index)
            except sqlite3.Error as e:
                logger.warning(f"فشل إنشاء فهرس {index}: {e}")
    
    # ============== عمليات المجموعات ==============
    
    def save_group(self, group_id: int, info: Dict) -> bool:
        """حفظ أو تحديث معلومات المجموعة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO groups 
                    (original_id, title, username, about, photo_path, photo_file_id,
                     participants_count, is_megagroup, created_date, backup_date,
                     backup_status, total_members_backup, total_messages_backup,
                     total_media_backup, backup_size, checkpoint, settings, invite_link,
                     pinned_message_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    group_id, 
                    info.get('title', 'Unknown'), 
                    info.get('username'), 
                    info.get('about'),
                    info.get('photo_path'), 
                    info.get('photo_file_id'), 
                    info.get('participants_count', 0),
                    info.get('is_megagroup', False), 
                    info.get('created_date'), 
                    datetime.now(),
                    info.get('backup_status', 'pending'), 
                    info.get('total_members_backup', 0),
                    info.get('total_messages_backup', 0), 
                    info.get('total_media_backup', 0),
                    info.get('backup_size', 0), 
                    info.get('checkpoint', 0),
                    json.dumps(info.get('settings', {})), 
                    info.get('invite_link'),
                    info.get('pinned_message_id')
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل حفظ المجموعة {group_id}: {e}")
            return False
    
    def get_group(self, group_id: int) -> Optional[Dict]:
        """استرجاع معلومات المجموعة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM groups WHERE original_id = ?', (group_id,))
                row = cursor.fetchone()
                if row:
                    result = dict(row)
                    if result.get('settings'):
                        try:
                            result['settings'] = json.loads(result['settings'])
                        except:
                            result['settings'] = {}
                    return result
                return None
        except Exception as e:
            logger.error(f"فشل استرجاع المجموعة {group_id}: {e}")
            return None
    
    def get_all_groups(self, status: str = None) -> List[Dict]:
        """استرجاع جميع المجموعات"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if status:
                    cursor.execute('SELECT * FROM groups WHERE backup_status = ?', (status,))
                else:
                    cursor.execute('SELECT * FROM groups')
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"فشل استرجاع المجموعات: {e}")
            return []
    
    def update_group_status(self, group_id: int, status: str, progress: int = None) -> bool:
        """تحديث حالة المجموعة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if progress is not None:
                    cursor.execute('''
                        UPDATE groups 
                        SET backup_status = ?, checkpoint = ?
                        WHERE original_id = ?
                    ''', (status, progress, group_id))
                else:
                    cursor.execute('''
                        UPDATE groups 
                        SET backup_status = ?
                        WHERE original_id = ?
                    ''', (status, group_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل تحديث حالة المجموعة: {e}")
            return False
    
    def delete_group(self, group_id: int) -> bool:
        """حذف مجموعة وجميع بياناتها المرتبطة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM groups WHERE original_id = ?', (group_id,))
                # الأعضاء والرسائل سيتم حذفها تلقائياً بسبب ON DELETE CASCADE
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل حذف المجموعة {group_id}: {e}")
            return False
    
    # ============== عمليات الأعضاء ==============
        
    def save_members_batch(self, group_id: int, members: List[Dict]) -> int:
        """حفظ دفعة من الأعضاء بكفاءة عالية"""
        if not members:
            return 0
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                data = []
                for member in members:
                    data.append((
                        group_id,
                        member.get('user_id'),
                        member.get('username'),
                        member.get('first_name'),
                        member.get('last_name'),
                        member.get('phone'),
                        # تأكد من تحويل القيم إلى 0 أو 1
                        1 if member.get('is_admin', False) else 0,
                        1 if member.get('is_creator', False) else 0,
                        1 if member.get('is_bot', False) else 0,
                        1 if member.get('is_deleted', False) else 0,
                        'pending',
                        None,
                        None,
                        json.dumps(member.get('admin_rights', {})),
                        member.get('join_date', datetime.now()),
                        'pending',
                        None
                    ))
                
                cursor.executemany('''
                    INSERT OR IGNORE INTO members 
                    (group_id, user_id, username, first_name, last_name, phone,
                    is_admin, is_creator, is_bot, is_deleted, added_status,
                    added_date, error, admin_rights, join_date, restore_status, restore_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data)
                
                conn.commit()
                return len(data)
                
        except Exception as e:
            logger.error(f"فشل حفظ دفعة الأعضاء: {e}")
            return 0
    
    def get_pending_members(self, group_id: int, limit: int = 1000) -> List[Dict]:
        """استرجاع الأعضاء المعلقين"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM members 
                    WHERE group_id = ? AND added_status = 'pending' AND is_deleted = 0
                    ORDER BY is_creator DESC, is_admin DESC, user_id
                    LIMIT ?
                ''', (group_id, limit))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"فشل استرجاع الأعضاء المعلقين: {e}")
            return []
    
    def get_members_for_restore(self, group_id: int, limit: int = 1000) -> List[Dict]:
        """استرجاع الأعضاء للاستعادة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM members 
                    WHERE group_id = ? AND restore_status = 'pending' AND is_deleted = 0
                    ORDER BY is_creator DESC, is_admin DESC, user_id
                    LIMIT ?
                ''', (group_id, limit))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"فشل استرجاع الأعضاء للاستعادة: {e}")
            return []
    
    def update_member_status(self, group_id: int, user_id: int, status: str, error: str = None) -> bool:
        """تحديث حالة عضو"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE members 
                    SET added_status = ?, added_date = ?, error = ?
                    WHERE group_id = ? AND user_id = ?
                ''', (status, datetime.now() if status == 'added' else None, error, group_id, user_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل تحديث حالة العضو: {e}")
            return False
    
    def update_member_restore_status(self, group_id: int, user_id: int, status: str, error: str = None) -> bool:
        """تحديث حالة استعادة عضو"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE members 
                    SET restore_status = ?, restore_date = ?, error = ?
                    WHERE group_id = ? AND user_id = ?
                ''', (status, datetime.now() if status == 'added' else None, error, group_id, user_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل تحديث حالة استعادة العضو: {e}")
            return False
    
    def update_members_batch_status(self, group_id: int, user_ids: List[int], status: str) -> int:
        """تحديث حالة دفعة من الأعضاء"""
        if not user_ids:
            return 0
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                placeholders = ','.join(['?'] * len(user_ids))
                cursor.execute(f'''
                    UPDATE members 
                    SET added_status = ?, added_date = ?
                    WHERE group_id = ? AND user_id IN ({placeholders})
                ''', (status, datetime.now() if status == 'added' else None, group_id, *user_ids))
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            logger.error(f"فشل تحديث حالة الدفعة: {e}")
            return 0
    
    def update_members_batch_restore_status(self, group_id: int, user_ids: List[int], status: str) -> int:
        """تحديث حالة استعادة دفعة من الأعضاء"""
        if not user_ids:
            return 0
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                placeholders = ','.join(['?'] * len(user_ids))
                cursor.execute(f'''
                    UPDATE members 
                    SET restore_status = ?, restore_date = ?
                    WHERE group_id = ? AND user_id IN ({placeholders})
                ''', (status, datetime.now() if status == 'added' else None, group_id, *user_ids))
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            logger.error(f"فشل تحديث حالة استعادة الدفعة: {e}")
            return 0
    
    def get_members_count(self, group_id: int) -> int:
        """الحصول على عدد الأعضاء في مجموعة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM members WHERE group_id = ?', (group_id,))
                result = cursor.fetchone()
                return result[0] if result and result[0] is not None else 0
        except Exception as e:
            logger.error(f"خطأ في get_members_count: {e}")
            return 0
    
    def get_restored_members_count(self, group_id: int) -> int:
        """الحصول على عدد الأعضاء المستعادين"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM members WHERE group_id = ? AND restore_status = "added"', (group_id,))
                result = cursor.fetchone()
                return result[0] if result and result[0] is not None else 0
        except Exception as e:
            logger.error(f"خطأ في get_restored_members_count: {e}")
            return 0
    
    def get_members_stats(self, group_id: int) -> Dict:
        """الحصول على إحصائيات الأعضاء"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # إجمالي الأعضاء
                cursor.execute('SELECT COUNT(*) FROM members WHERE group_id = ?', (group_id,))
                total = cursor.fetchone()[0] or 0
                
                # المشرفين
                cursor.execute('SELECT COUNT(*) FROM members WHERE group_id = ? AND is_admin = 1', (group_id,))
                admins = cursor.fetchone()[0] or 0
                
                # المالكين
                cursor.execute('SELECT COUNT(*) FROM members WHERE group_id = ? AND is_creator = 1', (group_id,))
                creators = cursor.fetchone()[0] or 0
                
                # البوتات
                cursor.execute('SELECT COUNT(*) FROM members WHERE group_id = ? AND is_bot = 1', (group_id,))
                bots = cursor.fetchone()[0] or 0
                
                # حالة الاستعادة
                cursor.execute('SELECT COUNT(*) FROM members WHERE group_id = ? AND restore_status = "added"', (group_id,))
                added = cursor.fetchone()[0] or 0
                
                cursor.execute('SELECT COUNT(*) FROM members WHERE group_id = ? AND restore_status = "failed"', (group_id,))
                failed = cursor.fetchone()[0] or 0
                
                cursor.execute('SELECT COUNT(*) FROM members WHERE group_id = ? AND restore_status = "pending"', (group_id,))
                pending = cursor.fetchone()[0] or 0
                
                return {
                    'total': total,
                    'admins': admins,
                    'creators': creators,
                    'bots': bots,
                    'added': added,
                    'failed': failed,
                    'pending': pending
                }
        except Exception as e:
            logger.error(f"خطأ في get_members_stats: {e}")
            return {
                'total': 0, 'admins': 0, 'creators': 0, 'bots': 0,
                'added': 0, 'failed': 0, 'pending': 0
            }
    
    # ============== عمليات الرسائل ==============
    
    def save_message(self, group_id: int, message: Dict) -> bool:
        """حفظ رسالة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO messages 
                    (group_id, message_id, sender_id, sender_name, text, media_type,
                     media_path, media_file_id, reply_to, date, is_pinned, entities)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    group_id, 
                    message.get('message_id'), 
                    message.get('sender_id'),
                    message.get('sender_name'), 
                    message.get('text'), 
                    message.get('media_type'),
                    message.get('media_path'), 
                    message.get('media_file_id'), 
                    message.get('reply_to'),
                    message.get('date'), 
                    message.get('is_pinned', False),
                    json.dumps(message.get('entities', []))
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل حفظ الرسالة: {e}")
            return False
    
    def get_messages_count(self, group_id: int) -> int:
        """الحصول على عدد الرسائل في مجموعة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM messages WHERE group_id = ?', (group_id,))
                result = cursor.fetchone()
                return result[0] if result and result[0] is not None else 0
        except Exception as e:
            logger.error(f"خطأ في get_messages_count: {e}")
            return 0
    
    # ============== عمليات نقاط التوقف ==============
    
    def save_checkpoint(self, group_id: int, operation: str, processed: int, total: int, offset: int = None, batch_offset: str = None) -> bool:
        """حفظ نقطة توقف"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO checkpoints 
                    (group_id, operation_type, processed_count, total_count, last_offset, batch_offset, last_update, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    group_id, 
                    operation, 
                    processed, 
                    total, 
                    offset or processed, 
                    batch_offset, 
                    datetime.now(), 
                    'running'
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل حفظ نقطة التوقف: {e}")
            return False
    
    def get_checkpoint(self, group_id: int, operation: str = None) -> Optional[Dict]:
        """استرجاع نقطة التوقف"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if operation:
                    cursor.execute(
                        'SELECT * FROM checkpoints WHERE group_id = ? AND operation_type = ?',
                        (group_id, operation)
                    )
                else:
                    cursor.execute(
                        'SELECT * FROM checkpoints WHERE group_id = ?',
                        (group_id,)
                    )
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"فشل استرجاع نقطة التوقف: {e}")
            return None
    
    def update_checkpoint_status(self, group_id: int, status: str, details: str = None) -> bool:
        """تحديث حالة نقطة التوقف"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE checkpoints 
                    SET status = ?, details = ?
                    WHERE group_id = ?
                ''', (status, details, group_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل تحديث حالة نقطة التوقف: {e}")
            return False
    
    def delete_checkpoint(self, group_id: int) -> bool:
        """حذف نقطة توقف"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM checkpoints WHERE group_id = ?', (group_id,))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل حذف نقطة التوقف: {e}")
            return False
    
    # ============== عمليات التسجيل ==============
    
    def log_operation(self, group_id: int, operation: str, status: str, details: str = None, progress: int = None, duration: int = None) -> bool:
        """تسجيل عملية"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO operations_log 
                    (group_id, operation_type, status, details, progress, timestamp, duration)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (group_id, operation, status, details, progress, datetime.now(), duration))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل تسجيل العملية: {e}")
            return False
    
    def get_operation_logs(self, group_id: int = None, limit: int = 100) -> List[Dict]:
        """استرجاع سجل العمليات"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if group_id:
                    cursor.execute('''
                        SELECT * FROM operations_log 
                        WHERE group_id = ? 
                        ORDER BY timestamp DESC 
                        LIMIT ?
                    ''', (group_id, limit))
                else:
                    cursor.execute('''
                        SELECT * FROM operations_log 
                        ORDER BY timestamp DESC 
                        LIMIT ?
                    ''', (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"فشل استرجاع سجل العمليات: {e}")
            return []
    
    # ============== عمليات الإشعارات ==============
    
    def add_notification(self, group_id: int, user_id: int, user_type: str, message: str) -> bool:
        """إضافة إشعار"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO notifications 
                    (group_id, user_id, user_type, message, created_date)
                    VALUES (?, ?, ?, ?, ?)
                ''', (group_id, user_id, user_type, message, datetime.now()))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل إضافة الإشعار: {e}")
            return False
    
    def get_pending_notifications(self, limit: int = 100) -> List[Dict]:
        """استرجاع الإشعارات المعلقة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM notifications 
                    WHERE sent = 0 
                    ORDER BY created_date 
                    LIMIT ?
                ''', (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"فشل استرجاع الإشعارات المعلقة: {e}")
            return []
    
    def mark_notification_sent(self, notification_id: int, error: str = None) -> bool:
        """تحديث حالة إرسال الإشعار"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE notifications 
                    SET sent = 1, sent_date = ?, error = ?
                    WHERE id = ?
                ''', (datetime.now(), error, notification_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل تحديث حالة الإشعار: {e}")
            return False
    
    # ============== عمليات الإحصائيات ==============
    
    def get_total_stats(self) -> Dict:
        """الحصول على إحصائيات عامة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # عدد المجموعات
                cursor.execute('SELECT COUNT(*) FROM groups WHERE backup_status = "completed"')
                groups_count = cursor.fetchone()[0] or 0
                
                # إجمالي الأعضاء
                cursor.execute('SELECT SUM(total_members_backup) FROM groups WHERE backup_status = "completed"')
                members_sum = cursor.fetchone()[0]
                members_count = members_sum if members_sum is not None else 0
                
                # حجم النسخ
                cursor.execute('SELECT SUM(backup_size) FROM groups WHERE backup_status = "completed"')
                size_sum = cursor.fetchone()[0]
                backup_size = size_sum if size_sum is not None else 0
                
                return {
                    'groups': groups_count,
                    'members': members_count,
                    'backup_size_mb': backup_size / (1024 * 1024) if backup_size else 0
                }
        except Exception as e:
            logger.error(f"خطأ في get_total_stats: {e}")
            return {'groups': 0, 'members': 0, 'backup_size_mb': 0}
    
    def save_statistics(self, group_id: int, stats: Dict) -> bool:
        """حفظ إحصائيات يومية"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO statistics 
                    (group_id, date, members_count, messages_count, media_count, restore_success_rate, created_at)
                    VALUES (?, DATE('now'), ?, ?, ?, ?, ?)
                ''', (
                    group_id,
                    stats.get('members_count', 0),
                    stats.get('messages_count', 0),
                    stats.get('media_count', 0),
                    stats.get('restore_success_rate', 0),
                    datetime.now()
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"فشل حفظ الإحصائيات: {e}")
            return False
    
    def get_group_statistics(self, group_id: int, days: int = 30) -> List[Dict]:
        """استرجاع إحصائيات المجموعة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM statistics 
                    WHERE group_id = ? AND date >= DATE('now', ?)
                    ORDER BY date DESC
                ''', (group_id, f'-{days} days'))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"فشل استرجاع الإحصائيات: {e}")
            return []
    
    # ============== عمليات التنظيف ==============
    
    def cleanup_old_logs(self, days: int = 30) -> int:
        """تنظيف سجل العمليات القديم"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    DELETE FROM operations_log 
                    WHERE timestamp < datetime('now', ?)
                ''', (f'-{days} days',))
                deleted = cursor.rowcount
                conn.commit()
                logger.info(f"تم تنظيف {deleted} سجل قديم")
                return deleted
        except Exception as e:
            logger.error(f"فشل تنظيف السجلات القديمة: {e}")
            return 0
    
    def cleanup_old_notifications(self, days: int = 7) -> int:
        """تنظيف الإشعارات القديمة"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    DELETE FROM notifications 
                    WHERE created_date < datetime('now', ?) AND sent = 1
                ''', (f'-{days} days',))
                deleted = cursor.rowcount
                conn.commit()
                return deleted
        except Exception as e:
            logger.error(f"فشل تنظيف الإشعارات القديمة: {e}")
            return 0
    
    def optimize_database(self) -> bool:
        """تحسين قاعدة البيانات"""
        try:
            with self.get_connection() as conn:
                conn.execute("VACUUM")
                conn.execute("ANALYZE")
                logger.info("✅ تم تحسين قاعدة البيانات")
                return True
        except Exception as e:
            logger.error(f"فشل تحسين قاعدة البيانات: {e}")
            return False
    
    def close(self):
        """إغلاق قاعدة البيانات"""
        logger.info("تم إغلاق قاعدة البيانات")