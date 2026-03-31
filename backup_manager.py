"""
backup_manager.py - مدير النسخ الاحتياطي المتكامل
مع إدارة الملفات والوسائط والضغط والتشفير
"""

import os
import shutil
import json
import zipfile
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import asyncio

from config import PerformanceConfig, SECURITY_SETTINGS
from utils import ensure_directory, get_file_size, format_bytes, safe_delete_file

logger = logging.getLogger(__name__)

class BackupManager:
    """
    مدير متكامل للنسخ الاحتياطية
    يدعم:
    - حفظ الصور والوسائط والرسائل
    - ضغط الملفات
    - تشفير البيانات الحساسة
    - تنظيف النسخ القديمة
    - استعادة الملفات
    """
    
    def __init__(self, backup_path: str = "backups"):
        self.backup_path = backup_path
        self.config = PerformanceConfig()
        
        # إنشاء هيكل المجلدات
        self.structure = {
            'photos': f"{backup_path}/photos",
            'media': f"{backup_path}/media",
            'messages': f"{backup_path}/messages",
            'compressed': f"{backup_path}/compressed",
            'temp': f"{backup_path}/temp",
            'metadata': f"{backup_path}/metadata"
        }
        
        # إنشاء جميع المجلدات
        for folder in self.structure.values():
            ensure_directory(folder)
        
        logger.info("✅ تم تهيئة مدير النسخ الاحتياطي")
    
    # ============== حفظ الملفات ==============
    
    def save_photo(self, group_id: int, photo_data: bytes, format: str = 'jpg') -> Optional[str]:
        """حفظ صورة المجموعة"""
        try:
            if not photo_data:
                logger.warning(f"لا توجد بيانات صورة للمجموعة {group_id}")
                return None
            
            photo_path = f"{self.structure['photos']}/group_{group_id}_photo.{format}"
            
            with open(photo_path, 'wb') as f:
                f.write(photo_data)
            
            logger.info(f"✅ تم حفظ صورة المجموعة {group_id}")
            return photo_path
            
        except Exception as e:
            logger.error(f"فشل حفظ الصورة للمجموعة {group_id}: {e}")
            return None
    
    def save_media(self, message_id: int, media_data: bytes, media_type: str, 
                   extension: str = None) -> Optional[str]:
        """حفظ وسائط (صورة، فيديو، مستند)"""
        try:
            if not media_data:
                return None
            
            # تحديد الامتداد المناسب
            if not extension:
                extensions = {
                    'photo': 'jpg',
                    'video': 'mp4',
                    'document': 'file',
                    'audio': 'mp3',
                    'animation': 'gif',
                    'sticker': 'webp'
                }
                extension = extensions.get(media_type, 'bin')
            
            # إنشاء اسم ملف فريد
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            media_path = f"{self.structure['media']}/msg_{message_id}_{timestamp}.{extension}"
            
            with open(media_path, 'wb') as f:
                f.write(media_data)
            
            logger.info(f"✅ تم حفظ وسائط للرسالة {message_id}")
            return media_path
            
        except Exception as e:
            logger.error(f"فشل حفظ الوسائط للرسالة {message_id}: {e}")
            return None
    
    def save_message_text(self, group_id: int, message_id: int, 
                          text: str, metadata: Dict = None) -> Optional[str]:
        """حفظ نص الرسالة مع بيانات وصفية"""
        try:
            if not text:
                return None
            
            message_path = f"{self.structure['messages']}/group_{group_id}_msg_{message_id}.json"
            
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
            logger.error(f"فشل حفظ نص الرسالة {message_id}: {e}")
            return None
    
    def save_group_metadata(self, group_id: int, metadata: Dict) -> Optional[str]:
        """حفظ بيانات وصفية للمجموعة"""
        try:
            if not metadata:
                return None
            
            metadata_path = f"{self.structure['metadata']}/group_{group_id}_metadata.json"
            
            # إضافة معلومات إضافية
            metadata['last_backup'] = datetime.now().isoformat()
            metadata['backup_size'] = self.get_group_backup_size(group_id)
            
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ تم حفظ البيانات الوصفية للمجموعة {group_id}")
            return metadata_path
            
        except Exception as e:
            logger.error(f"فشل حفظ البيانات الوصفية للمجموعة {group_id}: {e}")
            return None
    
    # ============== استعادة الملفات ==============
    
    def load_photo(self, group_id: int) -> Optional[bytes]:
        """استعادة صورة المجموعة"""
        try:
            photo_pattern = f"group_{group_id}_photo"
            photos_dir = self.structure['photos']
            
            if not os.path.exists(photos_dir):
                return None
            
            for filename in os.listdir(photos_dir):
                if filename.startswith(photo_pattern):
                    photo_path = os.path.join(photos_dir, filename)
                    if os.path.exists(photo_path):
                        with open(photo_path, 'rb') as f:
                            return f.read()
            
            return None
            
        except Exception as e:
            logger.error(f"فشل استعادة صورة المجموعة {group_id}: {e}")
            return None
    
    def load_group_metadata(self, group_id: int) -> Optional[Dict]:
        """استعادة البيانات الوصفية للمجموعة"""
        try:
            metadata_path = f"{self.structure['metadata']}/group_{group_id}_metadata.json"
            
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            return None
            
        except Exception as e:
            logger.error(f"فشل استعادة البيانات الوصفية للمجموعة {group_id}: {e}")
            return None
    
    def get_group_media_files(self, group_id: int) -> List[str]:
        """الحصول على قائمة ملفات الوسائط لمجموعة محددة"""
        media_files = []
        try:
            # البحث في مجلد الوسائط
            media_dir = self.structure['media']
            if os.path.exists(media_dir):
                for filename in os.listdir(media_dir):
                    if f"group_{group_id}" in filename:
                        file_path = os.path.join(media_dir, filename)
                        if os.path.exists(file_path):
                            media_files.append(file_path)
            
            # البحث في مجلد الرسائل
            messages_dir = self.structure['messages']
            if os.path.exists(messages_dir):
                for filename in os.listdir(messages_dir):
                    if f"group_{group_id}" in filename:
                        file_path = os.path.join(messages_dir, filename)
                        if os.path.exists(file_path):
                            media_files.append(file_path)
                    
        except Exception as e:
            logger.error(f"فشل استرجاع ملفات الوسائط للمجموعة {group_id}: {e}")
        
        return media_files
    
    # ============== إدارة الضغط ==============
    
    def compress_backup(self, group_id: int, output_name: str = None) -> Optional[str]:
        """ضغط النسخة الاحتياطية لمجموعة محددة"""
        try:
            if not output_name:
                output_name = f"group_{group_id}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            zip_path = f"{self.structure['compressed']}/{output_name}.zip"
            
            # التأكد من وجود المجلد
            ensure_directory(os.path.dirname(zip_path))
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # إضافة الصور
                photos_dir = self.structure['photos']
                if os.path.exists(photos_dir):
                    for filename in os.listdir(photos_dir):
                        if str(group_id) in filename:
                            file_path = os.path.join(photos_dir, filename)
                            if os.path.exists(file_path):
                                zipf.write(file_path, f"photos/{filename}")
                
                # إضافة الوسائط
                media_dir = self.structure['media']
                if os.path.exists(media_dir):
                    for filename in os.listdir(media_dir):
                        if str(group_id) in filename:
                            file_path = os.path.join(media_dir, filename)
                            if os.path.exists(file_path):
                                zipf.write(file_path, f"media/{filename}")
                
                # إضافة الرسائل
                messages_dir = self.structure['messages']
                if os.path.exists(messages_dir):
                    for filename in os.listdir(messages_dir):
                        if str(group_id) in filename:
                            file_path = os.path.join(messages_dir, filename)
                            if os.path.exists(file_path):
                                zipf.write(file_path, f"messages/{filename}")
                
                # إضافة البيانات الوصفية
                metadata_path = f"{self.structure['metadata']}/group_{group_id}_metadata.json"
                if os.path.exists(metadata_path):
                    zipf.write(metadata_path, "metadata.json")
            
            logger.info(f"✅ تم ضغط النسخة الاحتياطية للمجموعة {group_id}: {zip_path}")
            return zip_path
            
        except Exception as e:
            logger.error(f"فشل ضغط النسخة الاحتياطية للمجموعة {group_id}: {e}")
            return None
    
    def extract_backup(self, zip_path: str, extract_to: str = None) -> bool:
        """فك ضغط نسخة احتياطية"""
        try:
            if not os.path.exists(zip_path):
                logger.error(f"الملف غير موجود: {zip_path}")
                return False
            
            if not extract_to:
                extract_to = self.structure['temp']
            
            ensure_directory(extract_to)
            
            with zipfile.ZipFile(zip_path, 'r') as zipf:
                zipf.extractall(extract_to)
            
            logger.info(f"✅ تم فك ضغط النسخة الاحتياطية: {zip_path}")
            return True
            
        except Exception as e:
            logger.error(f"فشل فك ضغط النسخة الاحتياطية: {e}")
            return False
    
    # ============== إدارة التشفير ==============
    
    def encrypt_backup(self, file_path: str, password: str) -> Optional[str]:
        """تشفير ملف النسخة الاحتياطية"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"الملف غير موجود: {file_path}")
                return None
            
            if not SECURITY_SETTINGS.get('encrypt_backup', False):
                logger.warning("التشفير غير مفعل في الإعدادات")
                return None
            
            # محاولة استيراد cryptography
            try:
                from cryptography.fernet import Fernet
                from cryptography.hazmat.primitives import hashes
                from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
                import base64
            except ImportError:
                logger.error("مكتبة cryptography غير مثبتة. قم بتثبيتها: pip install cryptography")
                return None
            
            # إنشاء مفتاح من كلمة المرور
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'telegram_backup_salt',
                iterations=100000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
            
            # قراءة الملف
            with open(file_path, 'rb') as f:
                data = f.read()
            
            # تشفير
            fernet = Fernet(key)
            encrypted_data = fernet.encrypt(data)
            
            # حفظ الملف المشفر
            encrypted_path = f"{file_path}.encrypted"
            with open(encrypted_path, 'wb') as f:
                f.write(encrypted_data)
            
            # حذف الملف الأصلي
            os.remove(file_path)
            
            logger.info(f"✅ تم تشفير النسخة الاحتياطية: {encrypted_path}")
            return encrypted_path
            
        except Exception as e:
            logger.error(f"فشل تشفير النسخة الاحتياطية: {e}")
            return None
    
    def decrypt_backup(self, encrypted_path: str, password: str) -> Optional[str]:
        """فك تشفير ملف النسخة الاحتياطية"""
        try:
            if not os.path.exists(encrypted_path):
                logger.error(f"الملف غير موجود: {encrypted_path}")
                return None
            
            try:
                from cryptography.fernet import Fernet
                from cryptography.hazmat.primitives import hashes
                from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
                import base64
            except ImportError:
                logger.error("مكتبة cryptography غير مثبتة")
                return None
            
            # إنشاء مفتاح من كلمة المرور
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'telegram_backup_salt',
                iterations=100000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
            
            # قراءة الملف المشفر
            with open(encrypted_path, 'rb') as f:
                encrypted_data = f.read()
            
            # فك التشفير
            fernet = Fernet(key)
            decrypted_data = fernet.decrypt(encrypted_data)
            
            # حفظ الملف المفكوك
            decrypted_path = encrypted_path.replace('.encrypted', '')
            with open(decrypted_path, 'wb') as f:
                f.write(decrypted_data)
            
            logger.info(f"✅ تم فك تشفير النسخة الاحتياطية: {decrypted_path}")
            return decrypted_path
            
        except Exception as e:
            logger.error(f"فشل فك تشفير النسخة الاحتياطية: {e}")
            return None
    
    # ============== إدارة حجم النسخ ==============
    
    def get_backup_size(self) -> int:
        """الحصول على حجم النسخ الاحتياطية"""
        try:
            total_size = 0
            backup_path = "backups"
            
            if not os.path.exists(backup_path):
                return 0
            
            for root, dirs, files in os.walk(backup_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        if os.path.exists(file_path):
                            total_size += os.path.getsize(file_path)
                    except:
                        continue
            
            return total_size if total_size is not None else 0
        except Exception as e:
            logger.error(f"خطأ في get_backup_size: {e}")
            return 0
    
    def get_group_backup_size(self, group_id: int) -> int:
        """حجم النسخ الاحتياطية لمجموعة محددة"""
        try:
            total = 0
            
            # البحث في جميع المجلدات
            for folder_path in self.structure.values():
                if os.path.exists(folder_path):
                    for filename in os.listdir(folder_path):
                        if str(group_id) in filename:
                            file_path = os.path.join(folder_path, filename)
                            if os.path.exists(file_path):
                                total += os.path.getsize(file_path)
            
            return total if total is not None else 0
        except Exception as e:
            logger.error(f"خطأ في get_group_backup_size: {e}")
            return 0
    
    def _get_folder_size(self, folder_path: str) -> int:
        """حساب حجم المجلد"""
        try:
            total = 0
            if not os.path.exists(folder_path):
                return 0
            
            for dirpath, dirnames, filenames in os.walk(folder_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    try:
                        if os.path.exists(fp):
                            total += os.path.getsize(fp)
                    except:
                        continue
            return total
        except Exception as e:
            logger.error(f"خطأ في _get_folder_size: {e}")
            return 0
    
    # ============== تنظيف النسخ القديمة ==============
    
    def cleanup_old_backups(self, days: int = 30, group_id: int = None) -> Dict:
        """تنظيف النسخ الاحتياطية القديمة"""
        results = {
            'deleted_files': 0,
            'freed_space': 0,
            'errors': []
        }
        
        try:
            cutoff_time = datetime.now() - timedelta(days=days)
            
            for folder_name, folder_path in self.structure.items():
                if not os.path.exists(folder_path):
                    continue
                
                for filename in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, filename)
                    
                    # تصفية حسب المجموعة إذا تم تحديدها
                    if group_id and str(group_id) not in filename:
                        continue
                    
                    try:
                        # الحصول على وقت آخر تعديل
                        if os.path.exists(file_path):
                            mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                            
                            if mtime < cutoff_time:
                                file_size = os.path.getsize(file_path)
                                os.remove(file_path)
                                
                                results['deleted_files'] += 1
                                results['freed_space'] += file_size
                                
                    except Exception as e:
                        results['errors'].append(f"{filename}: {e}")
            
            logger.info(f"✅ تم تنظيف {results['deleted_files']} ملف قديم، وفر {format_bytes(results['freed_space'])}")
            
        except Exception as e:
            logger.error(f"فشل تنظيف النسخ القديمة: {e}")
            results['errors'].append(str(e))
        
        return results
    
    def cleanup_by_group(self, group_id: int) -> Dict:
        """حذف جميع النسخ الاحتياطية لمجموعة محددة"""
        results = {
            'deleted_files': 0,
            'freed_space': 0,
            'errors': []
        }
        
        try:
            for folder_name, folder_path in self.structure.items():
                if not os.path.exists(folder_path):
                    continue
                
                for filename in os.listdir(folder_path):
                    if str(group_id) in filename:
                        file_path = os.path.join(folder_path, filename)
                        try:
                            if os.path.exists(file_path):
                                file_size = os.path.getsize(file_path)
                                os.remove(file_path)
                                
                                results['deleted_files'] += 1
                                results['freed_space'] += file_size
                            
                        except Exception as e:
                            results['errors'].append(f"{filename}: {e}")
            
            logger.info(f"✅ تم حذف {results['deleted_files']} ملف للمجموعة {group_id}")
            
        except Exception as e:
            logger.error(f"فشل تنظيف المجموعة {group_id}: {e}")
            results['errors'].append(str(e))
        
        return results
    
    # ============== إدارة التحقق ==============
    
    def verify_backup_integrity(self, group_id: int) -> Dict:
        """التحقق من سلامة النسخة الاحتياطية"""
        results = {
            'valid': True,
            'files_checked': 0,
            'corrupted_files': [],
            'missing_files': []
        }
        
        try:
            # التحقق من الصورة
            photo_exists = False
            photos_dir = self.structure['photos']
            if os.path.exists(photos_dir):
                for filename in os.listdir(photos_dir):
                    if str(group_id) in filename:
                        photo_exists = True
                        results['files_checked'] += 1
                        # التحقق من صحة الصورة
                        if not self._verify_image(os.path.join(photos_dir, filename)):
                            results['corrupted_files'].append(filename)
            
            if not photo_exists:
                results['missing_files'].append('photo')
            
            # التحقق من البيانات الوصفية
            metadata_path = f"{self.structure['metadata']}/group_{group_id}_metadata.json"
            if os.path.exists(metadata_path):
                results['files_checked'] += 1
                if not self._verify_json(metadata_path):
                    results['corrupted_files'].append('metadata.json')
            else:
                results['missing_files'].append('metadata')
            
            results['valid'] = len(results['corrupted_files']) == 0
            
        except Exception as e:
            logger.error(f"فشل التحقق من سلامة النسخة: {e}")
            results['valid'] = False
        
        return results
    
    def _verify_image(self, image_path: str) -> bool:
        """التحقق من صحة صورة"""
        try:
            # التحقق من وجود الملف وحجمه
            if not os.path.exists(image_path) or os.path.getsize(image_path) == 0:
                return False
            
            # محاولة استخدام PIL إذا كانت متوفرة
            try:
                from PIL import Image
                with Image.open(image_path) as img:
                    img.verify()
                return True
            except ImportError:
                # إذا لم تكن PIL متوفرة، فقط تحقق من وجود الملف
                return True
            except:
                return False
        except:
            return False
    
    def _verify_json(self, json_path: str) -> bool:
        """التحقق من صحة ملف JSON"""
        try:
            if not os.path.exists(json_path) or os.path.getsize(json_path) == 0:
                return False
            
            with open(json_path, 'r', encoding='utf-8') as f:
                json.load(f)
            return True
        except:
            return False
    
    # ============== إدارة التقارير ==============
    
    def generate_backup_report(self, group_id: int = None) -> str:
        """إنشاء تقرير مفصل عن النسخ الاحتياطية"""
        report = []
        report.append("=" * 50)
        report.append("📊 تقرير النسخ الاحتياطية")
        report.append("=" * 50)
        report.append(f"📅 التاريخ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # إحصائيات عامة
        total_size = self.get_backup_size()
        report.append("📦 **إحصائيات عامة:**")
        report.append(f"• الحجم الإجمالي: {format_bytes(total_size)}")
        
        # إحصائيات لكل مجلد
        for folder_name, folder_path in self.structure.items():
            if os.path.exists(folder_path):
                folder_size = self._get_folder_size(folder_path)
                try:
                    file_count = len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
                    report.append(f"• {folder_name}: {file_count} ملف, {format_bytes(folder_size)}")
                except:
                    report.append(f"• {folder_name}: {format_bytes(folder_size)}")
        
        report.append("")
        
        # تفاصيل المجموعة إذا تم تحديدها
        if group_id:
            group_size = self.get_group_backup_size(group_id)
            report.append(f"📛 **المجموعة {group_id}:**")
            report.append(f"• حجم النسخ: {format_bytes(group_size)}")
            
            # الملفات الخاصة بالمجموعة
            for folder_name, folder_path in self.structure.items():
                if os.path.exists(folder_path):
                    try:
                        group_files = [f for f in os.listdir(folder_path) if str(group_id) in f]
                        if group_files:
                            report.append(f"• {folder_name}: {len(group_files)} ملف")
                    except:
                        pass
        
        report.append("")
        report.append("=" * 50)
        
        return "\n".join(report)
    
    # ============== إدارة النسخ الاحتياطي المجدول ==============
    
    async def scheduled_cleanup(self, days: int = 30, interval_hours: int = 24):
        """تنظيف مجدول للنسخ القديمة"""
        while True:
            try:
                logger.info(f"🔄 بدء التنظيف المجدول للنسخ الأقدم من {days} يوم")
                result = self.cleanup_old_backups(days)
                logger.info(f"✅ اكتمل التنظيف المجدول: حذف {result['deleted_files']} ملف")
            except Exception as e:
                logger.error(f"خطأ في التنظيف المجدول: {e}")
            
            await asyncio.sleep(interval_hours * 3600)
    
    # ============== دوال مساعدة إضافية ==============
    
    def get_backup_stats(self) -> Dict:
        """الحصول على إحصائيات النسخ الاحتياطية"""
        try:
            stats = {
                'total_size': 0,
                'folders': {}
            }
            
            backup_path = "backups"
            
            if not os.path.exists(backup_path):
                return stats
            
            for folder in os.listdir(backup_path):
                folder_path = os.path.join(backup_path, folder)
                if os.path.isdir(folder_path):
                    folder_size = 0
                    file_count = 0
                    
                    for root, dirs, files in os.walk(folder_path):
                        for file in files:
                            file_path = os.path.join(root, file)
                            try:
                                if os.path.exists(file_path):
                                    folder_size += os.path.getsize(file_path)
                                    file_count += 1
                            except:
                                continue
                    
                    stats['folders'][folder] = {
                        'size': folder_size if folder_size is not None else 0,
                        'count': file_count
                    }
                    stats['total_size'] += folder_size if folder_size is not None else 0
            
            return stats
        except Exception as e:
            logger.error(f"خطأ في get_backup_stats: {e}")
            return {'total_size': 0, 'folders': {}}
    
    def export_backup_metadata(self, group_id: int, export_path: str) -> bool:
        """تصدير البيانات الوصفية للنسخة الاحتياطية"""
        try:
            metadata = self.load_group_metadata(group_id)
            if not metadata:
                logger.warning(f"لا توجد بيانات وصفية للمجموعة {group_id}")
                return False
            
            ensure_directory(os.path.dirname(export_path))
            
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ تم تصدير البيانات الوصفية للمجموعة {group_id} إلى {export_path}")
            return True
            
        except Exception as e:
            logger.error(f"فشل تصدير البيانات الوصفية: {e}")
            return False
    
    def import_backup_metadata(self, group_id: int, import_path: str) -> bool:
        """استيراد البيانات الوصفية للنسخة الاحتياطية"""
        try:
            if not os.path.exists(import_path):
                logger.error(f"الملف غير موجود: {import_path}")
                return False
            
            with open(import_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            self.save_group_metadata(group_id, metadata)
            logger.info(f"✅ تم استيراد البيانات الوصفية للمجموعة {group_id}")
            return True
            
        except Exception as e:
            logger.error(f"فشل استيراد البيانات الوصفية: {e}")
            return False