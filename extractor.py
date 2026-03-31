"""
extractor.py - استخراج الأعضاء من المجموعات الضخمة
"""

import asyncio
import logging
import time  # ✅ تم إضافة import time
from typing import List, Dict, Optional
from telethon import TelegramClient
from telethon.tl.types import ChannelParticipantsSearch, ChannelParticipantsAdmins
from telethon.errors import FloodWaitError, RPCError

from database import UltimateDatabase
from utils import (
    retry_on_error, handle_errors, ProgressTracker, 
    format_number, estimate_time
)
from config import PerformanceConfig

logger = logging.getLogger(__name__)

class MassiveMemberExtractor:
    """
    مستخرج متخصص للمجموعات الضخمة
    مع نظام نقاط التوقف والاستئناف التلقائي
    """
    
    def __init__(self, client: TelegramClient, db: UltimateDatabase):
        self.client = client
        self.db = db
        self.config = PerformanceConfig()
        self.is_running = False
        self.current_progress = {}
        self._stop_flag = False
    
    async def extract_all_members(self, group_id: int, force_restart: bool = False) -> Dict:
        """
        استخراج جميع أعضاء المجموعة
        يدعم الاستئناف التلقائي ونقاط التوقف
        """
        try:
            self.is_running = True
            self._stop_flag = False
            
            # الحصول على المجموعة
            chat = await self._get_group(group_id)
            if not chat:
                return {'success': False, 'error': 'المجموعة غير موجودة'}
            
            # التحقق من الصلاحيات
            if not await self._check_permissions(chat):
                return {'success': False, 'error': 'الصلاحيات غير كافية'}
            
            # الحصول على عدد الأعضاء
            total_members = await self._get_total_members(chat)
            logger.info(f"🚀 بدء استخراج {format_number(total_members)} عضو من {chat.title if hasattr(chat, 'title') else chat.id}")
            
            # التحقق من نقطة التوقف
            checkpoint = None
            if not force_restart:
                checkpoint = self.db.get_checkpoint(group_id, 'extract_members')
            
            offset = checkpoint.get('last_offset', 0) if checkpoint else 0
            processed = checkpoint.get('processed_count', 0) if checkpoint else 0
            
            # إعداد متتبع التقدم
            tracker = ProgressTracker(total_members, self.config.CHECKPOINT_INTERVAL)
            tracker.start()
            if processed > 0:
                tracker.update(processed)
            
            # تحديث حالة المجموعة
            self.db.update_group_status(group_id, 'extracting', processed)
            self.db.log_operation(group_id, 'extract_members', 'started', 
                                  f"بدء استخراج {format_number(total_members)} عضو")
            
            # استخراج الأعضاء
            all_members = []
            batch_count = 0
            
            try:
                async for member in self.client.iter_participants(
                    chat,
                    offset=offset,
                    limit=self.config.MAX_MEMBERS_TO_BACKUP,
                    aggressive=True
                ):
                    if self._stop_flag:
                        logger.warning("⚠️ تم إيقاف الاستخراج يدوياً")
                        break
                    
                    # تجميع بيانات العضو
                    member_data = await self._extract_member_data(member)
                    all_members.append(member_data)
                    
                    tracker.update()
                    processed += 1
                    
                    # حفظ دفعة
                    if len(all_members) >= self.config.MEMBERS_BATCH_SIZE:
                        saved = self.db.save_members_batch(group_id, all_members)
                        batch_count += 1
                        
                        # تحديث نقطة التوقف
                        self.db.save_checkpoint(
                            group_id, 'extract_members', 
                            processed, total_members, offset + processed
                        )
                        
                        logger.info(f"💾 دفعة {batch_count}: حفظ {saved} عضو (المجموع: {format_number(processed)})")
                        
                        # تحديث التقدم
                        progress = tracker.get_progress()
                        safe_progress = {
                            'processed': int(progress.get('processed') or 0),
                            'total': int(progress.get('total') or 0),
                            'percentage': float(progress.get('percentage') or 0),
                            'remaining': progress.get('remaining', 'غير معروف'),
                            'rate': float(progress.get('rate') or 0)
                        }
                        self.current_progress[group_id] = safe_progress

                        self.db.log_operation(
                            group_id, 'extract_members', 'in_progress',
                            f"تم استخراج {format_number(processed)}/{format_number(total_members)} عضو ({progress['percentage']:.1f}%)",
                            int(progress['percentage'])
                        )
                        
                        all_members = []
                        
                        # تأخير قصير لتجنب الحظر
                        await asyncio.sleep(self.config.EXTRACT_DELAY)
                
            except FloodWaitError as e:
                logger.warning(f"FloodWait: انتظار {e.seconds} ثانية")
                await asyncio.sleep(e.seconds)
            except RPCError as e:
                logger.error(f"خطأ في الاتصال: {e}")
                return {'success': False, 'error': f'خطأ في الاتصال: {e}'}
            
            # حفظ الأعضاء المتبقين
            if all_members:
                saved = self.db.save_members_batch(group_id, all_members)
                logger.info(f"💾 آخر دفعة: {saved} عضو")
            
            # تحديث الإحصائيات النهائية
            final_count = self.db.get_members_count(group_id)
            self.db.update_group_status(group_id, 'completed', final_count)
            self.db.save_checkpoint(group_id, 'extract_members', final_count, final_count, final_count)
            
            duration = time.time() - tracker.start_time if tracker.start_time else 0
            
            logger.info(f"✅ اكتمل استخراج {format_number(final_count)} عضو")
            self.db.log_operation(group_id, 'extract_members', 'completed',
                                 f"اكتمل استخراج {format_number(final_count)} عضو", 100)
            
            # تحديث التقدم النهائي
            final_progress = {
                'processed': int(final_count),
                'total': int(total_members),
                'percentage': 100.0,
                'remaining': '0 ثانية',
                'rate': float(final_count / duration) if duration > 0 else 0.0
            }
            self.current_progress[group_id] = final_progress
            
            return {
                'success': True,
                'total_members': final_count,
                'duration': duration
            }
            
        except Exception as e:
            logger.error(f"❌ فشل استخراج الأعضاء: {e}")
            self.db.log_operation(group_id, 'extract_members', 'failed', str(e))
            return {'success': False, 'error': str(e)}
        
        finally:
            self.is_running = False
    
    async def _get_group(self, group_id: int):
        """الحصول على كيان المجموعة"""
        try:
            return await self.client.get_entity(group_id)
        except Exception as e:
            logger.error(f"المجموعة غير موجودة: {e}")
            return None
    
    async def _check_permissions(self, chat) -> bool:
        """التحقق من صلاحيات البوت"""
        try:
            me = await self.client.get_me()
            permissions = await self.client.get_permissions(chat, me)
            # التحقق من وجود الصلاحيات المطلوبة
            return permissions.is_admin and getattr(permissions, 'invite_users', False)
        except Exception as e:
            logger.error(f"فشل التحقق من الصلاحيات: {e}")
            return False
    
    async def _get_total_members(self, chat) -> int:
        """الحصول على عدد الأعضاء الكلي"""
        try:
            # محاولة الحصول على العدد من الكيان مباشرة
            if hasattr(chat, 'participants_count'):
                return chat.participants_count
            
            # محاولة الحصول على المعلومات الكاملة
            full = await self.client.get_entity(chat)
            if hasattr(full, 'participants_count'):
                return full.participants_count
            
            # إذا فشل كل شيء، استخدم الحد الأقصى
            return self.config.MAX_MEMBERS_TO_BACKUP
        except Exception as e:
            logger.warning(f"فشل الحصول على عدد الأعضاء: {e}")
            return self.config.MAX_MEMBERS_TO_BACKUP
    
    async def _extract_member_data(self, member) -> Dict:
        """استخراج بيانات العضو"""
        data = {
            'user_id': member.id,
            'username': getattr(member, 'username', None),
            'first_name': getattr(member, 'first_name', None),
            'last_name': getattr(member, 'last_name', None),
            'is_admin': False,
            'is_creator': False,
            'is_bot': getattr(member, 'bot', False),
            'is_deleted': getattr(member, 'deleted', False),
            'join_date': None,
            'admin_rights': {}
        }
        
        # التحقق من صلاحيات المشرف
        if hasattr(member, 'participant') and member.participant:
            participant = member.participant
            
            # التحقق من صلاحيات المشرف
            if hasattr(participant, 'admin_rights') and participant.admin_rights:
                data['is_admin'] = True
                rights = participant.admin_rights
                data['admin_rights'] = {
                    'change_info': getattr(rights, 'change_info', False),
                    'post_messages': getattr(rights, 'post_messages', False),
                    'edit_messages': getattr(rights, 'edit_messages', False),
                    'delete_messages': getattr(rights, 'delete_messages', False),
                    'ban_users': getattr(rights, 'ban_users', False),
                    'invite_users': getattr(rights, 'invite_users', False),
                    'pin_messages': getattr(rights, 'pin_messages', False),
                    'add_admins': getattr(rights, 'add_admins', False)
                }
            
            # التحقق من كون العضو هو المالك
            if hasattr(participant, 'creator') and participant.creator:
                data['is_creator'] = True
                data['is_admin'] = True
        
        return data
    
    def stop(self):
        """إيقاف عملية الاستخراج"""
        self._stop_flag = True
        logger.info("🛑 تم إرسال إشارة إيقاف لعملية الاستخراج")
    
    def get_progress(self, group_id: int) -> Dict:
        """الحصول على تقدم العملية (مع ضمان القيم الرقمية)"""
        progress = self.current_progress.get(group_id, {})
        
        # تحويل جميع القيم الرقمية إلى int أو float مع معالجة None
        safe_progress = {
            'processed': int(progress.get('processed') or 0),
            'total': int(progress.get('total') or 0),
            'percentage': float(progress.get('percentage') or 0),
            'remaining': progress.get('remaining', 'غير معروف'),
            'rate': float(progress.get('rate') or 0)
        }
        
        return safe_progress

    def is_running(self) -> bool:
        """التحقق من أن العملية قيد التشغيل"""
        return self.is_running
    
    def reset(self):
        """إعادة تعيين حالة المستخرج"""
        self._stop_flag = False
        self.is_running = False
        self.current_progress.clear()