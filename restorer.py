"""
restorer.py - استعادة الأعضاء مع توزيع الحمل
"""

import asyncio
import logging
import random
from typing import List, Dict, Optional
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, UserPrivacyRestrictedError, 
    UserAlreadyParticipantError, PeerFloodError
)
from telethon.tl.functions.channels import InviteToChannelRequest, EditAdminRequest
from telethon.tl.types import ChatAdminRights

from database import UltimateDatabase
from utils import (
    retry_on_error, handle_errors, ProgressTracker,
    format_number, estimate_time, split_members
)
from config import PerformanceConfig, HELPER_ACCOUNTS

logger = logging.getLogger(__name__)

class MassiveMemberRestorer:
    """
    مستعيد متخصص للمجموعات الضخمة
    مع توزيع الحمل على حسابات متعددة
    """
    
    def __init__(self, main_client: TelegramClient, db: UltimateDatabase):
        self.main_client = main_client
        self.db = db
        self.config = PerformanceConfig()
        self.helper_clients = []
        self.is_running = False
        self._stop_flag = False
        self.restore_stats = {}
        
        # تهيئة الحسابات المساعدة
        self._init_helper_clients()
    
    def _init_helper_clients(self):
        """تهيئة الحسابات المساعدة"""
        for account in HELPER_ACCOUNTS:
            try:
                client = TelegramClient(
                    account.get('session', f"helper_{account['phone']}"),
                    account['api_id'],
                    account['api_hash']
                )
                self.helper_clients.append({
                    'client': client,
                    'phone': account['phone'],
                    'is_connected': False
                })
                logger.info(f"✅ تم تهيئة حساب مساعد: {account['phone']}")
            except Exception as e:
                logger.error(f"فشل تهيئة الحساب المساعد: {e}")
    
    async def connect_helpers(self):
        """توصيل الحسابات المساعدة"""
        for helper in self.helper_clients:
            try:
                await helper['client'].start(phone=helper['phone'])
                helper['is_connected'] = True
                logger.info(f"✅ تم توصيل الحساب المساعد: {helper['phone']}")
            except Exception as e:
                logger.error(f"فشل توصيل الحساب المساعد: {e}")
    
    async def restore_all_members(self, group_id: int, target_group_id: int, 
                                  max_members: int = None) -> Dict:
        """
        استعادة جميع الأعضاء مع توزيع الحمل
        """
        try:
            self.is_running = True
            self._stop_flag = False
            
            # الحصول على الأعضاء المعلقين
            pending_members = self.db.get_pending_members(group_id, max_members or 100000)
            
            if not pending_members:
                logger.warning(f"لا يوجد أعضاء معلقين للمجموعة {group_id}")
                return {'success': True, 'restored': 0, 'failed': 0}
            
            total_pending = len(pending_members)
            logger.info(f"🚀 بدء استعادة {format_number(total_pending)} عضو")
            
            # توصيل الحسابات المساعدة
            await self.connect_helpers()
            
            # توزيع الأعضاء على الحسابات المتاحة
            if self.helper_clients:
                result = await self._restore_with_helpers(
                    group_id, target_group_id, pending_members
                )
            else:
                result = await self._restore_single_thread(
                    group_id, target_group_id, pending_members
                )
            
            # تحديث إحصائيات المجموعة
            stats = self.db.get_members_stats(group_id)
            logger.info(f"✅ اكتملت الاستعادة: {result['restored']} مستعاد, {result['failed']} فشل")
            
            self.db.log_operation(
                group_id, 'restore_members', 'completed',
                f"استعادة {result['restored']}/{result['total']} عضو",
                int(result['restored'] / result['total'] * 100) if result['total'] else 0
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ فشل استعادة الأعضاء: {e}")
            return {'success': False, 'error': str(e)}
        
        finally:
            self.is_running = False
    
    async def _restore_with_helpers(self, group_id: int, target_group_id: int, 
                                     members: List[Dict]) -> Dict:
        """استعادة باستخدام عدة حسابات مساعدة"""
        
        # حساب عدد الحسابات المتاحة
        active_helpers = [h for h in self.helper_clients if h['is_connected']]
        total_clients = len(active_helpers) + 1  # +1 للبوت الرئيسي
        
        # توزيع الأعضاء
        chunk_size = len(members) // total_clients
        chunks = split_members(members, max(chunk_size, 1))
        
        # توزيع المهام
        tasks = []
        
        # البوت الرئيسي
        main_chunk = chunks[0] if chunks else []
        if main_chunk:
            tasks.append(self._restore_chunk(
                group_id, target_group_id, main_chunk, self.main_client
            ))
        
        # الحسابات المساعدة
        for i, helper in enumerate(active_helpers[:len(chunks)-1]):
            chunk = chunks[i + 1] if i + 1 < len(chunks) else []
            if chunk:
                tasks.append(self._restore_chunk(
                    group_id, target_group_id, chunk, helper['client']
                ))
        
        # تنفيذ جميع المهام بالتوازي
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # تجميع النتائج
        total_restored = 0
        total_failed = 0
        
        for result in results:
            if isinstance(result, dict):
                total_restored += result.get('restored', 0)
                total_failed += result.get('failed', 0)
        
        return {
            'success': True,
            'restored': total_restored,
            'failed': total_failed,
            'total': len(members)
        }
    
    async def _restore_chunk(self, group_id: int, target_group_id: int,
                          members: List[Dict], client) -> Dict:
        """استعادة دفعة من الأعضاء"""
        restored = 0
        failed = 0
        
        # تحويل جميع القيم بشكل آمن
        for member in members:
            # تأكد من أن جميع القيم البوليانية هي int
            member['is_creator'] = int(member.get('is_creator') or 0)
            member['is_admin'] = int(member.get('is_admin') or 0)
            member['is_bot'] = int(member.get('is_bot') or 0)
            member['is_deleted'] = int(member.get('is_deleted') or 0)
        
        # الترتيب الآن آمن
        sorted_members = sorted(members, key=lambda x: (
            -x['is_creator'],  # المالك أولاً (1 > 0)
            -x['is_admin']     # ثم المشرفين
        ))
        
        for member in sorted_members:
            if self._stop_flag:
                break
            
            result = await self._add_single_member(client, target_group_id, member)
            
            if result['success']:
                restored += 1
                self.db.update_member_status(group_id, member['user_id'], 'added')
                
                # تعيين صلاحيات المشرف
                if member.get('is_admin') and not member.get('is_creator'):
                    await self._set_admin_rights(client, target_group_id, member)
            else:
                failed += 1
                self.db.update_member_status(group_id, member['user_id'], 'failed', result.get('error'))
            
            # تأخير عشوائي لتجنب الأنماط
            await asyncio.sleep(random.uniform(
                self.config.DELAY_BETWEEN_MEMBERS * 0.8,
                self.config.DELAY_BETWEEN_MEMBERS * 1.2
            ))
        
        return {'restored': restored, 'failed': failed}

    @retry_on_error(max_retries=3)
    async def _add_single_member(self, client, target_group_id: int, member: Dict) -> Dict:
        """إضافة عضو واحد"""
        try:
            user_id = member['user_id']
            
            # تجاوز البوتات
            if member.get('is_bot') or member.get('is_deleted'):
                return {'success': False, 'error': 'bot_or_deleted'}
            
            await client(InviteToChannelRequest(
                channel=target_group_id,
                users=[user_id]
            ))
            
            return {'success': True}
            
        except FloodWaitError as e:
            logger.warning(f"FloodWait: انتظار {e.seconds} ثانية")
            await asyncio.sleep(e.seconds)
            return await self._add_single_member(client, target_group_id, member)
            
        except UserPrivacyRestrictedError:
            return {'success': False, 'error': 'privacy_restricted'}
            
        except UserAlreadyParticipantError:
            return {'success': True}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _set_admin_rights(self, client, target_group_id: int, member: Dict):
        """تعيين صلاحيات المشرف"""
        try:
            rights = member.get('admin_rights', {})
            
            await client(EditAdminRequest(
                channel=target_group_id,
                user_id=member['user_id'],
                admin_rights=ChatAdminRights(
                    change_info=rights.get('change_info', True),
                    post_messages=rights.get('post_messages', True),
                    edit_messages=rights.get('edit_messages', True),
                    delete_messages=rights.get('delete_messages', True),
                    ban_users=rights.get('ban_users', True),
                    invite_users=rights.get('invite_users', True),
                    pin_messages=rights.get('pin_messages', True),
                    add_admins=rights.get('add_admins', False)
                ),
                rank=member.get('rank', 'admin')
            ))
            logger.info(f"✅ تم تعيين {member.get('username')} كمشرف")
            
        except Exception as e:
            logger.warning(f"فشل تعيين المشرف {member.get('username')}: {e}")
    
    async def _restore_single_thread(self, group_id: int, target_group_id: int,
                                      members: List[Dict]) -> Dict:
        """استعادة بخيط واحد (بدون حسابات مساعدة)"""
        return await self._restore_chunk(
            group_id, target_group_id, members, self.main_client
        )
    
    def stop(self):
        """إيقاف عملية الاستعادة"""
        self._stop_flag = True
    
    async def close_helpers(self):
        """إغلاق الحسابات المساعدة"""
        for helper in self.helper_clients:
            try:
                await helper['client'].disconnect()
            except:
                pass