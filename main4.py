"""
main.py - البوت الرئيسي المتكامل (النسخة المصححة)
يدعم: المعرف (ID)، اليوزرنيم، الرابط
يدعم: عدد غير محدود من المجموعات
"""

import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional

from telethon import TelegramClient, events
from telethon.tl.types import KeyboardButtonCallback
from telethon.tl.functions.messages import CreateChatRequest, MigrateChatRequest, ImportChatInviteRequest, EditChatAboutRequest
from telethon.tl.functions.channels import EditPhotoRequest, EditTitleRequest
from telethon.errors import UsernameNotOccupiedError, InviteHashInvalidError, ChannelInvalidError, MessageIdInvalidError

from config import (
    API_ID, API_HASH, BOT_TOKEN, BACKUP_CHANNEL_ID,
    PerformanceConfig, MESSAGES, NOTIFICATION_SETTINGS
)
from database import UltimateDatabase
from extractor import MassiveMemberExtractor
from restorer import MassiveMemberRestorer
from backup_manager import BackupManager
from utils import (
    format_duration, format_number, ensure_directory,
    create_progress_bar, format_bytes,
    retry_on_error, handle_errors
)

# إعداد التسجيل
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# تهيئة العميل
bot = TelegramClient('ultimate_recovery_bot', API_ID, API_HASH)

# ============== دالة check_admin_rights المصححة ==============
async def check_admin_rights(client, chat_id):
    """التحقق من صلاحيات البوت"""
    try:
        chat = await client.get_entity(chat_id)
        permissions = await client.get_permissions(chat, 'me')
        
        # التحقق من وجود الخصائص قبل استخدامها
        return {
            'is_admin': getattr(permissions, 'is_admin', False),
            'can_invite_users': getattr(permissions, 'invite_users', False),
            'can_change_info': getattr(permissions, 'change_info', False),
            'can_add_admins': getattr(permissions, 'add_admins', False),
            'can_delete_messages': getattr(permissions, 'delete_messages', False),
            'can_restrict_members': getattr(permissions, 'ban_users', False),
            'can_pin_messages': getattr(permissions, 'pin_messages', False),
            'can_promote_members': getattr(permissions, 'add_admins', False)
        }
    except Exception as e:
        logger.error(f"خطأ في التحقق من الصلاحيات: {e}")
        return {'is_admin': False}

# ============== تعريف الأزرار ==============

class Buttons:
    """كل أزرار البوت"""
    
    @staticmethod
    def main_menu():
        """القائمة الرئيسية"""
        return [
            [
                KeyboardButtonCallback("📦 نسخ احتياطي", b"backup"),
                KeyboardButtonCallback("🔄 استعادة", b"restore"),
            ],
            [
                KeyboardButtonCallback("📊 الحالة", b"status"),
                KeyboardButtonCallback("📈 تقدم العملية", b"progress"),
            ],
            [
                KeyboardButtonCallback("📋 قائمة المجموعات", b"groups_list"),
                KeyboardButtonCallback("⚙️ الإعدادات", b"settings"),
            ],
            [
                KeyboardButtonCallback("❓ المساعدة", b"help"),
                KeyboardButtonCallback("🛑 إيقاف الكل", b"stop_all"),
            ]
        ]
    
    @staticmethod
    def backup_menu():
        """قائمة النسخ الاحتياطي"""
        return [
            [KeyboardButtonCallback("🔄 نسخ المجموعة الحالية", b"backup_current")],
            [KeyboardButtonCallback("📝 إدخال معرف أو رابط", b"backup_manual")],
            [KeyboardButtonCallback("📋 اختيار من القائمة", b"backup_select")],
            [KeyboardButtonCallback("🔙 رجوع", b"back_to_main")],
        ]
    
    @staticmethod
    def restore_menu():
        """قائمة الاستعادة"""
        return [
            [KeyboardButtonCallback("🔄 استعادة من آخر نسخة", b"restore_latest")],
            [KeyboardButtonCallback("📝 إدخال معرف المجموعة", b"restore_manual")],
            [KeyboardButtonCallback("📋 عرض المجموعات المنسوخة", b"restore_list")],
            [KeyboardButtonCallback("🔙 رجوع", b"back_to_main")],
        ]
    
    @staticmethod
    def groups_list_menu(groups: List[Dict]):
        """قائمة المجموعات المراقبة"""
        buttons = []
        for group in groups[:15]:
            group_id = group.get('original_id')
            group_name = group.get('title', str(group_id))[:30]
            buttons.append([
                KeyboardButtonCallback(f"📛 {group_name}", f"group_{group_id}".encode())
            ])
        buttons.append([KeyboardButtonCallback("🔄 تحديث", b"refresh_groups")])
        buttons.append([KeyboardButtonCallback("🔙 رجوع", b"back_to_main")])
        return buttons
    
    @staticmethod
    def group_actions_menu(group_id: int, group_name: str):
        """قائمة إجراءات المجموعة"""
        return [
            [KeyboardButtonCallback("📦 نسخ احتياطي", f"backup_{group_id}".encode())],
            [KeyboardButtonCallback("🔄 استعادة", f"restore_group_{group_id}".encode())],
            [KeyboardButtonCallback("📊 إحصائيات", f"stats_{group_id}".encode())],
            [KeyboardButtonCallback("🗑️ حذف النسخة", f"delete_backup_{group_id}".encode())],
            [KeyboardButtonCallback("🔙 رجوع", b"groups_list")],
        ]
    
    @staticmethod
    def settings_menu():
        """قائمة الإعدادات"""
        return [
            [
                KeyboardButtonCallback("🔔 تفعيل الإشعارات", b"settings_notify_on"),
                KeyboardButtonCallback("🔕 تعطيل الإشعارات", b"settings_notify_off"),
            ],
            [
                KeyboardButtonCallback("💾 تنظيف النسخ القديمة", b"settings_cleanup"),
                KeyboardButtonCallback("📊 حجم النسخ", b"settings_size"),
            ],
            [
                KeyboardButtonCallback("🔄 تحديث البيانات", b"settings_refresh"),
                KeyboardButtonCallback("📤 تصدير البيانات", b"settings_export"),
            ],
            [
                KeyboardButtonCallback("🔙 رجوع", b"back_to_main"),
            ]
        ]
    
    @staticmethod
    def confirm_menu(action: str, group_id: int):
        """قائمة التأكيد"""
        return [
            [
                KeyboardButtonCallback("✅ تأكيد", f"confirm_{action}_{group_id}".encode()),
                KeyboardButtonCallback("❌ إلغاء", b"back_to_main"),
            ]
        ]
    
    @staticmethod
    def progress_menu(group_id: int):
        """قائمة متابعة التقدم"""
        return [
            [
                KeyboardButtonCallback("🔄 تحديث", f"refresh_progress_{group_id}".encode()),
                KeyboardButtonCallback("🛑 إيقاف", f"stop_{group_id}".encode()),
            ],
            [KeyboardButtonCallback("🔙 رجوع", b"back_to_main")],
        ]

# ============== دوال مساعدة لتحويل الإدخال ==============

class InputParser:
    """تحويل المعرف/اليوزرنيم/الرابط إلى معرف المجموعة"""
    
    @staticmethod
    def parse_input(input_text: str) -> Dict:
        """
        تحويل الإدخال إلى معرف المجموعة
        يدعم:
        - معرف رقمي: -100123456789
        - يوزرنيم: @username أو username
        - رابط: https://t.me/username أو https://t.me/joinchat/xxxx
        """
        input_text = input_text.strip()
        
        # حالة 1: معرف رقمي
        if input_text.lstrip('-').isdigit():
            return {'type': 'id', 'value': int(input_text)}
        
        # حالة 2: رابط دعوة (joinchat)
        join_match = re.search(r't\.me/joinchat/([a-zA-Z0-9_-]+)', input_text)
        if join_match:
            return {'type': 'invite', 'value': join_match.group(1)}
        
        # حالة 3: رابط يوزرنيم
        username_match = re.search(r't\.me/([a-zA-Z0-9_]+)', input_text)
        if username_match:
            username = username_match.group(1)
            if username not in ['joinchat', 'c']:
                return {'type': 'username', 'value': username}
        
        # حالة 4: يوزرنيم مباشر
        if input_text.startswith('@'):
            return {'type': 'username', 'value': input_text[1:]}
        
        # حالة 5: يوزرنيم بدون @
        if re.match(r'^[a-zA-Z][a-zA-Z0-9_]{4,}$', input_text):
            return {'type': 'username', 'value': input_text}
        
        return {'type': 'invalid', 'value': input_text}
    
    @staticmethod
    async def resolve_to_chat_id(client, parsed: Dict) -> Optional[int]:
        """تحويل الكائن إلى معرف المجموعة"""
        try:
            if parsed['type'] == 'id':
                chat = await client.get_entity(parsed['value'])
                return chat.id
            
            elif parsed['type'] == 'username':
                chat = await client.get_entity(f"@{parsed['value']}")
                return chat.id
            
            elif parsed['type'] == 'invite':
                # انضمام إلى المجموعة عبر رابط الدعوة
                result = await client(ImportChatInviteRequest(parsed['value']))
                if result.chats:
                    return result.chats[0].id
            
            return None
            
        except (UsernameNotOccupiedError, InviteHashInvalidError, ChannelInvalidError) as e:
            logger.error(f"فشل تحويل الإدخال: {e}")
            return None
        except Exception as e:
            logger.error(f"خطأ غير متوقع: {e}")
            return None

# ============== البوت الرئيسي ==============

class UltimateRecoveryBot:
    """البوت الرئيسي المتكامل"""
    
    def __init__(self):
        self.client = bot
        self.db = UltimateDatabase()
        self.backup_manager = BackupManager()
        self.config = PerformanceConfig()
        self.monitored_groups = {}
        self.running_operations = {}
        self.user_states = {}
        self.input_parser = InputParser()
        self.input_handlers = {}  # لتخزين مراجع المعالجات
        self.last_messages = {}  # لتخزين آخر رسالة لكل مستخدم
        
        # سيتم تهيئتها بعد بدء العميل
        self.extractor = None
        self.restorer = None
    
    async def start(self):
        """بدء تشغيل البوت"""
        try:
            await self.client.start(bot_token=BOT_TOKEN)
            logger.info("✅ تم الاتصال بـ Telegram API")
            
            # تهيئة المكونات
            self.extractor = MassiveMemberExtractor(self.client, self.db)
            self.restorer = MassiveMemberRestorer(self.client, self.db)
            
            # تسجيل المعالجات
            await self._register_handlers()
            
            # إنشاء المجلدات
            ensure_directory("backups/photos")
            ensure_directory("backups/media")
            ensure_directory("backups/messages")
            ensure_directory("backups/compressed")
            ensure_directory("backups/temp")
            ensure_directory("backups/metadata")
            
            # تحميل المجموعات المراقبة من قاعدة البيانات
            await self._load_monitored_groups()
            
            logger.info("🚀 البوت جاهز للعمل")
            
        except Exception as e:
            logger.error(f"❌ فشل بدء البوت: {e}")
            raise
    
    async def _load_monitored_groups(self):
        """تحميل المجموعات المراقبة من قاعدة البيانات"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT original_id, title, total_members_backup, backup_date 
                    FROM groups 
                    WHERE backup_status = 'completed'
                ''')
                groups = cursor.fetchall()
                
                for group in groups:
                    self.monitored_groups[group[0]] = {
                        'name': group[1],
                        'backup_date': group[3],
                        'members': group[2]
                    }
                
                logger.info(f"📋 تم تحميل {len(self.monitored_groups)} مجموعة مراقبة")
        except Exception as e:
            logger.error(f"فشل تحميل المجموعات: {e}")
    
    async def _register_handlers(self):
        """تسجيل معالجات الأحداث"""
        
        @self.client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            await self.handle_start(event)
        
        @self.client.on(events.NewMessage(pattern='/help'))
        async def help_handler(event):
            await self.handle_help(event)
        
        @self.client.on(events.CallbackQuery)
        async def callback_handler(event):
            await self.handle_callback(event)
        
        @self.client.on(events.ChatAction)
        async def group_deleted_handler(event):
            await self.handle_group_deleted(event)
    
    async def safe_edit_message(self, event, text, buttons=None):
        """تعديل رسالة بأمان مع معالجة الأخطاء"""
        try:
            # التحقق من وجود event.message
            if hasattr(event, 'message') and event.message:
                try:
                    await event.edit(text, buttons=buttons)
                    # تخزين آخر رسالة تم تعديلها
                    if hasattr(event, 'chat_id') and event.chat_id:
                        self.last_messages[event.chat_id] = event.message.id
                except MessageIdInvalidError:
                    # إذا كانت الرسالة غير صالحة، أرسل رسالة جديدة
                    logger.warning("الرسالة غير صالحة للتعديل، إرسال رسالة جديدة")
                    if hasattr(event, 'reply'):
                        new_msg = await event.reply(text, buttons=buttons)
                        if new_msg and hasattr(event, 'chat_id'):
                            self.last_messages[event.chat_id] = new_msg.id
                except Exception as e:
                    logger.error(f"فشل تعديل الرسالة: {e}")
                    # إذا فشل التعديل، أرسل رسالة جديدة
                    if hasattr(event, 'reply'):
                        new_msg = await event.reply(text, buttons=buttons)
                        if new_msg and hasattr(event, 'chat_id'):
                            self.last_messages[event.chat_id] = new_msg.id
            else:
                # إذا لم يكن هناك رسالة، أرسل رسالة جديدة
                if hasattr(event, 'reply'):
                    new_msg = await event.reply(text, buttons=buttons)
                    if new_msg and hasattr(event, 'chat_id'):
                        self.last_messages[event.chat_id] = new_msg.id
        except Exception as e:
            logger.error(f"خطأ في safe_edit_message: {e}")
            try:
                if hasattr(event, 'reply'):
                    new_msg = await event.reply(text, buttons=buttons)
                    if new_msg and hasattr(event, 'chat_id'):
                        self.last_messages[event.chat_id] = new_msg.id
            except:
                pass
    
    # ============== معالجات الأوامر النصية ==============
    
    async def handle_start(self, event):
        """معالج أمر /start"""
        try:
            self.user_states[event.sender_id] = {'state': 'main'}
            
            stats = self.db.get_total_stats()
            
            message = MESSAGES['start'].format(
                monitored_groups=len(self.monitored_groups),
                total_members=stats.get('members', 0),
                backup_size=stats.get('backup_size_mb', 0)
            )
            
            await self.safe_edit_message(event, message, Buttons.main_menu())
            
        except Exception as e:
            logger.error(f"خطأ في /start: {e}")
            await self.send_error(event, e)
    
    async def handle_help(self, event):
        """معالج أمر /help"""
        help_text = """
📖 **دليل استخدام البوت الاحترافي**

**🔹 ما يمكنني فعله:**
✅ نسخ احتياطي كامل للمجموعات (حتى 200,000 عضو)
✅ استعادة تلقائية عند حذف المجموعة
✅ استعادة الأعضاء والمشرفين والصلاحيات
✅ إشعار فوري للمالك والمشرفين
✅ نظام نقاط توقف لاستكمال العمليات

**🔹 طرق إدخال المجموعة:**
• **معرف رقمي:** `-100123456789`
• **يوزرنيم:** `@username` أو `username`
• **رابط:** `https://t.me/username`
• **رابط دعوة:** `https://t.me/joinchat/xxxx`

**🔹 الأزرار المتاحة:**
• 📦 نسخ احتياطي - نسخ مجموعة جديدة
• 🔄 استعادة - استعادة مجموعة محذوفة
• 📊 الحالة - عرض الإحصائيات
• 📈 تقدم العملية - متابعة العمليات
• 📋 قائمة المجموعات - عرض المجموعات المنسوخة
• ⚙️ الإعدادات - ضبط البوت

**⚠️ ملاحظات مهمة:**
• يجب أن يكون البوت مديراً في المجموعة
• العمليات الكبيرة قد تستغرق عدة ساعات
• يمكنك إدخال أكثر من مجموعة (نسخ منفصلة)
"""
        await self.safe_edit_message(event, help_text, Buttons.main_menu())
    
    # ============== معالج الأزرار ==============
    
    async def handle_callback(self, event):
        """معالج الأزرار التفاعلية"""
        try:
            data = event.data.decode()
            
            # قائمة الأزرار الرئيسية
            handlers = {
                'backup': self._handle_backup_button,
                'restore': self._handle_restore_button,
                'status': self._handle_status_button,
                'progress': self._handle_progress_button,
                'groups_list': self._handle_groups_list_button,
                'settings': self._handle_settings_button,
                'help': self._handle_help_button,
                'stop_all': self._handle_stop_all_button,
                'back_to_main': self._handle_back_to_main,
                'backup_current': self._handle_backup_current,
                'backup_manual': self._handle_backup_manual,
                'backup_select': self._handle_backup_select,
                'restore_latest': self._handle_restore_latest,
                'restore_manual': self._handle_restore_manual,
                'restore_list': self._handle_restore_list,
                'refresh_groups': self._handle_refresh_groups,
                'settings_notify_on': self._handle_settings_notify_on,
                'settings_notify_off': self._handle_settings_notify_off,
                'settings_cleanup': self._handle_settings_cleanup,
                'settings_size': self._handle_settings_size,
                'settings_refresh': self._handle_settings_refresh,
                'settings_export': self._handle_settings_export,
            }
            
            # معالجة الأزرار الخاصة
            if data.startswith('backup_') and data not in ['backup_current', 'backup_manual', 'backup_select']:
                group_id = int(data.split('_')[1])
                await self._start_backup(event, group_id)
            
            elif data.startswith('restore_group_'):
                group_id = int(data.split('_')[2])
                await self._start_restore(event, group_id)
            
            elif data.startswith('group_'):
                group_id = int(data.split('_')[1])
                await self._show_group_actions(event, group_id)
            
            elif data.startswith('stats_'):
                group_id = int(data.split('_')[1])
                await self._show_group_stats(event, group_id)
            
            elif data.startswith('delete_backup_'):
                group_id = int(data.split('_')[2])
                await self._confirm_delete_backup(event, group_id)
            
            elif data.startswith('refresh_progress_'):
                group_id = int(data.split('_')[2])
                await self._show_progress(event, group_id)
            
            elif data.startswith('stop_'):
                group_id = int(data.split('_')[1])
                await self._stop_operation(event, group_id)
            
            elif data.startswith('confirm_delete_'):
                group_id = int(data.split('_')[2])
                await self._delete_backup(event, group_id)
            
            elif data in handlers:
                await handlers[data](event)
            else:
                await event.answer("❌ زر غير معروف", alert=True)
                
        except Exception as e:
            logger.error(f"خطأ في معالج الأزرار: {e}")
            await event.answer(f"❌ حدث خطأ: {str(e)[:50]}", alert=True)
    
    # ============== معالجات الأزرار الرئيسية ==============
    
    async def _handle_backup_button(self, event):
        """زر النسخ الاحتياطي"""
        await event.answer("📦 جاري تحضير قائمة النسخ الاحتياطي...")
        await self.safe_edit_message(
            event,
            "📦 **قائمة النسخ الاحتياطي**\n\n"
            "اختر طريقة إدخال المجموعة:\n\n"
            "• **نسخ المجموعة الحالية** - استخدم هذه الزر إذا كنت في المجموعة\n"
            "• **إدخال معرف أو رابط** - أدخل ID/Username/Link يدوياً\n"
            "• **اختيار من القائمة** - اختر من المجموعات المنسوخة سابقاً",
            buttons=Buttons.backup_menu()
        )
    
    async def _handle_restore_button(self, event):
        """زر الاستعادة"""
        await event.answer("🔄 جاري تحضير قائمة الاستعادة...")
        await self.safe_edit_message(
            event,
            "🔄 **قائمة الاستعادة**\n\n"
            "اختر طريقة استعادة المجموعة:\n\n"
            "• **استعادة من آخر نسخة** - استعادة أحدث مجموعة تم نسخها\n"
            "• **إدخال معرف المجموعة** - أدخل معرف المجموعة المراد استعادتها\n"
            "• **عرض المجموعات المنسوخة** - اختر من القائمة",
            buttons=Buttons.restore_menu()
        )
    
    async def _handle_status_button(self, event):
        """زر الحالة"""
        await event.answer("📊 جاري جلب الحالة...")
        
        stats = self.db.get_total_stats()
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT original_id, title, total_members_backup, backup_date 
                FROM groups 
                WHERE backup_status = 'completed'
                ORDER BY backup_date DESC
                LIMIT 10
            ''')
            recent_groups = cursor.fetchall()
        
        message = f"""
📊 **حالة النسخ الاحتياطية**

**📦 إحصائيات عامة:**
• مجموعات منسوخة: {stats.get('groups', 0)}
• إجمالي الأعضاء: {format_number(stats.get('members', 0))}
• حجم النسخ: {stats.get('backup_size_mb', 0)} MB
• عمليات جارية: {len(self.running_operations)}

**📋 آخر النسخ الاحتياطية:**
"""
        
        for group in recent_groups:
            date = str(group[3]).split('.')[0] if group[3] else "غير معروف"
            message += f"\n📛 {group[1]}\n🆔 `{group[0]}` | 👥 {format_number(group[2])} | 📅 {date}"
        
        await self.safe_edit_message(event, message, Buttons.main_menu())
        
    async def _handle_progress_button(self, event):
        """زر تقدم العملية"""
        await event.answer("📈 جاري جلب التقدم...")
        
        if not self.running_operations:
            await self.safe_edit_message(
                event,
                "📭 **لا توجد عمليات جارية حالياً**\n\n"
                "لبدء عملية جديدة، اختر نسخ احتياطي من القائمة الرئيسية.",
                buttons=Buttons.main_menu()
            )
            return
        
        message = "📈 **العمليات الجارية:**\n\n"
        
        for gid, op in self.running_operations.items():
            group_name = "غير معروف"
            try:
                group_data = self.db.get_group(gid)
                if group_data:
                    group_name = group_data.get('title', str(gid))
            except:
                pass
            
            progress = self.extractor.get_progress(gid) if self.extractor else {}
            # 🔧 التصحيح: تأكد من أن percentage رقم وليس None
            percentage = progress.get('percentage') or 0
            if percentage is None:  # إذا كانت None، حولها إلى 0
                percentage = 0
            bar = create_progress_bar(percentage)
            
            message += f"📛 **{group_name}**\n"
            message += f"🆔 `{gid}`\n"
            message += f"📊 النوع: {op.get('type', 'غير معروف')}\n"
            message += f"{bar} {percentage:.1f}%\n"
            message += f"👥 تم: {format_number(progress.get('processed', 0))} / {format_number(progress.get('total', 0))}\n"
            message += f"⏱️ المتبقي: {progress.get('remaining', 'غير معروف')}\n"
            message += "━━━━━━━━━━━━━━━━━━━━\n\n"
        
        await self.safe_edit_message(event, message, Buttons.main_menu())
    
    async def _handle_groups_list_button(self, event):
        """زر قائمة المجموعات"""
        await event.answer("📋 جاري جلب قائمة المجموعات...")
        
        groups_list = []
        for gid, info in self.monitored_groups.items():
            groups_list.append({
                'original_id': gid,
                'title': info.get('name', str(gid)),
                'members': info.get('members', 0)
            })
        
        if groups_list:
            await self.safe_edit_message(
                event,
                "📋 **المجموعات المراقبة**\n\n"
                f"إجمالي: {len(groups_list)} مجموعة\n\n"
                "اختر مجموعة لعرض الإجراءات المتاحة:",
                buttons=Buttons.groups_list_menu(groups_list)
            )
        else:
            await self.safe_edit_message(
                event,
                "📭 **لا توجد مجموعات مراقبة حالياً**\n\n"
                "قم بعمل نسخ احتياطي أولاً من القائمة الرئيسية.",
                buttons=Buttons.main_menu()
            )
    
    async def _handle_settings_button(self, event):
        """زر الإعدادات"""
        await event.answer("⚙️ جاري تحضير الإعدادات...")
        
        backup_size = self.backup_manager.get_backup_size()
        
        message = f"""
⚙️ **الإعدادات**

**💾 معلومات النسخ الاحتياطي:**
• الحجم الكلي: {format_bytes(backup_size)}
• مسار النسخ: backups/
• المجموعات المراقبة: {len(self.monitored_groups)}

**🔔 الإشعارات:**
• إشعار للمالك: {'✅ مفعل' if NOTIFICATION_SETTINGS.get('notify_creator') else '❌ معطل'}
• إشعار للمشرفين: {'✅ مفعل' if NOTIFICATION_SETTINGS.get('notify_admins') else '❌ معطل'}

**⚡ الأداء:**
• الحد الأقصى للأعضاء: {format_number(self.config.MAX_MEMBERS_TO_BACKUP)}
• حجم دفعة الاستخراج: {self.config.MEMBERS_BATCH_SIZE}
• تأخير بين الإضافات: {self.config.DELAY_BETWEEN_MEMBERS} ثانية
"""
        
        await self.safe_edit_message(event, message, Buttons.settings_menu())
    
    async def _handle_help_button(self, event):
        """زر المساعدة"""
        await self.handle_help(event)
    
    async def _handle_stop_all_button(self, event):
        """زر إيقاف الكل"""
        await event.answer("🛑 جاري إيقاف جميع العمليات...")
        
        if self.extractor:
            self.extractor.stop()
        if self.restorer:
            self.restorer.stop()
        self.running_operations.clear()
        
        await self.safe_edit_message(
            event,
            "🛑 **تم إيقاف جميع العمليات الجارية**\n\n"
            "يمكنك البدء من جديد من القائمة الرئيسية.",
            buttons=Buttons.main_menu()
        )
    
    async def _handle_back_to_main(self, event):
        """زر الرجوع للقائمة الرئيسية"""
        await self.safe_edit_message(event, "🎯 **القائمة الرئيسية**\n\nاختر ما تريد القيام به:", buttons=Buttons.main_menu())
    
    async def _handle_backup_current(self, event):
        """زر نسخ المجموعة الحالية"""
        await event.answer("📦 جاري الحصول على المجموعة الحالية...")
        
        try:
            chat = await event.get_chat()
            if hasattr(chat, 'title'):
                group_id = chat.id
                group_name = chat.title
                await self._start_backup(event, group_id, group_name)
            else:
                await self.safe_edit_message(
                    event,
                    "❌ **لا يمكن استخدام هذا الزر في المحادثة الخاصة**\n\n"
                    "الرجاء استخدام:\n"
                    "• 'إدخال معرف أو رابط' لإدخال معرف المجموعة يدوياً\n"
                    "• الدخول إلى المجموعة ثم استخدام الزر",
                    buttons=Buttons.backup_menu()
                )
        except Exception as e:
            await self.safe_edit_message(
                event,
                f"❌ حدث خطأ: {str(e)}\n\n"
                f"الرجاء استخدام 'إدخال معرف أو رابط'",
                buttons=Buttons.backup_menu()
            )
    
    async def _handle_backup_manual(self, event):
        """زر إدخال معرف أو رابط"""
        try:
            await event.answer("📝 الرجاء إرسال معرف أو رابط المجموعة...")
            
            self.user_states[event.sender_id] = {'state': 'waiting_backup_input'}
            
            await self.safe_edit_message(
                event,
                "📝 **إدخال معرف أو رابط المجموعة**\n\n"
                "أرسل أحد الأشكال التالية:\n\n"
                "• **معرف رقمي:** `-100123456789`\n"
                "• **يوزرنيم:** `@username` أو `username`\n"
                "• **رابط:** `https://t.me/username`\n"
                "• **رابط دعوة:** `https://t.me/joinchat/xxxx`\n\n"
                "_يمكنك إرسال أكثر من مجموعة (واحد في كل مرة)_",
                buttons=[[KeyboardButtonCallback("🔙 إلغاء", b"back_to_main")]]
            )
            
            # إنشاء معالج جديد وتخزين مرجعه
            @self.client.on(events.NewMessage(from_users=event.sender_id))
            async def wait_for_input(msg):
                try:
                    if self.user_states.get(msg.sender_id, {}).get('state') != 'waiting_backup_input':
                        return
                    
                    input_text = msg.text.strip() if msg.text else ""
                    
                    if input_text == '/cancel':
                        await self.safe_edit_message(msg, "❌ تم الإلغاء", buttons=Buttons.main_menu())
                        self.user_states[msg.sender_id] = {'state': 'main'}
                        # إزالة المعالج باستخدام المرجع المخزن
                        if event.sender_id in self.input_handlers:
                            self.client.remove_event_handler(self.input_handlers[event.sender_id])
                            del self.input_handlers[event.sender_id]
                        return
                    
                    if not input_text:
                        await self.safe_edit_message(msg, "❌ الرجاء إدخال نص صالح")
                        return
                    
                    # تحويل الإدخال
                    parsed = self.input_parser.parse_input(input_text)
                    
                    if parsed['type'] == 'invalid':
                        await self.safe_edit_message(
                            msg,
                            "❌ **إدخال غير صالح**\n\n"
                            "الرجاء إرسال:\n"
                            "• معرف رقمي: `-100123456789`\n"
                            "• يوزرنيم: `@username`\n"
                            "• رابط: `https://t.me/username`"
                        )
                        return
                    
                    # تحويل إلى معرف المجموعة
                    group_id = await self.input_parser.resolve_to_chat_id(self.client, parsed)
                    
                    if not group_id:
                        await self.safe_edit_message(
                            msg,
                            "❌ **لا يمكن الوصول للمجموعة**\n\n"
                            "تأكد من:\n"
                            "• صحة المعرف/الرابط\n"
                            "• البوت مشرف في المجموعة\n"
                            "• المجموعة غير محظورة"
                        )
                        return
                    
                    await self._start_backup(msg, group_id)
                    self.user_states[msg.sender_id] = {'state': 'main'}
                    # إزالة المعالج بعد الانتهاء
                    if event.sender_id in self.input_handlers:
                        self.client.remove_event_handler(self.input_handlers[event.sender_id])
                        del self.input_handlers[event.sender_id]
                    
                except Exception as e:
                    logger.error(f"خطأ في wait_for_input: {e}")
                    await self.safe_edit_message(msg, f"❌ حدث خطأ: {str(e)}")
                    if event.sender_id in self.input_handlers:
                        self.client.remove_event_handler(self.input_handlers[event.sender_id])
                        del self.input_handlers[event.sender_id]
            
            # تخزين مرجع المعالج
            self.input_handlers[event.sender_id] = wait_for_input
            
        except Exception as e:
            logger.error(f"خطأ في _handle_backup_manual: {e}")
            await self.safe_edit_message(event, f"❌ حدث خطأ: {str(e)}", buttons=Buttons.main_menu())
    
    async def _handle_backup_select(self, event):
        """زر اختيار من القائمة"""
        await event.answer("📋 جاري جلب قائمة المجموعات...")
        await self._handle_groups_list_button(event)
    
    async def _handle_restore_latest(self, event):
        """زر استعادة آخر نسخة"""
        await event.answer("🔄 جاري البحث عن آخر نسخة...")
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT original_id, title 
                FROM groups 
                WHERE backup_status = 'completed'
                ORDER BY backup_date DESC
                LIMIT 1
            ''')
            latest = cursor.fetchone()
        
        if latest:
            group_id = latest[0]
            group_name = latest[1]
            
            await self.safe_edit_message(
                event,
                f"🔄 **تأكيد استعادة المجموعة**\n\n"
                f"📛 **الاسم:** {group_name}\n"
                f"🆔 **المعرف:** `{group_id}`\n\n"
                f"هل تريد استعادة هذه المجموعة؟",
                buttons=Buttons.confirm_menu('restore', group_id)
            )
        else:
            await self.safe_edit_message(
                event,
                "📭 **لا توجد نسخ احتياطية متاحة**\n\n"
                "قم بعمل نسخ احتياطي أولاً من القائمة الرئيسية.",
                buttons=Buttons.main_menu()
            )
    
    async def _handle_restore_manual(self, event):
        """زر إدخال معرف المجموعة للاستعادة"""
        await event.answer("📝 الرجاء إرسال معرف المجموعة...")
        
        self.user_states[event.sender_id] = {'state': 'waiting_restore_id'}
        
        await self.safe_edit_message(
            event,
            "📝 **إدخال معرف المجموعة للاستعادة**\n\n"
            "أرسل معرف المجموعة (ID) الذي تريد استعادته.\n\n"
            "مثال: `-100123456789`\n\n"
            "_يمكنك رؤية قائمة المجموعات المنسوخة من زر 'عرض المجموعات المنسوخة'_",
            buttons=[[KeyboardButtonCallback("🔙 إلغاء", b"back_to_main")]]
        )
        
        @self.client.on(events.NewMessage(from_users=event.sender_id))
        async def wait_for_restore_id(msg):
            if self.user_states.get(msg.sender_id, {}).get('state') == 'waiting_restore_id':
                try:
                    group_id = int(msg.text.strip())
                    await self._start_restore(msg, group_id)
                    self.user_states[msg.sender_id] = {'state': 'main'}
                    self.client.remove_event_handler(wait_for_restore_id)
                except ValueError:
                    await self.safe_edit_message(msg, "❌ معرف غير صالح. الرجاء إرسال رقم صحيح.")
    
    async def _handle_restore_list(self, event):
        """زر عرض المجموعات المنسوخة"""
        await event.answer("📋 جاري جلب القائمة...")
        await self._handle_groups_list_button(event)
    
    async def _handle_refresh_groups(self, event):
        """زر تحديث قائمة المجموعات"""
        await event.answer("🔄 جاري تحديث القائمة...")
        await self._load_monitored_groups()
        await self._handle_groups_list_button(event)
    
    async def _handle_settings_notify_on(self, event):
        """تفعيل الإشعارات"""
        NOTIFICATION_SETTINGS['notify_creator'] = True
        NOTIFICATION_SETTINGS['notify_admins'] = True
        await event.answer("✅ تم تفعيل الإشعارات")
        await self._handle_settings_button(event)
    
    async def _handle_settings_notify_off(self, event):
        """تعطيل الإشعارات"""
        NOTIFICATION_SETTINGS['notify_creator'] = False
        NOTIFICATION_SETTINGS['notify_admins'] = False
        await event.answer("🔕 تم تعطيل الإشعارات")
        await self._handle_settings_button(event)
    
    async def _handle_settings_cleanup(self, event):
        """تنظيف النسخ القديمة"""
        await event.answer("💾 جاري تنظيف النسخ القديمة...")
        
        result = self.backup_manager.cleanup_old_backups(days=30)
        
        await self.safe_edit_message(
            event,
            f"💾 **تنظيف النسخ القديمة**\n\n"
            f"• تم حذف: {result['deleted_files']} ملف\n"
            f"• المساحة المحررة: {format_bytes(result['freed_space'])}\n"
            f"• الأخطاء: {len(result['errors'])}",
            buttons=Buttons.settings_menu()
        )
    
    async def _handle_settings_size(self, event):
        """عرض حجم النسخ"""
        await event.answer("📊 جاري حساب حجم النسخ...")
        
        stats = self.backup_manager.get_backup_stats()
        
        message = f"""
📊 **حجم النسخ الاحتياطية**

**💾 الحجم الكلي:** {format_bytes(stats.get('total_size', 0))}

**📁 تفاصيل المجلدات:**
"""
        
        for folder, info in stats.get('folders', {}).items():
            message += f"• {folder}: {info['count']} ملف, {format_bytes(info['size'])}\n"
        
        message += f"\n**📛 المجموعات المنسوخة:** {len(self.monitored_groups)}"
        
        await self.safe_edit_message(event, message, buttons=Buttons.settings_menu())
    
    async def _handle_settings_refresh(self, event):
        """تحديث البيانات"""
        await event.answer("🔄 جاري تحديث البيانات...")
        await self._load_monitored_groups()
        await event.answer("✅ تم تحديث البيانات", alert=True)
        await self._handle_settings_button(event)
    
    async def _handle_settings_export(self, event):
        """تصدير البيانات"""
        await event.answer("📤 جاري تصدير البيانات...")
        
        export_path = f"backups/export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            import json
            data = {
                'groups': list(self.monitored_groups.keys()),
                'export_date': datetime.now().isoformat(),
                'total_groups': len(self.monitored_groups)
            }
            
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            await self.safe_edit_message(
                event,
                f"📤 **تم تصدير البيانات**\n\n"
                f"📁 الملف: `{export_path}`\n"
                f"📊 عدد المجموعات: {len(self.monitored_groups)}",
                buttons=Buttons.settings_menu()
            )
        except Exception as e:
            await self.safe_edit_message(event, f"❌ فشل التصدير: {e}", buttons=Buttons.settings_menu())
    
    # ============== إجراءات المجموعات ==============
    
    async def _show_group_actions(self, event, group_id: int):
        """عرض إجراءات المجموعة"""
        group_data = self.db.get_group(group_id)
        if not group_data:
            await event.answer("❌ المجموعة غير موجودة", alert=True)
            return
        
        group_name = group_data.get('title', str(group_id))
        
        await self.safe_edit_message(
            event,
            f"📛 **{group_name}**\n"
            f"🆔 `{group_id}`\n"
            f"👥 الأعضاء: {format_number(group_data.get('total_members_backup', 0))}\n"
            f"📅 آخر نسخ: {group_data.get('backup_date', 'غير معروف')}\n\n"
            f"**ماذا تريد أن تفعل؟**",
            buttons=Buttons.group_actions_menu(group_id, group_name)
        )
    
    async def _show_group_stats(self, event, group_id: int):
        """عرض إحصائيات المجموعة"""
        group_data = self.db.get_group(group_id)
        if not group_data:
            await event.answer("❌ المجموعة غير موجودة", alert=True)
            return
        
        stats = self.db.get_members_stats(group_id)
        
        message = f"""
📊 **إحصائيات المجموعة**

📛 **الاسم:** {group_data.get('title')}
🆔 **المعرف:** `{group_id}`

**👥 إحصائيات الأعضاء:**
• إجمالي الأعضاء: {format_number(stats.get('total', 0))}
• المشرفين: {format_number(stats.get('admins', 0))}
• المالك: {'✅' if stats.get('creators', 0) > 0 else '❌'}
• البوتات: {format_number(stats.get('bots', 0))}

**📊 حالة الاستعادة:**
• تمت إضافتهم: {format_number(stats.get('added', 0))}
• فشل الإضافة: {format_number(stats.get('failed', 0))}
• قيد الانتظار: {format_number(stats.get('pending', 0))}

**💾 معلومات النسخ:**
• تاريخ النسخ: {group_data.get('backup_date', 'غير معروف')}
• حجم النسخ: {format_bytes(group_data.get('backup_size', 0))}
"""
        
        await self.safe_edit_message(event, message, buttons=Buttons.group_actions_menu(group_id, group_data.get('title', str(group_id))))
    
    async def _confirm_delete_backup(self, event, group_id: int):
        """تأكيد حذف النسخة الاحتياطية"""
        await self.safe_edit_message(
            event,
            f"⚠️ **تأكيد حذف النسخة الاحتياطية**\n\n"
            f"🆔 المعرف: `{group_id}`\n\n"
            f"هل أنت متأكد من حذف هذه النسخة؟\n"
            f"لا يمكن استرداد البيانات بعد الحذف!",
            buttons=[
                [KeyboardButtonCallback("✅ نعم، احذف", f"confirm_delete_{group_id}".encode())],
                [KeyboardButtonCallback("❌ إلغاء", f"group_{group_id}".encode())]
            ]
        )
    
    async def _delete_backup(self, event, group_id: int):
        """حذف النسخة الاحتياطية"""
        try:
            # حذف من قاعدة البيانات
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM groups WHERE original_id = ?', (group_id,))
                cursor.execute('DELETE FROM members WHERE group_id = ?', (group_id,))
                cursor.execute('DELETE FROM messages WHERE group_id = ?', (group_id,))
                cursor.execute('DELETE FROM checkpoints WHERE group_id = ?', (group_id,))
                conn.commit()
            
            # حذف من المراقبة
            if group_id in self.monitored_groups:
                del self.monitored_groups[group_id]
            
            # حذف الملفات
            result = self.backup_manager.cleanup_old_backups(days=0)
            
            await self.safe_edit_message(
                event,
                f"✅ **تم حذف النسخة الاحتياطية**\n\n"
                f"🆔 المعرف: `{group_id}`\n\n"
                f"تم حذف جميع البيانات بنجاح.",
                buttons=Buttons.main_menu()
            )
            
        except Exception as e:
            await self.safe_edit_message(event, f"❌ فشل الحذف: {e}", buttons=Buttons.main_menu())
    
    # ============== العمليات الأساسية ==============
    
    async def _start_backup(self, event, group_id: int, group_name: str = None):
        """بدء عملية النسخ الاحتياطي"""
        try:
            # التحقق من وجود المجموعة
            try:
                chat = await self.client.get_entity(group_id)
                if not hasattr(chat, 'title'):
                    await self.safe_edit_message(
                        event,
                        f"❌ المعرف `{group_id}` ليس لمجموعة صالحة",
                        buttons=Buttons.main_menu()
                    )
                    return
                group_name = chat.title
            except Exception as e:
                await self.safe_edit_message(
                    event,
                    f"❌ لا يمكن الوصول للمجموعة `{group_id}`\n\n"
                    f"الخطأ: {str(e)}\n\n"
                    f"تأكد من:\n"
                    f"• صحة المعرف\n"
                    f"• البوت مشرف في المجموعة",
                    buttons=Buttons.main_menu()
                )
                return
            
            # التحقق من الصلاحيات
            try:
                permissions = await check_admin_rights(self.client, group_id)
                is_admin = permissions.get('is_admin', False)
            except:
                is_admin = False
            
            if not is_admin:
                await self.safe_edit_message(
                    event,
                    f"❌ **الصلاحيات غير كافية**\n\n"
                    f"البوت يحتاج صلاحيات **مدير** في المجموعة `{group_name}`\n\n"
                    f"الرجاء:\n"
                    f"1. اذهب إلى المجموعة\n"
                    f"2. اضغط على 'إدارة المجموعة'\n"
                    f"3. أضف البوت كمدير\n"
                    f"4. أعد المحاولة",
                    buttons=Buttons.main_menu()
                )
                return
            
            # الحصول على عدد الأعضاء
            members_count = getattr(chat, 'participants_count', 0)
            
            # إرسال رسالة البدء
            estimated_time = f"{members_count / 1000:.1f} دقيقة" if members_count > 0 else "غير معروف"
            
            # تخزين معرف الرسالة للاستخدام لاحقاً
            if hasattr(event, 'message') and event.message:
                self.last_messages[event.chat_id] = event.message.id
            
            await self.safe_edit_message(
                event,
                MESSAGES['backup_start'].format(
                    group_name=group_name,
                    group_id=group_id,
                    members_count=format_number(members_count),
                    estimated_time=estimated_time
                ),
                buttons=Buttons.progress_menu(group_id)
            )
            
            # تشغيل النسخ في الخلفية
            asyncio.create_task(self._run_backup(group_id, event.chat_id, event.id, group_name))
            
        except Exception as e:
            logger.error(f"خطأ في بدء النسخ: {e}")
            await self.safe_edit_message(event, f"❌ حدث خطأ: {str(e)}", buttons=Buttons.main_menu())
    
    async def _run_backup(self, group_id: int, chat_id: int, message_id: int, group_name: str):
        """تشغيل عملية النسخ الاحتياطي في الخلفية"""
        try:
            start_time = datetime.now()
            self.running_operations[group_id] = {'type': 'backup', 'start': start_time}
            
            # 1. حفظ معلومات المجموعة
            chat = await self.client.get_entity(group_id)
            group_info = {
                'title': getattr(chat, 'title', str(group_id)),
                'username': getattr(chat, 'username', None),
                'participants_count': getattr(chat, 'participants_count', 0),
                'is_megagroup': getattr(chat, 'megagroup', False),
                'created_date': datetime.now(),
                'backup_status': 'in_progress'
            }
            self.db.save_group(group_id, group_info)
            
            # 2. استخراج الأعضاء
            extract_result = await self.extractor.extract_all_members(group_id)
            
            if not extract_result or not extract_result.get('success'):
                error_msg = extract_result.get('error', 'خطأ غير معروف') if extract_result else 'خطأ غير معروف'
                # محاولة الحصول على الرسالة
                try:
                    # استخدام الرسالة المخزنة أو جلبها
                    msg = None
                    if chat_id in self.last_messages:
                        try:
                            msg = await self.client.get_messages(chat_id, ids=self.last_messages[chat_id])
                        except:
                            pass
                    if not msg and message_id:
                        try:
                            msg = await self.client.get_messages(chat_id, ids=message_id)
                        except:
                            pass
                    if msg:
                        await self.safe_edit_message(msg, f"❌ فشل النسخ الاحتياطي: {error_msg}", buttons=Buttons.main_menu())
                except Exception as e:
                    logger.error(f"فشل إرسال رسالة الخطأ: {e}")
                return
            
            # 3. تحديث الإحصائيات
            members_count = extract_result.get('total_members', 0)
            backup_size = self.backup_manager.get_backup_size()
            
            group_info['backup_status'] = 'completed'
            group_info['total_members_backup'] = members_count
            group_info['backup_size'] = backup_size if backup_size else 0
            self.db.save_group(group_id, group_info)
            
            # 4. إضافة للمراقبة
            self.monitored_groups[group_id] = {
                'name': group_info['title'],
                'backup_date': datetime.now(),
                'members': members_count
            }
            
            # 5. إرسال تقرير الإكمال
            duration = (datetime.now() - start_time).total_seconds()
            
            try:
                # استخدام الرسالة المخزنة أو جلبها
                msg = None
                if chat_id in self.last_messages:
                    try:
                        msg = await self.client.get_messages(chat_id, ids=self.last_messages[chat_id])
                    except:
                        pass
                if not msg and message_id:
                    try:
                        msg = await self.client.get_messages(chat_id, ids=message_id)
                    except:
                        pass
                if msg:
                    await self.safe_edit_message(
                        msg,
                        MESSAGES['backup_complete'].format(
                            group_name=group_info['title'],
                            members_count=format_number(members_count),
                            backup_size=format_bytes(backup_size),
                            duration=format_duration(duration)
                        ),
                        buttons=Buttons.main_menu()
                    )
            except Exception as e:
                logger.error(f"فشل إرسال رسالة الإكمال: {e}")
            
        except Exception as e:
            logger.error(f"خطأ في عملية النسخ: {e}")
            try:
                msg = None
                if chat_id in self.last_messages:
                    try:
                        msg = await self.client.get_messages(chat_id, ids=self.last_messages[chat_id])
                    except:
                        pass
                if not msg and message_id:
                    try:
                        msg = await self.client.get_messages(chat_id, ids=message_id)
                    except:
                        pass
                if msg:
                    await self.safe_edit_message(
                        msg,
                        f"❌ خطأ في النسخ الاحتياطي: {str(e)}",
                        buttons=Buttons.main_menu()
                    )
            except:
                pass
        finally:
            if group_id in self.running_operations:
                del self.running_operations[group_id]
        
    async def _start_restore(self, event, group_id: int):
        """بدء عملية الاستعادة"""
        try:
            group_data = self.db.get_group(group_id)
            if not group_data:
                await self.safe_edit_message(
                    event,
                    f"❌ لا توجد نسخة احتياطية للمجموعة `{group_id}`",
                    buttons=Buttons.main_menu()
                )
                return
            
            await self.safe_edit_message(
                event,
                f"🔄 **بدء استعادة المجموعة**\n\n"
                f"📛 **الاسم:** {group_data.get('title')}\n"
                f"👥 **الأعضاء:** {format_number(group_data.get('total_members_backup', 0))}\n\n"
                f"⏱️ الوقت المتوقع: {group_data.get('total_members_backup', 0) / 50:.0f} دقيقة\n\n"
                f"_سيتم إعلامك عند اكتمال الاستعادة_",
                buttons=Buttons.progress_menu(group_id)
            )
            
            asyncio.create_task(self._run_restore(group_id, event.chat_id, event.id))
            
        except Exception as e:
            logger.error(f"خطأ في بدء الاستعادة: {e}")
            await self.safe_edit_message(event, f"❌ حدث خطأ: {str(e)}", buttons=Buttons.main_menu())
    
    async def _run_restore(self, original_group_id: int, chat_id: int, message_id: int):
        """تشغيل عملية الاستعادة"""
        try:
            start_time = datetime.now()
            self.running_operations[original_group_id] = {'type': 'restore', 'start': start_time}
            
            group_data = self.db.get_group(original_group_id)
            if not group_data:
                try:
                    msg = await self.client.get_messages(chat_id, ids=message_id) if message_id else None
                    if msg:
                        await self.safe_edit_message(msg, "❌ لا توجد نسخة احتياطية", buttons=Buttons.main_menu())
                except:
                    pass
                return
            
            # إنشاء مجموعة جديدة
            me = await self.client.get_me()
            result = await self.client(CreateChatRequest(
                users=[me.username],
                title=group_data.get('title', f'Restored Group {original_group_id}')
            ))
            new_chat = result.chats[0]
            new_group_id = new_chat.id
            
            # ترقية إلى سوبر جروب
            try:
                upgrade = await self.client(MigrateChatRequest(new_chat.id))
                if upgrade and upgrade.chats:
                    new_group_id = upgrade.chats[0].id
            except:
                pass
            
            # استعادة الصورة
            if group_data.get('photo_path') and os.path.exists(group_data['photo_path']):
                try:
                    await self.client(EditPhotoRequest(
                        channel=new_group_id,
                        photo=await self.client.upload_file(group_data['photo_path'])
                    ))
                except Exception as e:
                    logger.error(f"فشل استعادة الصورة: {e}")
            
            # استعادة الوصف
            if group_data.get('about'):
                try:
                    await self.client(EditChatAboutRequest(
                        peer=new_group_id,
                        about=group_data['about']
                    ))
                except Exception as e:
                    logger.error(f"فشل استعادة الوصف: {e}")
            
            # استعادة العنوان
            if group_data.get('title'):
                try:
                    await self.client(EditTitleRequest(
                        channel=new_group_id,
                        title=group_data['title']
                    ))
                except Exception as e:
                    logger.error(f"فشل استعادة العنوان: {e}")
            
            # استعادة الأعضاء
            members_count = self.db.get_members_count(original_group_id)
            
            try:
                msg = await self.client.get_messages(chat_id, ids=message_id) if message_id else None
                if msg:
                    await self.safe_edit_message(
                        msg,
                        f"🔄 **جاري استعادة الأعضاء...**\n\n👥 جاري إضافة {format_number(members_count)} عضو\n⏱️ الوقت المتوقع: {members_count / 50:.0f} دقيقة",
                        buttons=Buttons.progress_menu(original_group_id)
                    )
            except:
                pass
            
            restore_result = await self.restorer.restore_all_members(
                original_group_id, new_group_id,
                max_members=group_data.get('total_members_backup')
            )
            
            group_link = f"https://t.me/c/{str(new_group_id)[4:]}"
            duration = (datetime.now() - start_time).total_seconds()
            total = restore_result.get('total', 1)
            restored = restore_result.get('restored', 0)
            success_rate = (restored / total * 100) if total > 0 else 0
            
            try:
                msg = await self.client.get_messages(chat_id, ids=message_id) if message_id else None
                if msg:
                    await self.safe_edit_message(
                        msg,
                        MESSAGES['restore_complete'].format(
                            group_name=group_data.get('title'),
                            group_link=group_link,
                            new_group_id=new_group_id,
                            restored_members=format_number(restored),
                            total_members=format_number(total),
                            success_rate=success_rate,
                            duration=format_duration(duration)
                        ),
                        buttons=Buttons.main_menu()
                    )
            except:
                pass
            
        except Exception as e:
            logger.error(f"خطأ في الاستعادة: {e}")
            try:
                msg = await self.client.get_messages(chat_id, ids=message_id) if message_id else None
                if msg:
                    await self.safe_edit_message(
                        msg,
                        f"❌ خطأ في الاستعادة: {str(e)}",
                        buttons=Buttons.main_menu()
                    )
            except:
                pass
        finally:
            if original_group_id in self.running_operations:
                del self.running_operations[original_group_id]
    
    async def _show_progress(self, event, group_id: int):
        """عرض تقدم عملية محددة"""
        progress = self.extractor.get_progress(group_id) if self.extractor else {}
        
        if progress and progress.get('total', 0) > 0:
            percentage = progress.get('percentage') or 0
            # 🔧 التصحيح: تأكد من أن percentage رقم وليس None
            if percentage is None:
                percentage = 0
            bar = create_progress_bar(percentage)
            message = f"""
    📊 **تقدم العملية للمجموعة {group_id}**

    {bar} {percentage:.1f}%

    👥 تم استخراج: {format_number(progress.get('processed', 0))} / {format_number(progress.get('total', 0))}
    ⏱️ الوقت المتبقي: {progress.get('remaining', 'غير معروف')}
    📈 السرعة: {progress.get('rate', 0):.1f} عضو/ثانية
    """
            await self.safe_edit_message(event, message, buttons=Buttons.progress_menu(group_id))
        else:
            await self.safe_edit_message(
                event,
                f"📭 **لا توجد عملية جارية للمجموعة {group_id}**",
                buttons=Buttons.main_menu()
            )
        
    async def _stop_operation(self, event, group_id: int):
        """إيقاف عملية محددة"""
        if self.extractor:
            self.extractor.stop()
        if self.restorer:
            self.restorer.stop()
        
        if group_id in self.running_operations:
            del self.running_operations[group_id]
        
        await event.answer(f"🛑 تم إيقاف العملية للمجموعة {group_id}")
        await self.safe_edit_message(
            event,
            f"🛑 **تم إيقاف العملية للمجموعة {group_id}**\n\n"
            "يمكنك البدء من جديد من القائمة الرئيسية.",
            buttons=Buttons.main_menu()
        )
    
    # ============== معالج حذف المجموعات ==============
    
    async def handle_group_deleted(self, event):
        """معالج حذف المجموعة"""
        try:
            if hasattr(event, 'user_left') and event.user_left:
                me = await self.client.get_me()
                if event.user_id == me.id:
                    chat_id = event.chat_id
                    
                    group_name = "غير معروف"
                    try:
                        group_data = self.db.get_group(chat_id)
                        if group_data:
                            group_name = group_data.get('title', str(chat_id))
                    except:
                        pass
                    
                    logger.warning(f"🚨 تم حذف المجموعة: {group_name} ({chat_id})")
                    self.db.log_operation(chat_id, 'group_deleted', 'detected', group_name)
                    
                    if chat_id in self.monitored_groups or self.db.get_group(chat_id):
                        try:
                            await self.client.send_message(
                                BACKUP_CHANNEL_ID,
                                f"🚨 **تم حذف المجموعة** 🚨\n\n"
                                f"📛 **الاسم:** {group_name}\n"
                                f"🆔 **المعرف:** `{chat_id}`\n"
                                f"👥 **الأعضاء:** {format_number(self.db.get_members_count(chat_id))}\n"
                                f"⏱️ **وقت الحذف:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                                f"🔄 جاري استعادة المجموعة تلقائياً..."
                            )
                        except:
                            pass
                        
                        asyncio.create_task(self._run_restore(chat_id, BACKUP_CHANNEL_ID, None))
                    
        except Exception as e:
            logger.error(f"خطأ في معالج حذف المجموعة: {e}")
    
    # ============== دوال مساعدة ==============
    
    async def send_error(self, event, error: Exception):
        """إرسال رسالة خطأ منسقة"""
        error_message = f"""
❌ **حدث خطأ**

**الخطأ:** {str(error)}
**الوقت:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

**الحلول المقترحة:**
• تأكد من أن البوت مدير في المجموعة
• تحقق من صحة المعرف/الرابط
• تأكد من الاتصال بالإنترنت
• أعد المحاولة لاحقاً

_للحصول على مساعدة إضافية، استخدم /help_
"""
        try:
            await self.safe_edit_message(event, error_message, buttons=Buttons.main_menu())
        except:
            try:
                await event.reply(error_message)
            except:
                pass
    
    async def run(self):
        """تشغيل البوت"""
        try:
            await self.start()
            logger.info("✅ البوت جاهز للعمل مع جميع المميزات")
            await self.client.run_until_disconnected()
        except Exception as e:
            logger.error(f"❌ خطأ في تشغيل البوت: {e}")
            raise

# ============== تشغيل البوت ==============
if __name__ == "__main__":
    bot_instance = UltimateRecoveryBot()
    
    try:
        asyncio.run(bot_instance.run())
    except KeyboardInterrupt:
        logger.info("🛑 تم إيقاف البوت بواسطة المستخدم")
    except Exception as e:
        logger.error(f"❌ خطأ غير متوقع: {e}")
    finally:
        logger.info("👋 إغلاق البوت...")