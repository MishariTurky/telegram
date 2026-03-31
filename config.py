"""
config.py - إعدادات البوت الاحترافي
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ============== إعدادات تيليجرام ==============
API_ID = int(os.getenv('API_ID', 31919986))
API_HASH = os.getenv('API_HASH', 'a9073c9d911edfa5e0e7e40e68a8ebdb')
BOT_TOKEN = os.getenv('BOT_TOKEN', '8626762836:AAEcceQ4sPvifOJFO8W5fjh5DY139YOSHyM')
BACKUP_CHANNEL_ID = int(os.getenv('BACKUP_CHANNEL_ID', -100123456789))

# ============== إعدادات الأداء ==============
class PerformanceConfig:
    """إعدادات أداء البوت"""
    
    # إعدادات استخراج الأعضاء
    MAX_MEMBERS_TO_BACKUP = 200000  # الحد الأقصى للأعضاء
    MEMBERS_BATCH_SIZE = 500  # حجم دفعة الاستخراج
    EXTRACT_DELAY = 0.3  # تأخير بين الاستخراجات (ثواني)
    CHECKPOINT_INTERVAL = 5000  # حفظ نقطة توقف كل X عضو
    
    # إعدادات استعادة الأعضاء
    RESTORE_BATCH_SIZE = 30  # حجم دفعة الاستعادة
    DELAY_BETWEEN_BATCHES = 60  # تأخير بين الدفعات (ثواني)
    DELAY_BETWEEN_MEMBERS = 1.2  # تأخير بين الأعضاء (ثواني)
    MAX_RETRIES = 5  # عدد محاولات إعادة المحاولة
    
    # إعدادات قاعدة البيانات
    DB_TIMEOUT = 30  # مهلة قاعدة البيانات
    DB_JOURNAL_MODE = 'WAL'  # وضع الكتابة المتوازية
    DB_CACHE_SIZE = -20000  # حجم الذاكرة المؤقتة (20MB)
    
    # إعدادات الوقت
    OPERATION_TIMEOUT = 3600  # مهلة العملية (ساعة)
    KEEP_ALIVE_INTERVAL = 30  # فترة إبقاء الاتصال (ثواني)

# ============== إعدادات الحسابات المساعدة ==============
HELPER_ACCOUNTS = [
    # أضف حسابات مساعدة هنا لتوزيع الحمل
    # {
    #     'phone': '+1234567890',
    #     'api_id': 1234567,
    #     'api_hash': 'hash_here',
    #     'session': 'helper_1.session'
    # },
]

# ============== إعدادات الإشعارات ==============
NOTIFICATION_SETTINGS = {
    'notify_creator': True,
    'notify_admins': True,
    'notify_backup_channel': True,
    'send_detailed_report': True,
    'report_interval_minutes': 30  # تقرير كل 30 دقيقة
}

# ============== إعدادات الأمان ==============
SECURITY_SETTINGS = {
    'max_operations_per_hour': 10,  # حد العمليات في الساعة
    'require_confirmation': True,  # طلب تأكيد قبل العمليات الكبيرة
    'log_all_actions': True,  # تسجيل جميع الإجراءات
    'encrypt_backup': False,  # تشفير النسخ الاحتياطية
    'allowed_users': []  # قائمة المستخدمين المسموح لهم (فارغ = الكل)
}

# ============== إعدادات التنسيق ==============
FORMAT_SETTINGS = {
    'time_format': '%Y-%m-%d %H:%M:%S',
    'number_format': '{:,}',
    'date_format': '%Y-%m-%d'
}

# ============== رسائل البوت ==============
MESSAGES = {
    'start': """
🚀 **بوت استعادة المجموعات الاحترافي** 🚀

أنا بوت متكامل لحماية واستعادة مجموعات التلجرام.

**📊 الإحصائيات الحالية:**
• مجموعات مراقبة: {monitored_groups}
• أعضاء محفوظين: {total_members:,}
• حجم النسخ: {backup_size} MB

**🎯 المميزات:**
✅ نسخ احتياطي كامل (حتى 200,000 عضو)
✅ استعادة تلقائية فورية
✅ إشعار للمالك والمشرفين
✅ نظام نقاط توقف ذكي
✅ دعم حسابات مساعدة للتوزيع

**📝 الأوامر:**
/backup - نسخ احتياطي فوري
/restore - استعادة مجموعة
/status - حالة النسخ الاحتياطية
/progress - تقدم العملية
/stop - إيقاف العملية
/settings - إعدادات البوت
/help - المساعدة
""",
    
    'backup_start': """
🔄 **بدء النسخ الاحتياطي**

📛 **الاسم:** {group_name}
🆔 **المعرف:** `{group_id}`
👥 **الأعضاء المتوقع:** {members_count:,}

⏱️ **الوقت المتوقع:** {estimated_time}
📊 **سيتم حفظ:** الصورة، الوصف، الأعضاء، الرسائل

_سيتم إعلامك عند اكتمال العملية_
""",
    
    'backup_complete': """
✅ **اكتمل النسخ الاحتياطي**

📛 **الاسم:** {group_name}
👥 **الأعضاء:** {members_count:,}
💬 **الرسائل:** {messages_count:,}
📎 **الوسائط:** {media_count:,}
💾 **حجم النسخ:** {backup_size} MB
⏱️ **المدة:** {duration}

_تم حفظ جميع البيانات بنجاح_
""",
    
    'group_deleted': """
🚨 **🚨 تنبيه عاجل - تم حذف المجموعة 🚨**

📛 **الاسم:** {group_name}
🆔 **المعرف الأصلي:** `{group_id}`
👥 **الأعضاء:** {members_count:,}
⏱️ **وقت الحذف:** {delete_time}

🔄 **جاري استعادة المجموعة الآن...**

_سيتم إرسال رابط المجموعة الجديدة فور اكتمال الاستعادة_
""",
    
    'restore_complete': """
✅ **تم استعادة المجموعة بنجاح**

📛 **الاسم:** {group_name}
🔗 **الرابط الجديد:** {group_link}
🆔 **المعرف الجديد:** `{new_group_id}`

📊 **إحصائيات الاستعادة:**
👥 **الأعضاء المستعادين:** {restored_members:,} / {total_members:,}
👑 **المشرفين:** {restored_admins:,}
💬 **الرسائل:** {restored_messages:,}
📎 **الوسائط:** {restored_media:,}
🔗 **الروابط:** {restored_links:,}

📈 **نسبة النجاح:** {success_rate:.1f}%
⏱️ **المدة:** {duration}

🎉 تم استعادة المجموعة بنجاح!
""",
    
    'error': """
❌ **حدث خطأ**

**الخطأ:** {error}
**الوقت:** {time}

🔧 **الحلول المقترحة:**
{ solutions }

_للحصول على مساعدة إضافية، استخدم /help_
"""
}