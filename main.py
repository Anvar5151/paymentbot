import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import aiofiles
import pandas as pd
from io import BytesIO

import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, FSInputFile
)
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramAPIError
from aiogram.utils.keyboard import InlineKeyboardBuilder
import pytz

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
class Config:
    BOT_TOKEN = os.getenv('BOT_TOKEN', '8261270411:AAFLFiFb5IUGP7qOnNwTXv9be-QTeaanzvQ')
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://u3iq5cgdg6i8iu:p0a421befaade36b354ef7c54ebd196a264d162be538a3b4f4e8c9d2ce3264ef4@c2fbt7u7f4htth.cluster-czz5s0kz4scl.eu-west-1.rds.amazonaws.com:5432/d50dkobgsj63t1')
    ADMIN_IDS = list(map(int, os.getenv('ADMIN_IDS', '385129620').split(',')))
    MANDATORY_CHANNEL = os.getenv('MANDATORY_CHANNEL', '@janob_targetog_kanali')
    PAYMENT_CARD = os.getenv('PAYMENT_CARD', '5614 6873 0354 0661')
    CARD_OWNER = os.getenv('CARD_OWNER', 'Anvar Raxmadullayev')

# States
class RegistrationStates(StatesGroup):
    waiting_phone = State()
    waiting_name = State()
    waiting_age = State()
    waiting_region = State()
    waiting_height = State()
    waiting_weight = State()

class PaymentStates(StatesGroup):
    course_selection = State()
    waiting_receipt = State()
    pending_approval = State()

class AdminStates(StatesGroup):
    waiting_user_message = State()
    waiting_broadcast_message = State()
    waiting_user_id = State()

# Course configurations
COURSES = {
    'mustaqil': {
        'name': '🌟 Mustaqil',
        'price': 197000,
        'description': '✅ 21 kunlik dastur\n✅ Kunlik vazifalar\n✅ Ovqatlanish rejasi\n✅ Sport mashqlari',
        'channel_id': '@mustaqil_kurs'
    },
    'premium': {
        'name': '💎 Premium', 
        'price': 397000,
        'description': '✅ 21 kunlik dastur\n✅ Shaxsiy konsultatsiya\n✅ WhatsApp guruh\n✅ Haftalik nazorat',
        'channel_id': '@premium_kurs'
    },
    'vip': {
        'name': '👑 VIP',
        'price': 597000, 
        'description': '✅ 21 kunlik dastur\n✅ 1:1 mentor\n✅ Video qo\'ng\'iroqlar\n✅ Shaxsiy rejim',
        'channel_id': '@vip_kurs'
    }
}

REJECTION_REASONS = [
    "Noto'g'ri summa ko'rsatilgan",
    "Chek aniq emas yoki o'qib bo'lmaydi", 
    "Boshqa odamning cheki yuborilgan",
    "Chek soxta yoki tahrirlangan"
]

UZBEK_REGIONS = [
    "Toshkent shahri", "Toshkent viloyati", "Andijon", "Buxoro", 
    "Farg'ona", "Jizzax", "Xorazm", "Namangan", "Navoiy", 
    "Qashqadaryo", "Qoraqalpog'iston", "Samarqand", "Sirdaryo", "Surxondaryo"
]

# Messages
MESSAGES = {
    'welcome': "👋 Salom! Ozish marafoniga xush kelibsiz!\n\n📱 Iltimos, telefon raqamingizni ulashing:",
    'request_name': "✍️ Ism va familiyangizni kiriting:",
    'request_age': "🎂 Yoshingizni kiriting (13-80):",
    'request_region': "📍 Qaysi viloyatdansiz?",
    'request_height': "📏 Bo'yingizni kiriting (120-220 sm):",
    'request_weight': "⚖️ Vazningizni kiriting (30-300 kg):",
    'registration_complete': "✅ Ro'yxatdan o'tish yakunlandi!\n\nEndi kurs tarifini tanlang:",
    'payment_pending': "⏳ To'lovingiz tekshirilmoqda. Tez orada javob beramiz!",
    'payment_approved': "✅ To'lovingiz tasdiqlandi! Kursga xush kelibsiz!",
    'payment_rejected': "❌ To'lovingiz rad etildi.\nSabab: {reason}\n\nQaytadan urinib ko'ring.",
    'invalid_input': "❗️ Noto'g'ri ma'lumot. Iltimos, qayta kiriting:",
    'subscription_required': "📢 Botdan foydalanish uchun avval kanalimizga obuna bo'ling:",
    'card_copied': "✅ Karta raqami nusxalandi!",
    'receipt_received': "✅ Chek qabul qilindi! Admin tekshiradi.",
    'select_course': "📚 Kurs turini tanlang:",
    'payment_info': "💳 To'lov ma'lumotlari:\nKarta: {card}\nEgasi: {owner}\nSumma: {amount:,} so'm\n\n📋 Karta raqamini nusxalash uchun tugmani bosing\n\n💰 Pul o'tkazganingizdan so'ng chekni yuboring:"
}

# Database class
class Database:
    def __init__(self, url: str):
        self.url = url
        self.pool = None

    async def init_pool(self):
        self.pool = await asyncpg.create_pool(self.url, min_size=2, max_size=10)
        await self.create_tables()

    async def create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    phone VARCHAR(20) NOT NULL,
                    full_name VARCHAR(100) NOT NULL,
                    age INTEGER NOT NULL,
                    region VARCHAR(50) NOT NULL,
                    height INTEGER NOT NULL,
                    weight INTEGER NOT NULL,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_subscribed BOOLEAN DEFAULT FALSE
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id),
                    course_type VARCHAR(20) NOT NULL,
                    amount INTEGER NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    receipt_file_id VARCHAR(200),
                    submission_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    approval_date TIMESTAMP,
                    admin_id BIGINT,
                    rejection_reason VARCHAR(200)
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_actions (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT NOT NULL,
                    action_type VARCHAR(50) NOT NULL,
                    target_user_id BIGINT,
                    details TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

    async def add_user(self, user_data: Dict[str, Any]) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO users (user_id, phone, full_name, age, region, height, weight)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (user_id) DO UPDATE SET
                        phone = EXCLUDED.phone,
                        full_name = EXCLUDED.full_name,
                        age = EXCLUDED.age,
                        region = EXCLUDED.region,
                        height = EXCLUDED.height,
                        weight = EXCLUDED.weight
                """, *user_data.values())
                return True
        except Exception as e:
            logger.error(f"Error adding user: {e}")
            return False

    async def get_user(self, user_id: int) -> Optional[Dict]:
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting user: {e}")
            return None

    async def add_payment(self, payment_data: Dict[str, Any]) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO payments (user_id, course_type, amount, receipt_file_id)
                    VALUES ($1, $2, $3, $4)
                """, *payment_data.values())
                return True
        except Exception as e:
            logger.error(f"Error adding payment: {e}")
            return False

    async def update_payment_status(self, payment_id: int, status: str, admin_id: int, reason: str = None) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE payments 
                    SET status = $1, admin_id = $2, approval_date = CURRENT_TIMESTAMP, rejection_reason = $3
                    WHERE id = $4
                """, status, admin_id, reason, payment_id)
                return True
        except Exception as e:
            logger.error(f"Error updating payment: {e}")
            return False

    async def get_pending_payments(self) -> list:
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT p.*, u.full_name, u.phone 
                    FROM payments p 
                    JOIN users u ON p.user_id = u.user_id 
                    WHERE p.status = 'pending'
                    ORDER BY p.submission_date DESC
                """)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting pending payments: {e}")
            return []

    async def get_payment_by_id(self, payment_id: int) -> Optional[Dict]:
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT p.*, u.full_name, u.phone 
                    FROM payments p 
                    JOIN users u ON p.user_id = u.user_id 
                    WHERE p.id = $1
                """, payment_id)
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting payment: {e}")
            return None

    async def get_statistics(self) -> Dict[str, Any]:
        try:
            async with self.pool.acquire() as conn:
                # Total users
                total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
                
                # Users today
                users_today = await conn.fetchval("""
                    SELECT COUNT(*) FROM users 
                    WHERE registration_date >= CURRENT_DATE
                """)
                
                # Users this month
                users_month = await conn.fetchval("""
                    SELECT COUNT(*) FROM users 
                    WHERE registration_date >= date_trunc('month', CURRENT_DATE)
                """)
                
                # Payments by course
                payments_by_course = await conn.fetch("""
                    SELECT course_type, COUNT(*) as count, SUM(amount) as total
                    FROM payments WHERE status = 'approved'
                    GROUP BY course_type
                """)
                
                # Revenue stats
                total_revenue = await conn.fetchval("""
                    SELECT COALESCE(SUM(amount), 0) FROM payments WHERE status = 'approved'
                """)
                
                revenue_today = await conn.fetchval("""
                    SELECT COALESCE(SUM(amount), 0) FROM payments 
                    WHERE status = 'approved' AND approval_date >= CURRENT_DATE
                """)
                
                revenue_month = await conn.fetchval("""
                    SELECT COALESCE(SUM(amount), 0) FROM payments 
                    WHERE status = 'approved' AND approval_date >= date_trunc('month', CURRENT_DATE)
                """)

                return {
                    'total_users': total_users,
                    'users_today': users_today,
                    'users_month': users_month,
                    'payments_by_course': [dict(row) for row in payments_by_course],
                    'total_revenue': total_revenue,
                    'revenue_today': revenue_today,
                    'revenue_month': revenue_month
                }
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}

    async def get_all_users_for_export(self) -> list:
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT u.*, p.course_type, p.amount, p.status, p.submission_date
                    FROM users u
                    LEFT JOIN payments p ON u.user_id = p.user_id
                    ORDER BY u.registration_date DESC
                """)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting users for export: {e}")
            return []

    async def log_admin_action(self, admin_id: int, action_type: str, target_user_id: int = None, details: str = None):
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO admin_actions (admin_id, action_type, target_user_id, details)
                    VALUES ($1, $2, $3, $4)
                """, admin_id, action_type, target_user_id, details)
        except Exception as e:
            logger.error(f"Error logging admin action: {e}")

# Helper functions
def format_number(num: int) -> str:
    return f"{num:,}".replace(',', ' ')

def validate_phone(phone: str) -> bool:
    return bool(re.match(r'^\+998\d{9}$', phone))

def validate_name(name: str) -> bool:
    return bool(re.match(r'^[a-zA-ZА-Яа-я\s]{2,50}$', name.strip()))

def validate_age(age_str: str) -> tuple[bool, int]:
    try:
        age = int(age_str)
        return 13 <= age <= 80, age
    except ValueError:
        return False, 0

def validate_height(height_str: str) -> tuple[bool, int]:
    try:
        height = int(height_str)
        return 120 <= height <= 220, height
    except ValueError:
        return False, 0

def validate_weight(weight_str: str) -> tuple[bool, int]:
    try:
        weight = int(weight_str)
        return 30 <= weight <= 300, weight
    except ValueError:
        return False, 0

async def check_subscription(bot: Bot, user_id: int, channel: str) -> bool:
    try:
        member = await bot.get_chat_member(channel, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except TelegramAPIError:
        return False

def get_regions_keyboard():
    builder = InlineKeyboardBuilder()
    for region in UZBEK_REGIONS:
        builder.button(text=region, callback_data=f"region:{region}")
    builder.adjust(2)
    return builder.as_markup()

def get_courses_keyboard():
    builder = InlineKeyboardBuilder()
    for course_key, course_info in COURSES.items():
        builder.button(
            text=f"{course_info['name']} - {format_number(course_info['price'])} so'm",
            callback_data=f"course:{course_key}"
        )
    builder.adjust(1)
    return builder.as_markup()

def get_payment_keyboard(course_key: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="📋 Karta raqamini nusxalash", callback_data=f"copy_card:{course_key}")
    builder.button(text="📸 Chekni yuborish", callback_data=f"send_receipt:{course_key}")
    builder.adjust(1)
    return builder.as_markup()

def get_admin_payment_keyboard(payment_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Tasdiqlash", callback_data=f"approve:{payment_id}")
    builder.button(text="❌ Rad etish", callback_data=f"reject:{payment_id}")
    builder.adjust(2)
    return builder.as_markup()

def get_rejection_keyboard(payment_id: int):
    builder = InlineKeyboardBuilder()
    for i, reason in enumerate(REJECTION_REASONS):
        builder.button(text=reason, callback_data=f"reject_reason:{payment_id}:{i}")
    builder.adjust(1)
    return builder.as_markup()

def get_admin_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Statistika", callback_data="admin:stats")
    builder.button(text="👥 Foydalanuvchilar", callback_data="admin:users")
    builder.button(text="💬 Xabar yuborish", callback_data="admin:message")
    builder.button(text="📢 Umumiy xabar", callback_data="admin:broadcast")
    builder.button(text="📋 Excel export", callback_data="admin:export")
    builder.button(text="💳 To'lovlar", callback_data="admin:payments")
    builder.adjust(2)
    return builder.as_markup()

# Initialize
bot = Bot(token=Config.BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
db = Database(Config.DATABASE_URL)

# Handlers
@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    # Check subscription
    if not await check_subscription(bot, user_id, Config.MANDATORY_CHANNEL):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Kanalga obuna bo'lish", url=f"https://t.me/{Config.MANDATORY_CHANNEL[1:]}")],
            [InlineKeyboardButton(text="✅ Obunani tekshirish", callback_data="check_subscription")]
        ])
        await message.answer(MESSAGES['subscription_required'], reply_markup=keyboard)
        return

    # Check if user exists
    user = await db.get_user(user_id)
    if user:
        await message.answer(MESSAGES['registration_complete'], reply_markup=get_courses_keyboard())
        await state.set_state(PaymentStates.course_selection)
    else:
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Telefon raqamni ulashish", request_contact=True)]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await message.answer(MESSAGES['welcome'], reply_markup=keyboard)
        await state.set_state(RegistrationStates.waiting_phone)

@router.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if await check_subscription(bot, user_id, Config.MANDATORY_CHANNEL):
        await callback.message.delete()
        user = await db.get_user(user_id)
        if user:
            await callback.message.answer(MESSAGES['registration_complete'], reply_markup=get_courses_keyboard())
            await state.set_state(PaymentStates.course_selection)
        else:
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="📱 Telefon raqamni ulashish", request_contact=True)]],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await callback.message.answer(MESSAGES['welcome'], reply_markup=keyboard)
            await state.set_state(RegistrationStates.waiting_phone)
    else:
        await callback.answer("❌ Siz hali kanalga obuna bo'lmadingiz!", show_alert=True)

@router.message(RegistrationStates.waiting_phone, F.contact)
async def phone_handler(message: Message, state: FSMContext):
    phone = message.contact.phone_number
    if not phone.startswith('+'):
        phone = '+' + phone
    
    if not validate_phone(phone):
        await message.answer("❗️ Iltimos, O'zbekiston raqamini yuboring!")
        return
    
    await state.update_data(user_id=message.from_user.id, phone=phone)
    await message.answer(MESSAGES['request_name'], reply_markup=ReplyKeyboardRemove())
    await state.set_state(RegistrationStates.waiting_name)

@router.message(RegistrationStates.waiting_name)
async def name_handler(message: Message, state: FSMContext):
    name = message.text.strip()
    
    if not validate_name(name):
        await message.answer("❗️ Ism va familiya 2-50 ta harf bo'lishi kerak!")
        return
    
    await state.update_data(full_name=name)
    await message.answer(MESSAGES['request_age'])
    await state.set_state(RegistrationStates.waiting_age)

@router.message(RegistrationStates.waiting_age)
async def age_handler(message: Message, state: FSMContext):
    is_valid, age = validate_age(message.text)
    
    if not is_valid:
        await message.answer("❗️ Yosh 13 dan 80 gacha bo'lishi kerak!")
        return
    
    await state.update_data(age=age)
    await message.answer(MESSAGES['request_region'], reply_markup=get_regions_keyboard())
    await state.set_state(RegistrationStates.waiting_region)

@router.callback_query(RegistrationStates.waiting_region, F.data.startswith("region:"))
async def region_handler(callback: CallbackQuery, state: FSMContext):
    region = callback.data.split(":", 1)[1]
    
    await state.update_data(region=region)
    await callback.message.edit_text(MESSAGES['request_height'])
    await state.set_state(RegistrationStates.waiting_height)

@router.message(RegistrationStates.waiting_height)
async def height_handler(message: Message, state: FSMContext):
    is_valid, height = validate_height(message.text)
    
    if not is_valid:
        await message.answer("❗️ Bo'y 120 dan 220 sm gacha bo'lishi kerak!")
        return
    
    await state.update_data(height=height)
    await message.answer(MESSAGES['request_weight'])
    await state.set_state(RegistrationStates.waiting_weight)

@router.message(RegistrationStates.waiting_weight)
async def weight_handler(message: Message, state: FSMContext):
    is_valid, weight = validate_weight(message.text)
    
    if not is_valid:
        await message.answer("❗️ Vazn 30 dan 300 kg gacha bo'lishi kerak!")
        return
    
    await state.update_data(weight=weight)
    
    # Save user to database
    data = await state.get_data()
    if await db.add_user(data):
        await message.answer(MESSAGES['registration_complete'], reply_markup=get_courses_keyboard())
        await state.set_state(PaymentStates.course_selection)
    else:
        await message.answer("❗️ Xatolik yuz berdi. Qaytadan urinib ko'ring.")
        await state.clear()

@router.callback_query(PaymentStates.course_selection, F.data.startswith("course:"))
async def course_selection_handler(callback: CallbackQuery, state: FSMContext):
    course_key = callback.data.split(":", 1)[1]
    course = COURSES[course_key]
    
    await state.update_data(selected_course=course_key)
    
    text = f"{course['name']}\n\n{course['description']}\n\n💰 Narxi: {format_number(course['price'])} so'm"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 To'lov qilish", callback_data=f"pay:{course_key}")],
        [InlineKeyboardButton(text="🔙 Ortga", callback_data="back_to_courses")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard)

@router.callback_query(F.data == "back_to_courses")
async def back_to_courses_handler(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(MESSAGES['select_course'], reply_markup=get_courses_keyboard())

@router.callback_query(F.data.startswith("pay:"))
async def payment_handler(callback: CallbackQuery, state: FSMContext):
    course_key = callback.data.split(":", 1)[1]
    course = COURSES[course_key]
    
    text = MESSAGES['payment_info'].format(
        card=Config.PAYMENT_CARD,
        owner=Config.CARD_OWNER,
        amount=course['price']
    )
    await callback.message.edit_text(text, reply_markup=get_payment_keyboard(course_key))

@router.callback_query(F.data.startswith("copy_card:"))
async def copy_card_handler(callback: CallbackQuery):
    await callback.answer(MESSAGES['card_copied'], show_alert=True)

@router.callback_query(F.data.startswith("send_receipt:"))
async def send_receipt_handler(callback: CallbackQuery, state: FSMContext):
    course_key = callback.data.split(":", 1)[1]
    await state.update_data(selected_course=course_key)
    
    await callback.message.edit_text("📸 Chek rasmini yoki faylini yuboring:")
    await state.set_state(PaymentStates.waiting_receipt)

@router.message(PaymentStates.waiting_receipt, F.content_type.in_({'photo', 'document'}))
async def receipt_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    course_key = data['selected_course']
    course = COURSES[course_key]
    
    # Get file ID
    if message.photo:
        file_id = message.photo[-1].file_id
    else:
        file_id = message.document.file_id
    
    # Save payment to database
    payment_data = {
        'user_id': message.from_user.id,
        'course_type': course_key,
        'amount': course['price'],
        'receipt_file_id': file_id
    }
    
    if await db.add_payment(payment_data):
        await message.answer(MESSAGES['receipt_received'])
        await message.answer(MESSAGES['payment_pending'])
        
        # Notify admins
        user = await db.get_user(message.from_user.id)
        admin_text = f"🆕 Yangi to'lov!\n\n"
        admin_text += f"👤 Foydalanuvchi: {user['full_name']}\n"
        admin_text += f"📱 Telefon: {user['phone']}\n"
        admin_text += f"💰 Kurs: {course['name']}\n"
        admin_text += f"💵 Summa: {format_number(course['price'])} so'm\n"
        admin_text += f"📅 Vaqt: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        # Get payment ID for admin keyboard
        payments = await db.get_pending_payments()
        payment_id = payments[0]['id'] if payments else 0
        
        for admin_id in Config.ADMIN_IDS:
            try:
                if message.photo:
                    await bot.send_photo(
                        admin_id, 
                        file_id, 
                        caption=admin_text,
                        reply_markup=get_admin_payment_keyboard(payment_id)
                    )
                else:
                    await bot.send_document(
                        admin_id,
                        file_id,
                        caption=admin_text,
                        reply_markup=get_admin_payment_keyboard(payment_id)
                    )
            except TelegramAPIError as e:
                logger.error(f"Error sending to admin {admin_id}: {e}")
        
        await state.set_state(PaymentStates.pending_approval)
    else:
        await message.answer("❗️ Xatolik yuz berdi. Qaytadan urinib ko'ring.")

# Admin handlers
@router.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id not in Config.ADMIN_IDS:
        return
    
    await message.answer("🔧 Admin Panel", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("approve:"))
async def approve_payment(callback: CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    
    payment_id = int(callback.data.split(":", 1)[1])
    payment = await db.get_payment_by_id(payment_id)
    
    if not payment:
        await callback.answer("❌ To'lov topilmadi!", show_alert=True)
        return
    
    if await db.update_payment_status(payment_id, 'approved', callback.from_user.id):
        # Generate invite link
        course = COURSES[payment['course_type']]
        try:
            invite_link = await bot.create_chat_invite_link(
                course['channel_id'],
                expire_date=datetime.now() + timedelta(hours=24),
                member_limit=1,
                name=f"User_{payment['user_id']}"
            )
            
            # Send approval message to user
            approval_text = f"{MESSAGES['payment_approved']}\n\n"
            approval_text += f"🎯 Kurs: {course['name']}\n"
            approval_text += f"🔗 Kanalga kirish: {invite_link.invite_link}\n\n"
            approval_text += f"⚠️ Havola 24 soat amal qiladi!"
            
            await bot.send_message(payment['user_id'], approval_text)
            
            # Update callback message
            await callback.message.edit_caption(
                f"✅ TASDIQLANDI\n\n{callback.message.caption}",
                reply_markup=None
            )
            
            await db.log_admin_action(
                callback.from_user.id, 
                'approve_payment', 
                payment['user_id'],
                f"Payment ID: {payment_id}, Course: {payment['course_type']}"
            )
            
        except TelegramAPIError as e:
            logger.error(f"Error creating invite link: {e}")
            await callback.answer("❌ Xatolik yuz berdi!", show_alert=True)
    else:
        await callback.answer("❌ Ma'lumotlar bazasida xatolik!", show_alert=True)

@router.callback_query(F.data.startswith("reject:"))
async def reject_payment_menu(callback: CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    
    payment_id = int(callback.data.split(":", 1)[1])
    await callback.message.edit_reply_markup(reply_markup=get_rejection_keyboard(payment_id))

@router.callback_query(F.data.startswith("reject_reason:"))
async def reject_payment(callback: CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    
    parts = callback.data.split(":")
    payment_id = int(parts[1])
    reason_index = int(parts[2])
    reason = REJECTION_REASONS[reason_index]
    
    payment = await db.get_payment_by_id(payment_id)
    
    if not payment:
        await callback.answer("❌ To'lov topilmadi!", show_alert=True)
        return
    
    if await db.update_payment_status(payment_id, 'rejected', callback.from_user.id, reason):
        # Send rejection message to user
        rejection_text = MESSAGES['payment_rejected'].format(reason=reason)
        await bot.send_message(payment['user_id'], rejection_text)
        
        # Update callback message
        await callback.message.edit_caption(
            f"❌ RAD ETILDI\nSabab: {reason}\n\n{callback.message.caption}",
            reply_markup=None
        )
        
        await db.log_admin_action(
            callback.from_user.id,
            'reject_payment', 
            payment['user_id'],
            f"Payment ID: {payment_id}, Reason: {reason}"
        )
        
    else:
        await callback.answer("❌ Ma'lumotlar bazasida xatolik!", show_alert=True)

@router.callback_query(F.data == "admin:stats")
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    
    stats = await db.get_statistics()
    
    text = "📊 STATISTIKA\n\n"
    text += f"👥 Jami foydalanuvchilar: {stats.get('total_users', 0)}\n"
    text += f"🆕 Bugungi yangi: {stats.get('users_today', 0)}\n"
    text += f"📅 Oylik yangi: {stats.get('users_month', 0)}\n\n"
    
    text += "💰 DAROMAD:\n"
    text += f"💵 Jami: {format_number(stats.get('total_revenue', 0))} so'm\n"
    text += f"📅 Bugun: {format_number(stats.get('revenue_today', 0))} so'm\n"
    text += f"📊 Oylik: {format_number(stats.get('revenue_month', 0))} so'm\n\n"
    
    text += "📚 KURSLAR BO'YICHA:\n"
    for course_stat in stats.get('payments_by_course', []):
        course_name = COURSES.get(course_stat['course_type'], {}).get('name', course_stat['course_type'])
        text += f"{course_name}: {course_stat['count']} ta - {format_number(course_stat['total'])} so'm\n"
    
    await callback.message.edit_text(text, reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "admin:message")
async def admin_message_menu(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    
    await callback.message.edit_text("👤 Foydalanuvchi ID sini kiriting:")
    await state.set_state(AdminStates.waiting_user_id)

@router.message(AdminStates.waiting_user_id)
async def get_user_id(message: Message, state: FSMContext):
    if message.from_user.id not in Config.ADMIN_IDS:
        return
    
    try:
        user_id = int(message.text.strip())
        user = await db.get_user(user_id)
        
        if user:
            await state.update_data(target_user_id=user_id)
            await message.answer(f"✅ Foydalanuvchi topildi: {user['full_name']}\n\nXabaringizni kiriting:")
            await state.set_state(AdminStates.waiting_user_message)
        else:
            await message.answer("❌ Foydalanuvchi topilmadi!")
            await state.clear()
    except ValueError:
        await message.answer("❌ Noto'g'ri ID format!")

@router.message(AdminStates.waiting_user_message)
async def send_user_message(message: Message, state: FSMContext):
    if message.from_user.id not in Config.ADMIN_IDS:
        return
    
    data = await state.get_data()
    target_user_id = data['target_user_id']
    user = await db.get_user(target_user_id)
    
    try:
        admin_message = f"📢 Admin xabari:\n\n{message.text}\n\n"
        admin_message += f"Salom, {user['full_name']}!"
        
        if message.photo:
            await bot.send_photo(target_user_id, message.photo[-1].file_id, caption=admin_message)
        elif message.video:
            await bot.send_video(target_user_id, message.video.file_id, caption=admin_message)
        else:
            await bot.send_message(target_user_id, admin_message)
        
        await message.answer("✅ Xabar yuborildi!")
        await db.log_admin_action(message.from_user.id, 'send_message', target_user_id, message.text[:100])
        
    except TelegramAPIError as e:
        await message.answer(f"❌ Xabar yuborishda xatolik: {e}")
    
    await state.clear()

@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_menu(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    
    await callback.message.edit_text("📢 Barcha foydalanuvchilarga yuborish uchun xabaringizni kiriting:")
    await state.set_state(AdminStates.waiting_broadcast_message)

@router.message(AdminStates.waiting_broadcast_message)
async def broadcast_message(message: Message, state: FSMContext):
    if message.from_user.id not in Config.ADMIN_IDS:
        return
    
    # Get all users
    users = await db.get_all_users_for_export()
    user_ids = list(set([user['user_id'] for user in users]))
    
    success_count = 0
    failed_count = 0
    
    status_msg = await message.answer(f"📤 Yuborilmoqda... 0/{len(user_ids)}")
    
    for i, user_id in enumerate(user_ids):
        try:
            user = await db.get_user(user_id)
            if user:
                broadcast_text = f"📢 Umumiy xabar:\n\n{message.text}\n\n"
                broadcast_text += f"Salom, {user['full_name']}!"
                
                if message.photo:
                    await bot.send_photo(user_id, message.photo[-1].file_id, caption=broadcast_text)
                elif message.video:
                    await bot.send_video(user_id, message.video.file_id, caption=broadcast_text)
                else:
                    await bot.send_message(user_id, broadcast_text)
                
                success_count += 1
            
            # Update status every 10 messages
            if (i + 1) % 10 == 0:
                await status_msg.edit_text(f"📤 Yuborilmoqda... {i + 1}/{len(user_ids)}")
                
        except TelegramAPIError:
            failed_count += 1
            continue
        
        # Small delay to avoid rate limits
        await asyncio.sleep(0.05)
    
    await status_msg.edit_text(
        f"✅ Xabar yuborish tugallandi!\n\n"
        f"✅ Muvaffaqiyatli: {success_count}\n"
        f"❌ Muvaffaqiyatsiz: {failed_count}"
    )
    
    await db.log_admin_action(message.from_user.id, 'broadcast', None, f"Sent to {success_count} users")
    await state.clear()

@router.callback_query(F.data == "admin:export")
async def admin_export(callback: CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    
    try:
        users_data = await db.get_all_users_for_export()
        
        if not users_data:
            await callback.answer("❌ Ma'lumotlar topilmadi!", show_alert=True)
            return
        
        # Create DataFrame
        df = pd.DataFrame(users_data)
        
        # Format data
        if 'registration_date' in df.columns:
            df['registration_date'] = pd.to_datetime(df['registration_date']).dt.strftime('%Y-%m-%d %H:%M')
        if 'submission_date' in df.columns:
            df['submission_date'] = pd.to_datetime(df['submission_date']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Create Excel file
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Foydalanuvchilar', index=False)
        
        excel_buffer.seek(0)
        
        # Send file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"users_export_{timestamp}.xlsx"
        
        await bot.send_document(
            callback.from_user.id,
            document=FSInputFile(excel_buffer, filename=filename),
            caption=f"📊 Foydalanuvchilar ma'lumotlari\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        
        await db.log_admin_action(callback.from_user.id, 'export_data', None, f"Exported {len(users_data)} records")
        
    except Exception as e:
        logger.error(f"Export error: {e}")
        await callback.answer("❌ Export qilishda xatolik!", show_alert=True)

@router.callback_query(F.data == "admin:payments")
async def admin_payments(callback: CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    
    pending_payments = await db.get_pending_payments()
    
    if not pending_payments:
        await callback.message.edit_text("✅ Kutilayotgan to'lovlar yo'q", reply_markup=get_admin_keyboard())
        return
    
    text = f"⏳ Kutilayotgan to'lovlar: {len(pending_payments)}\n\n"
    
    for payment in pending_payments[:5]:  # Show first 5
        course_name = COURSES.get(payment['course_type'], {}).get('name', payment['course_type'])
        text += f"👤 {payment['full_name']}\n"
        text += f"💰 {course_name} - {format_number(payment['amount'])} so'm\n"
        text += f"📅 {payment['submission_date'].strftime('%Y-%m-%d %H:%M')}\n"
        text += "─────────────\n"
    
    if len(pending_payments) > 5:
        text += f"... va yana {len(pending_payments) - 5} ta"
    
    await callback.message.edit_text(text, reply_markup=get_admin_keyboard())

# Error handlers
@router.message()
async def handle_unknown_message(message: Message, state: FSMContext):
    current_state = await state.get_state()
    
    if current_state is None:
        # User not in any flow, redirect to start
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Boshiga qaytish", callback_data="restart")]
        ])
        await message.answer("❓ Noma'lum buyruq. Boshidan boshlang.", reply_markup=keyboard)
    else:
        await message.answer(MESSAGES['invalid_input'])

@router.callback_query(F.data == "restart")
async def restart_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    
    # Simulate /start command
    message = callback.message
    message.from_user = callback.from_user
    await start_handler(message, state)

# Main function
async def main():
    # Initialize database
    await db.init_pool()
    
    # Register router
    dp.include_router(router)
    
    # Error handling middleware
    @dp.error()
    async def error_handler(event, data):
        logger.error(f"Update error: {event.exception}")
        return True
    
    # Start polling
    try:
        logger.info("Bot started successfully!")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"Bot error: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")