import asyncio
import logging
import json
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from database import Database
from config import Config
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=Config.BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
db = Database()


# ─── STATES ────────────────────────────────────────────────────────────────────

class AdminStates(StatesGroup):
    broadcast_text   = State()
    delivery_data    = State()


# ─── KEYBOARDS ─────────────────────────────────────────────────────────────────

def main_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🛍 Открыть магазин", web_app=WebAppInfo(url=Config.WEBAPP_URL)))
    kb.add(InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders"))
    kb.add(InlineKeyboardButton("⭐ Мой уровень", callback_data="my_level"))
    kb.add(InlineKeyboardButton("💬 Поддержка", url=f"https://t.me/{Config.SUPPORT_USERNAME}"))
    return kb

def admin_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📊 Статистика",      callback_data="adm_stats"),
        InlineKeyboardButton("📦 Активные заказы", callback_data="adm_orders"),
        InlineKeyboardButton("👥 Пользователи",    callback_data="adm_users"),
        InlineKeyboardButton("📢 Рассылка",        callback_data="adm_broadcast"),
    )
    return kb

def back_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("◀️ Назад", callback_data="adm_main"))
    return kb


# ─── /START ────────────────────────────────────────────────────────────────────

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    user = message.from_user
    await db.upsert_user(user.id, user.username or "", user.full_name)

    # Реферальная система
    args = message.get_args()
    if args and args.startswith('ref_'):
        ref_code = args[4:]
        await db.apply_referral(user.id, ref_code)

    if user.id in Config.ADMIN_IDS:
        await message.answer(
            f"👑 Привет, <b>{user.first_name}</b>!\n\nИспользуй /admin для управления.",
            reply_markup=main_kb()
        )
    else:
        await message.answer(
            f"👋 Привет, <b>{user.first_name}</b>!\n\n"
            f"🔑 <b>KeyFlow</b> — зарубежные подписки из России\n\n"
            f"✅ Spotify, ChatGPT, Claude, Discord и другие\n"
            f"✅ Оплата СБП\n"
            f"✅ Выдача за 15 минут · Поддержка 24/7",
            reply_markup=main_kb()
        )


# ─── /ADMIN ────────────────────────────────────────────────────────────────────

@dp.message_handler(commands=['level'])
async def cmd_level(message: types.Message):
    info = await db.get_user_level_info(message.from_user.id)
    if not info:
        await message.answer("Информация не найдена. Напиши /start")
        return

    current = info.get('current_level')
    next_lvl = info.get('next_level')
    spent = info.get('total_spent', 0)
    promos = info.get('promos', [])

    if current:
        level_text = f"Твой уровень: <b>{current['name']}</b>"
    else:
        level_text = "Уровень: <b>Нет уровня</b>"

    if next_lvl:
        need = next_lvl['min_spent'] - spent
        progress = info.get('progress_pct', 0)
        bar_filled = int(progress / 10)
        bar = '█' * bar_filled + '░' * (10 - bar_filled)
        next_text = (
            f"\n\n📈 До уровня <b>{next_lvl['name']}</b>:\n"
            f"{bar} {progress}%\n"
            f"Осталось потратить: <b>{int(need)}₽</b>"
        )
    else:
        next_text = "\n\n🏆 Ты достиг максимального уровня!"

    promos_text = ""
    if promos:
        active = [p for p in promos if not p['used']]
        if active:
            promos_text = "\n\n🎟 <b>Твои промокоды:</b>\n"
            for p in active:
                promos_text += f"• <code>{p['code']}</code> — {p['discount']}% скидка\n"

    await message.answer(
        f"⭐ <b>Твой прогресс в KeyFlow</b>\n\n"
        f"💰 Потрачено всего: <b>{int(spent)}₽</b>\n"
        f"{level_text}"
        f"{next_text}"
        f"{promos_text}"
    )



    logger.info(f"Admin from {message.from_user.id}, ADMIN_IDS={Config.ADMIN_IDS}")
    if message.from_user.id not in Config.ADMIN_IDS:
        await message.answer('Нет доступа. ID: ' + str(message.from_user.id) + ' Need: ' + str(Config.ADMIN_IDS))
        return
    stats = await db.get_stats()
    await message.answer(
        f"👑 <b>Панель администратора</b>\n\n"
        f"👥 Пользователей: <b>{stats['total_users']}</b>\n"
        f"📦 Заказов всего: <b>{stats['total_orders']}</b>\n"
        f"✅ Выполнено: <b>{stats['completed_orders']}</b>\n"
        f"⏳ Ожидают: <b>{stats['pending_orders']}</b>\n"
        f"💰 Выручка: <b>{stats['total_revenue']}₽</b>",
        reply_markup=admin_kb()
    )


# ─── WEBAPP DATA ────────────────────────────────────────────────────────────────

@dp.message_handler(content_types=types.ContentType.WEB_APP_DATA)
async def handle_webapp_data(message: types.Message):
    try:
        data = json.loads(message.web_app_data.data)
        action = data.get('action')

        if action == 'create_order':
            await process_new_order(message, data)
        elif action == 'sbp_paid':
            await process_sbp_paid(message, data)

    except Exception as e:
        logger.error(f"WebApp error: {e}", exc_info=True)


async def process_new_order(message: types.Message, data: dict):
    order_id = await db.create_order(
        user_id=message.from_user.id,
        service_id=data.get('service_id', 0),
        variant_id=data.get('variant_id', 0),
        amount=data['amount'],
        payment_method=data['payment'],
        webapp_order_id=data.get('order_id')
    )

    service_name = data.get('service_name', 'Услуга')
    variant_dur  = data.get('variant_dur', '')
    amount       = data['amount']

    await message.answer(
        f"✅ <b>Заказ #{order_id} создан!</b>\n\n"
        f"🛍 {service_name} — {variant_dur}\n"
        f"💰 Сумма: <b>{amount}₽</b>\n\n"
        f"📱 Переведи <b>{amount}₽</b> по СБП:\n"
        f"<code>{Config.SBP_PHONE}</code> ({Config.SBP_BANK})\n"
        f"👤 Получатель: {Config.SBP_RECIPIENT}\n\n"
        f"💬 Комментарий: <code>#{data.get('order_id', order_id)}</code>\n\n"
        f"После перевода нажми «Я оплатил» в магазине.",
        reply_markup=main_kb()
    )

    await notify_admins_new_order(order_id, message.from_user, data)


async def process_sbp_paid(message: types.Message, data: dict):
    webapp_order_id = data.get('order_id')
    order = await db.get_order_by_webapp_id(webapp_order_id)
    if not order:
        return

    await db.update_order_status(order['id'], 'waiting_confirm')

    for admin_id in Config.ADMIN_IDS:
        try:
            kb = InlineKeyboardMarkup(row_width=1)
            kb.add(
                InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm:{order['id']}"),
                InlineKeyboardButton("❌ Отклонить",   callback_data=f"reject:{order['id']}")
            )
            await bot.send_message(
                admin_id,
                f"💰 <b>Клиент оплатил — Заказ #{order['id']}</b>\n\n"
                f"👤 @{message.from_user.username or 'без ника'}\n"
                f"💰 {order['amount']}₽ · СБП\n\n"
                f"Подтверди получение оплаты:",
                reply_markup=kb
            )
        except Exception:
            pass

    await message.answer("⏳ Ожидаем подтверждения оплаты. Обычно это занимает до 15 минут.")


# ─── CALLBACKS ─────────────────────────────────────────────────────────────────

@dp.callback_query_handler(lambda c: c.data == 'my_level')
async def cb_my_level(callback: types.CallbackQuery):
    await cmd_level(callback.message)
    await callback.message.delete()


@dp.callback_query_handler(lambda c: c.data == 'my_orders')
async def cb_my_orders(callback: types.CallbackQuery):
    orders = await db.get_user_orders(callback.from_user.id)
    if not orders:
        await callback.message.answer("📦 У тебя пока нет заказов.")
        return

    text = "📦 <b>Твои заказы:</b>\n\n"
    statuses = {'pending':'⏳', 'waiting_confirm':'🔄', 'paid':'✅', 'completed':'🎉', 'cancelled':'❌'}
    for o in orders[:10]:
        icon = statuses.get(o['status'], '•')
        text += f"{icon} Заказ #{o['id']} · {o['amount']}₽ · {o['created_at'][:10]}\n"

    await callback.message.answer(text)


@dp.callback_query_handler(lambda c: c.data == 'adm_main')
async def cb_adm_main(callback: types.CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    await cmd_admin(callback.message)


@dp.callback_query_handler(lambda c: c.data == 'adm_stats')
async def cb_adm_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    stats = await db.get_stats()
    await callback.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: <b>{stats['total_users']}</b>\n"
        f"📦 Всего заказов: <b>{stats['total_orders']}</b>\n"
        f"✅ Выполнено: <b>{stats['completed_orders']}</b>\n"
        f"⏳ Ожидают: <b>{stats['pending_orders']}</b>\n"
        f"💰 Выручка: <b>{stats['total_revenue']}₽</b>",
        reply_markup=back_kb()
    )


@dp.callback_query_handler(lambda c: c.data == 'adm_orders')
async def cb_adm_orders(callback: types.CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    orders = await db.get_pending_orders()
    if not orders:
        await callback.message.edit_text("📭 Активных заказов нет.", reply_markup=back_kb())
        return

    text = "📦 <b>Активные заказы:</b>\n\n"
    kb = InlineKeyboardMarkup(row_width=1)
    for o in orders:
        text += f"#{o['id']} · {o['amount']}₽ · {o['status']}\n"
        kb.add(InlineKeyboardButton(f"📦 Выдать #{o['id']}", callback_data=f"deliver:{o['id']}"))
    kb.add(InlineKeyboardButton("◀️ Назад", callback_data="adm_main"))
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data == 'adm_users')
async def cb_adm_users(callback: types.CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    stats = await db.get_stats()
    users = await db.get_recent_users(10)
    text = f"👥 <b>Пользователи</b>\n\nВсего: <b>{stats['total_users']}</b>\n\n<b>Последние:</b>\n"
    for u in users:
        text += f"• @{u.get('username') or 'без ника'} — {u['created_at'][:10]}\n"
    await callback.message.edit_text(text, reply_markup=back_kb())


@dp.callback_query_handler(lambda c: c.data == 'adm_broadcast')
async def cb_adm_broadcast(callback: types.CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    await AdminStates.broadcast_text.set()
    await callback.message.edit_text("📢 Введи текст рассылки:")


@dp.message_handler(state=AdminStates.broadcast_text)
async def adm_broadcast_send(message: types.Message, state: FSMContext):
    if message.from_user.id not in Config.ADMIN_IDS:
        return
    await state.finish()
    users = await db.get_all_users()
    success = 0
    for u in users:
        try:
            await bot.send_message(u['tg_id'], message.text, parse_mode="HTML")
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await message.answer(f"✅ Рассылка завершена: {success}/{len(users)}", reply_markup=back_kb())


@dp.callback_query_handler(lambda c: c.data.startswith('confirm:'))
async def cb_confirm(callback: types.CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    order_id = int(callback.data.split(':')[1])
    await db.update_order_status(order_id, 'paid')
    order = await db.get_order(order_id)

    try:
        await bot.send_message(
            order['user_id'],
            f"✅ <b>Оплата подтверждена!</b>\n\n"
            f"Заказ #{order_id} принят. Данные придут в течение 15 минут."
        )
    except Exception:
        pass

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📦 Выдать подписку", callback_data=f"deliver:{order_id}"))
    await callback.message.edit_text(
        f"✅ Оплата #{order_id} подтверждена. Выдай подписку:",
        reply_markup=kb
    )


@dp.callback_query_handler(lambda c: c.data.startswith('reject:'))
async def cb_reject(callback: types.CallbackQuery):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    order_id = int(callback.data.split(':')[1])
    await db.update_order_status(order_id, 'cancelled')
    order = await db.get_order(order_id)

    try:
        await bot.send_message(
            order['user_id'],
            f"❌ <b>Заказ #{order_id} отклонён.</b>\n\n"
            f"Оплата не найдена. Если уже оплатил — пиши @{Config.SUPPORT_USERNAME}"
        )
    except Exception:
        pass

    await callback.message.edit_text(f"❌ Заказ #{order_id} отклонён.")


@dp.callback_query_handler(lambda c: c.data.startswith('deliver:'))
async def cb_deliver(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    order_id = int(callback.data.split(':')[1])
    await state.update_data(delivery_order_id=order_id)
    await AdminStates.delivery_data.set()
    await callback.message.answer(
        f"📦 <b>Выдача подписки — Заказ #{order_id}</b>\n\n"
        f"Введи данные для клиента:\n\n"
        f"Пример:\n<code>Логин: user@mail.com\nПароль: Pass123!</code>"
    )


@dp.message_handler(state=AdminStates.delivery_data)
async def adm_delivery_data(message: types.Message, state: FSMContext):
    if message.from_user.id not in Config.ADMIN_IDS:
        return
    data = await state.get_data()
    order_id = data.get('delivery_order_id')
    await state.finish()

    order = await db.get_order(order_id)
    await db.update_order_status(order_id, 'completed')

    # Начисляем очки за покупку
    level_result = await db.add_spent(order['user_id'], order['amount'])

    try:
        await bot.send_message(
            order['user_id'],
            f"🎉 <b>Твоя подписка готова!</b>\n\n"
            f"📦 Заказ #{order_id}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"{message.text}\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"Спасибо за покупку в KeyFlow! 🔑\n"
            f"Поддержка: @{Config.SUPPORT_USERNAME}",
            reply_markup=main_kb()
        )

        # Уведомление о новом уровне
        if level_result and level_result.get('promo'):
            lvl = level_result['level']
            promo = level_result['promo']
            spent = level_result['total_spent']
            await bot.send_message(
                order['user_id'],
                f"🎊 <b>Новый уровень!</b>\n\n"
                f"Ты достиг уровня <b>{lvl['name']}</b>!\n"
                f"Потрачено: <b>{int(spent)}₽</b>\n\n"
                f"🎟 Твой личный промокод на скидку <b>{lvl['discount']}%</b>:\n"
                f"<code>{promo}</code>\n\n"
                f"Используй при следующей покупке — скидка применится автоматически!"
            )

        await message.answer(f"✅ Данные отправлены клиенту — заказ #{order_id} выполнен!")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить: {e}\n\nДанные: {message.text}")


# ─── HELPERS ───────────────────────────────────────────────────────────────────

async def notify_admins_new_order(order_id, user, data):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📦 Выдать подписку", callback_data=f"deliver:{order_id}"))

    for admin_id in Config.ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"🔔 <b>Новый заказ #{order_id}</b>\n\n"
                f"👤 @{user.username or 'без ника'} (ID: {user.id})\n"
                f"🛍 {data.get('service_name')} — {data.get('variant_dur')}\n"
                f"💰 {data.get('amount')}₽ · СБП\n"
                f"⏰ {datetime.now().strftime('%d.%m %H:%M')}",
                reply_markup=kb
            )
        except Exception:
            pass


# ─── MAIN ──────────────────────────────────────────────────────────────────────

from aiohttp import web

async def health(request):
    return web.Response(text="OK")

async def main():
    await db.init()
    logger.info("🔑 KeyFlow Bot запущен!")

    # Фиктивный веб-сервер для Render
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")

    await dp.start_polling()


if __name__ == "__main__":
    import os
    asyncio.run(main())
