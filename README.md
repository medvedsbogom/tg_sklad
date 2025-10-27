# tg_sklad
бот, который ведёт учёт инструментов и ключей на складе.
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException
import gspread
from google.oauth2.service_account import Credentials
import threading
import logging
import time
import uuid

# ================== КОНФИГ ==================
BOT_TOKEN = 
SPREADSHEET_ID =

LOG_SHEET_NAME = 'Log'
KEYS_SHEET_NAME = 'Keys'

OBJECTS = [
    'Урбан44', 'Урбан77', 'Победы', 'Лапшин', 'Кокос150', 'Мира20',
    'Троя', 'Набережная', 'Бурковский80'
]

ADMIN_IDS = 
REQUEST_DELAY = 0.0
CACHE_TTL = 5.0
MASTER_NAMES = 

# ================== ЛОГИ ====================

)

# ================== GOOGLE SHEETS ===========


CREDS_FILE = r"C:\Users\Mishk\OneDrive\Рабочий стол\telebot-service.json"

creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)
spreadsheet = gc.open_by_key(SPREADSHEET_ID)

# ================== УТИЛИТЫ / КЭШ =================
sheet_lock = threading.Lock()
SHEET_CACHE = {}

YO_CHAR = '\u0451'
E_CHAR = '\u0435'

def _normalize_base(value: str) -> str:
    if not value:
        return ''
    return value.strip().lower().replace(YO_CHAR, E_CHAR)

def normalize_input_text(value: str) -> str:
    """Normalize arbitrary user text for matching."""
    return _normalize_base(value)

def normalize_header(value: str) -> str:
    """Normalize header names to compare columns regardless of case/spacing."""
    base = _normalize_base(value)
    return ''.join(ch for ch in base if not ch.isspace())

def cache_invalidate(ws):
    """Очищает кэш для указанного листа"""
    SHEET_CACHE.pop(ws.id, None)

def cache_get(ws):
    """Получает данные листа из кэша или из Google Sheets"""
    rec = SHEET_CACHE.get(ws.id)
    if rec and (time.time() - rec['ts'] <= CACHE_TTL):
        return rec['header'], rec['rows']
    vals = ws.get_all_values()
    header = vals[0] if vals else []
    rows = vals[1:] if len(vals) > 1 else []
    SHEET_CACHE[ws.id] = {'ts': time.time(), 'header': header, 'rows': rows}
    return header, rows

def ensure_sheet(name: str, header: list[str]):
    """Проверяет или создаёт лист с указанными заголовками"""
    try:
        ws = spreadsheet.worksheet(name)
        current_header = ws.row_values(1)
        logging.info(f"Текущие заголовки в {name}: {current_header}")
        norm_current = [normalize_header(h) for h in current_header]
        norm_required = [normalize_header(h) for h in header]
        missing_cols = [h for h in header if normalize_header(h) not in norm_current]
        if missing_cols:
            logging.warning(f"В листе {name} отсутствуют столбцы: {missing_cols}")
            if len(ws.get_all_values()) > 1:  # Проверяем, есть ли данные, кроме заголовков
                logging.error(f"Лист {name} содержит данные, но заголовки некорректны. Очистка отменена.")
                raise ValueError(f"Лист {name} содержит данные, но отсутствуют столбцы: {missing_cols}. Проверьте структуру таблицы.")
            ws.clear()
            ws.append_row(header)
            logging.info(f"Лист {name} очищен и заголовки обновлены: {header}")
            cache_invalidate(ws)
        else:
            logging.info(f"Все необходимые столбцы найдены в {name}")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=name, rows=2000, cols=max(40, len(header)+5))
        ws.append_row(header)
        logging.info(f"Создан новый лист {name} с заголовками: {header}")
    return ws

def header_map_from(header_row):
    raw = {h.strip(): i+1 for i, h in enumerate(header_row) if h}
    norm = {normalize_header(h): i+1 for i, h in enumerate(header_row) if h}

    def pick(*names):
        for nm in names:
            idx = norm.get(normalize_header(nm))
            if idx:
                return idx
        logging.warning(f"Не найдена колонка для: {names}")
        return None
    idx = {}
    idx['ID'] = pick('id')
    idx['Тип'] = pick('тип', 'type')
    idx['Наименование'] = pick('наименование', 'название', 'name', 'наим.')
    idx['Объект'] = pick('объект', 'обьект', 'object', 'объект ')
    idx['Сотрудник'] = pick('сотрудник', 'сотр', 'employee', 'user', 'пользователь')
    idx['Склад'] = pick('склад', 'на складе', 'свободно/в офисе', 'свободно', 'free', 'stock', 'warehouse', 'в офисе', 'офис', 'office')
    idx['Общее количество'] = pick('общее количество', 'итого', 'всего')
    idx['Комментарий'] = pick('комментарий', 'comment')
    idx['С домофоном'] = pick('с домофоном', 'домофон')
    idx['Ключ'] = pick('ключ', 'key')
    for o in OBJECTS:
        idx[o] = norm.get(normalize_header(o))
        if not idx[o]:
            logging.warning(f"Колонка объекта {o} не найдена")
    idx['_raw'] = raw
    logging.debug(f"Маппинг заголовков: {idx}")
    return idx

def a1(row, col):
    from gspread.utils import rowcol_to_a1
    return rowcol_to_a1(row, col)

def normalize_type(t):
    return 'Уникальный' if (t or '').strip().lower().startswith('уник') else 'Количественный'

def get_tg_nick(user):
    if user.username:
        return '@' + user.username
    full = ' '.join(x for x in [user.first_name, user.last_name] if x)
    return full if full.strip() else f'id:{user.id}'

def clear_employee_validation(ws, header_row, col_name):
    try:
        idx = header_map_from(header_row).get(col_name)
        if not idx: return
        req = {
            "requests": [
                {
                    "setDataValidation": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 1,
                            "startColumnIndex": idx - 1,
                            "endColumnIndex": idx
                        },
                        "rule": None
                    }
                }
            ]
        }
        spreadsheet.batch_update(req)
    except Exception as e:
        logging.warning(f"Не снял валидацию: {ws.title}.{col_name}: {e}")

def objects_inline(prefix='obj_', exclude=None):
    mk = types.InlineKeyboardMarkup()
    for o in OBJECTS:
        if exclude and normalize_header(o) == normalize_header(exclude):
            continue
        mk.add(types.InlineKeyboardButton(o, callback_data=f"{prefix}{o}"))
    return mk

def master_inline(prefix='ms_'):
    mk = types.InlineKeyboardMarkup()
    for i, name in enumerate(MASTER_NAMES):
        mk.add(types.InlineKeyboardButton(name, callback_data=f"{prefix}{i}"))
    return mk

def batch_write(ws, updates):
    if not updates:
        return
    try:
        curr_rows = len(ws.get_all_values())
        curr_cols = len(ws.get_all_values()[0]) if curr_rows > 0 else 0
        logging.info(f"Текущие размеры таблицы: {curr_rows}x{curr_cols}")
        for r, c, v in updates:
            if r <= 0 or c <= 0:
                logging.error(f"Некорректные координаты: r={r}, c={c}")
                continue
        data = []
        for r, c, v in updates:
            rng = a1(r, c)
            data.append({'range': rng, 'values': [[v]]})
            logging.info(f"Подготовлено обновление: {rng} = {v}")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                ws.batch_update(data, value_input_option='USER_ENTERED')
                logging.info("Обновление успешно выполнено")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Ошибка при обновлении (попытка {attempt + 1}): {e}")
                    time.sleep(1)
                else:
                    logging.error(f"Не удалось выполнить обновление после {max_retries} попыток: {e}")
                    raise
        cache_invalidate(ws)
        time.sleep(REQUEST_DELAY)
    except Exception as e:
        logging.error(f"Ошибка в batch_write: {e}")
        raise

# Инициализация листов
sheet_log = ensure_sheet(LOG_SHEET_NAME, ['ID', 'Тип', 'Наименование', 'Объект', 'Сотрудник', 'Общее количество', 'Склад'] + OBJECTS)
sheet_keys = ensure_sheet(KEYS_SHEET_NAME, ['Ключ', 'Сотрудник', 'Склад', 'Комментарий', 'С домофоном'] + OBJECTS)

# Инициализация бота
bot = telebot.TeleBot(BOT_TOKEN)

try:
    from gspread_formatting import Color, CellFormat, format_cell_range
    HAVE_FMT = True
except:
    HAVE_FMT = False
    logging.warning("gspread-formatting не установлен — цвета не применятся.")

# Выполняем очистку валидации после инициализации листов
hdr, _ = cache_get(sheet_log)
clear_employee_validation(sheet_log, hdr, 'Сотрудник')
hdrk, _ = cache_get(sheet_keys)
clear_employee_validation(sheet_keys, hdrk, 'Сотрудник')

# ================== СОСТОЯНИЯ =================
U_SEL = {}
U_CONFIRMED = {}
U_RET_SEL = {}
Q_SEL = {}
Q_CONFIRMED = {}
Q_RET_STATE = {}
Q_LIST = {}
K_SEL = {}
K_CONFIRMED = {}
K_RET_STATE = {}
MV_STATE = {}
KG_SEL = {}
KR_SEL = {}

# ================== РАЗДЕЛ КЛЮЧЕЙ =================
def verify_keys_sheet():
    try:
        header, rows = cache_get(sheet_keys)
        logging.info(f"Проверка таблицы ключей. Текущие заголовки: {header}")
        required_columns = ['Ключ', 'Сотрудник', 'Склад', 'Комментарий', 'С домофоном'] + OBJECTS
        norm_header = [normalize_header(h) for h in header]
        norm_required = [normalize_header(h) for h in required_columns]
        missing = [h for h in required_columns if normalize_header(h) not in norm_header]
        if missing:
            logging.warning(f"Отсутствуют колонки в таблице ключей: {missing}")
            if len(rows) > 0:  # Проверяем, есть ли данные
                logging.error(f"Таблица ключей содержит данные, но заголовки некорректны. Очистка отменена.")
                raise ValueError(f"Таблица ключей содержит данные, но отсутствуют колонки: {missing}. Проверьте структуру таблицы.")
            sheet_keys.clear()
            sheet_keys.append_row(required_columns)
            logging.info("Таблица ключей переинициализирована")
            cache_invalidate(sheet_keys)
            return True
        return False
    except Exception as e:
        logging.error(f"Ошибка при проверке таблицы ключей: {e}")
        sheet_keys.clear()
        sheet_keys.append_row(['Ключ', 'Сотрудник', 'Склад', 'Комментарий', 'С домофоном'] + OBJECTS)
        cache_invalidate(sheet_keys)
        return True

def safe_operation(func):
    def wrapper(*args, **kwargs):
        verify_keys_sheet()
        return func(*args, **kwargs)
    return wrapper

@bot.message_handler(func=lambda m: 'учет ключей' in normalize_input_text(m.text))
@safe_operation
def keys_menu(message):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add('📋 Мои ключи', '🔑 Взять новый ключ')
    if message.from_user.id in ADMIN_IDS:
        kb.add('Выдать ключ', 'Забрать ключ')
    kb.add('🔙 Назад')
    bot.send_message(message.chat.id, 'Раздел ключей:', reply_markup=kb)

@bot.message_handler(func=lambda m: m.text == '📋 Мои ключи')
@safe_operation
def my_keys(message):
    nick = get_tg_nick(message.from_user)
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    i_key = idx.get('Ключ')
    i_emp = idx.get('Сотрудник')
    i_dom = idx.get('С домофоном')
    if not i_key or not i_emp:
        bot.send_message(message.chat.id, 'Ошибка: не найдены колонки Ключ или Сотрудник', reply_markup=keys_menu(message))
        return
    doors = []
    domofons = []
    for r in rows:
        if (r[i_emp - 1] or '').strip() != nick:
            continue
        dom_status = (r[i_dom - 1] or '0').strip() if i_dom else '0'
        key_name = (r[i_key - 1] or 'Ключ').strip()
        for obj in OBJECTS:
            col = idx.get(obj)
            if not col:
                continue
            try:
                qty = int((r[col - 1] or '0').replace(' ', '') or 0)
            except:
                qty = 0
            if qty <= 0:
                continue
            if dom_status == '1':
                domofons.append(f"{key_name} — {obj}: {qty} шт.")
            else:
                doors.append(f"{key_name} — {obj}: {qty} шт.")
    if not i_dom:
        logging.warning("Колонка 'С домофоном' не найдена, предполагается значение 0")
    txt = []
    if doors:
        txt.append("🚪 Физические двери:\n" + '\n'.join(doors))
    if domofons:
        txt.append("📞 Домофоны:\n" + '\n'.join(domofons))
    if not txt:
        txt = ["У тебя нет ключей."]
    bot.send_message(message.chat.id, '\n'.join(txt), reply_markup=keys_menu(message))

@bot.message_handler(func=lambda m: m.text == '🔑 Взять новый ключ')
@safe_operation
def k_take_new(message):
    K_SEL[message.from_user.id] = {'obj': None, 'qty': 0, 'domofon': None}
    bot.send_message(message.chat.id, "Для какого объекта?", reply_markup=objects_inline(prefix='kn_obj_'))

@bot.callback_query_handler(func=lambda c: c.data.startswith('kn_obj_'))
def kn_obj_select(call):
    obj = call.data[len('kn_obj_'):]
    K_SEL[call.from_user.id]['obj'] = obj
    mk = types.InlineKeyboardMarkup()
    mk.add(
        types.InlineKeyboardButton('Дверь (физ.)', callback_data='kn_dom_0'),
        types.InlineKeyboardButton('Домофон', callback_data='kn_dom_1')
    )
    bot.edit_message_text(f'Ключ для {obj}:', call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('kn_dom_'))
def kn_domofon_select(call):
    try:
        dom = int(call.data.split('_')[-1])
    except Exception:
        bot.answer_callback_query(call.id, 'Ошибка выбора типа')
        return
    sel = K_SEL.get(call.from_user.id, {})
    sel['domofon'] = dom
    sel['qty'] = 0
    K_SEL[call.from_user.id] = sel
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_dom = idx.get('С домофоном')
    if not i_stock:
        bot.answer_callback_query(call.id, 'Ошибка: не найдена колонка Склад')
        return
    total = 0
    for r in rows:
        if (r[i_dom - 1] or '0').strip() == str(dom):
            try:
                total += int((r[i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                pass
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton(f'Текущее количество: 0 шт. (на складе: {total})', callback_data='noop'))
    mk.row(
        types.InlineKeyboardButton('−', callback_data='kn_dec'),
        types.InlineKeyboardButton('+', callback_data='kn_inc')
    )
    mk.add(
        types.InlineKeyboardButton('✅ Подтвердить', callback_data='kn_confirm'),
        types.InlineKeyboardButton('Отмена', callback_data='kn_cancel')
    )
    bot.edit_message_text(f'Укажи количество ключей ({"Домофон" if dom else "Дверь"}):', call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data in ('kn_inc', 'kn_dec', 'kn_cancel', 'kn_confirm'))
def kn_qty_update(call):
    sel = K_SEL.get(call.from_user.id, {})
    if call.data == 'kn_cancel':
        K_SEL.pop(call.from_user.id, None)
        bot.edit_message_text('Операция отменена.', call.message.chat.id, call.message.message_id)
        bot.send_message(call.message.chat.id, 'Раздел ключей:', reply_markup=keys_menu(call.message))
        return
    if call.data == 'kn_confirm':
        if not sel.get('obj') or sel.get('qty', 0) <= 0 or sel.get('domofon') is None:
            bot.answer_callback_query(call.id, 'Выберите объект и задайте количество.')
            return
        header, rows = cache_get(sheet_keys)
        idx = header_map_from(header)
        i_stock = idx.get('Склад')
        i_emp = idx.get('Сотрудник')
        i_dom = idx.get('С домофоном')
        i_objcol = idx.get(sel['obj'])
        if not all([i_stock, i_emp, i_objcol]):
            error_msg = f"Ошибка: не найдены колонки: {'Склад ' if not i_stock else ''}{'Сотрудник ' if not i_emp else ''}{'Объект' if not i_objcol else ''}"
            logging.error(error_msg)
            bot.answer_callback_query(call.id, error_msg.strip())
            return
        qty = sel['qty']
        dom = sel['domofon']
        obj = sel['obj']
        nick = get_tg_nick(call.from_user)
        updates = []
        with sheet_lock:
            # Проверяем общее количество на складе для данного типа ключа
            total_stock = 0
            for r in rows:
                if (r[i_dom - 1] or '0').strip() == str(dom):
                    try:
                        total_stock += int((r[i_stock - 1] or '0').replace(' ', '') or 0)
                    except:
                        pass
            effective_qty = qty * 2 if dom else qty
            if total_stock < effective_qty:
                logging.warning(f'Запрошено {effective_qty}, но найдено только {total_stock}. Продолжаем выдачу как экстренную.')
            row_idx = None
            for r_i, r in enumerate(rows, start=2):
                if (r[i_emp - 1] or '').strip() == nick and (r[i_dom - 1] or '0').strip() == str(dom):
                    row_idx = r_i
                    break
            if row_idx is None:
                key_caption = f'Ключ ({"Домофон" if dom else "Дверь"})'
                sheet_keys.append_row([key_caption, nick, 0, '', dom] + [0] * len(OBJECTS))
                cache_invalidate(sheet_keys)
                header, rows = cache_get(sheet_keys)
                row_idx = len(rows) + 1
            try:
                stock = int((rows[row_idx - 2][i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                stock = 0
            try:
                curr = int((rows[row_idx - 2][i_objcol - 1] or '0').replace(' ', '') or 0)
            except:
                curr = 0
            if stock < effective_qty:
                logging.warning(f'У сотрудника {nick} найдено только {stock} свободных ключей. Устанавливаем остаток в 0.')
            updates.append((row_idx, i_stock, max(stock - effective_qty, 0)))
            updates.append((row_idx, i_objcol, curr + effective_qty))
            updates.append((row_idx, i_emp, nick))
            updates.append((row_idx, i_dom, dom))
            batch_write(sheet_keys, updates)
        K_SEL.pop(call.from_user.id, None)
        kind = 'Домофон' if dom else 'Дверь (физ.)'
        bot.edit_message_text(f'Взято {kind} по {obj}: {qty} шт. (зачёт: {effective_qty})', call.message.chat.id, call.message.message_id)
        bot.send_message(call.message.chat.id, 'Раздел ключей:', reply_markup=keys_menu(call.message))
        return
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_dom = idx.get('С домофоном')
    total = 0
    for r in rows:
        if (r[i_dom - 1] or '0').strip() == str(sel.get('domofon', 0)):
            try:
                total += int((r[i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                pass
    qty = sel.get('qty', 0)
    multiplier = 2 if sel.get('domofon') else 1
    if call.data == 'kn_inc':
        if total > 0 and qty * multiplier >= total:
            bot.answer_callback_query(call.id, 'Больше ключей нет на складе.')
            return
        sel['qty'] = qty + 1
    if call.data == 'kn_dec':
        if qty <= 0:
            bot.answer_callback_query(call.id, 'Нельзя уменьшить ниже нуля.')
            return
        sel['qty'] = qty - 1
    K_SEL[call.from_user.id] = sel
    dom = sel.get('domofon', 0)
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton(f'Текущее количество: {sel["qty"]} шт. (на складе: {total})', callback_data='noop'))
    mk.row(
        types.InlineKeyboardButton('−', callback_data='kn_dec'),
        types.InlineKeyboardButton('+', callback_data='kn_inc')
    )
    mk.add(
        types.InlineKeyboardButton('✅ Подтвердить', callback_data='kn_confirm'),
        types.InlineKeyboardButton('Отмена', callback_data='kn_cancel')
    )
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)
    except ApiTelegramException as exc:
        if 'message is not modified' not in (getattr(exc, 'description', '') or '').lower():
            raise

@bot.message_handler(func=lambda m: m.text == 'Выдать ключ' and m.from_user.id in ADMIN_IDS)
@safe_operation
def give_key_start(message):
    KG_SEL[message.from_user.id] = {'master': None, 'obj': None, 'qty': 0, 'domofon': None}
    bot.send_message(message.chat.id, 'Кому выдаём?', reply_markup=master_inline(prefix='kg_ms_'))

@bot.callback_query_handler(func=lambda c: c.data.startswith('kg_ms_'))
def kg_master_select(call):
    idx = int(call.data.split('_')[-1])
    master = MASTER_NAMES[idx]
    KG_SEL[call.from_user.id]['master'] = master
    bot.edit_message_text(f'Выдаём мастеру {master}\nДля какого объекта?', call.message.chat.id, call.message.message_id,
                         reply_markup=objects_inline(prefix='kg_obj_'))

@bot.callback_query_handler(func=lambda c: c.data.startswith('kg_obj_'))
def kg_obj_select(call):
    obj = call.data[len('kg_obj_'):]
    KG_SEL[call.from_user.id]['obj'] = obj
    mk = types.InlineKeyboardMarkup()
    mk.add(
        types.InlineKeyboardButton('Дверь (физ.)', callback_data='kg_dom_0'),
        types.InlineKeyboardButton('Домофон', callback_data='kg_dom_1')
    )
    bot.edit_message_text(f'Ключ для {obj}:', call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('kg_dom_'))
def kg_domofon_select(call):
    try:
        dom = int(call.data.split('_')[-1])
    except Exception:
        bot.answer_callback_query(call.id, 'Ошибка выбора типа')
        return
    sel = KG_SEL.get(call.from_user.id, {})
    sel['domofon'] = dom
    sel['qty'] = 0
    KG_SEL[call.from_user.id] = sel
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_dom = idx.get('С домофоном')
    if not i_stock:
        bot.answer_callback_query(call.id, 'Ошибка: не найдена колонка Склад')
        return
    total = 0
    for r in rows:
        if (r[i_dom - 1] or '0').strip() == str(dom):
            try:
                total += int((r[i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                pass
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton(f'Текущее количество: 0 шт. (на складе: {total})', callback_data='noop'))
    mk.row(
        types.InlineKeyboardButton('−', callback_data='kg_dec'),
        types.InlineKeyboardButton('+', callback_data='kg_inc')
    )
    mk.add(
        types.InlineKeyboardButton('✅ Подтвердить', callback_data='kg_confirm'),
        types.InlineKeyboardButton('Отмена', callback_data='kg_cancel')
    )
    bot.edit_message_text(f'Укажи количество ключей ({"Домофон" if dom else "Дверь"}):', call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data in ('kg_inc', 'kg_dec', 'kg_cancel', 'kg_confirm'))
def kg_qty_update(call):
    sel = KG_SEL.get(call.from_user.id, {})
    if call.data == 'kg_cancel':
        KG_SEL.pop(call.from_user.id, None)
        bot.edit_message_text('Операция отменена.', call.message.chat.id, call.message.message_id)
        bot.send_message(call.message.chat.id, 'Раздел ключей:', reply_markup=keys_menu(call.message))
        return
    if call.data == 'kg_confirm':
        if not sel.get('master') or not sel.get('obj') or sel.get('qty', 0) <= 0 or sel.get('domofon') is None:
            bot.answer_callback_query(call.id, 'Выберите мастера, объект и задайте количество.')
            return
        header, rows = cache_get(sheet_keys)
        idx = header_map_from(header)
        i_stock = idx.get('Склад')
        i_emp = idx.get('Сотрудник')
        i_dom = idx.get('С домофоном')
        i_objcol = idx.get(sel['obj'])
        if not all([i_stock, i_emp, i_objcol]):
            error_msg = f"Ошибка: не найдены колонки: {'Склад ' if not i_stock else ''}{'Сотрудник ' if not i_emp else ''}{'Объект' if not i_objcol else ''}"
            logging.error(error_msg)
            bot.answer_callback_query(call.id, error_msg.strip())
            return
        qty = sel['qty']
        dom = sel['domofon']
        obj = sel['obj']
        master = sel['master']
        updates = []
        with sheet_lock:
            total_stock = 0
            for r in rows:
                if (r[i_dom - 1] or '0').strip() == str(dom):
                    try:
                        total_stock += int((r[i_stock - 1] or '0').replace(' ', '') or 0)
                    except:
                        pass
            effective_qty = qty * 2 if dom else qty
            if total_stock < effective_qty:
                bot.answer_callback_query(call.id, f'Недостаточно на складе: {total_stock} шт.')
                return
            row_idx = None
            for r_i, r in enumerate(rows, start=2):
                if (r[i_emp - 1] or '').strip() == master and (r[i_dom - 1] or '0').strip() == str(dom):
                    row_idx = r_i
                    break
            if row_idx is None:
                sheet_keys.append_row(['Ключ', master, 0, '', dom] + [0] * len(OBJECTS))
                cache_invalidate(sheet_keys)
                header, rows = cache_get(sheet_keys)
                row_idx = len(rows) + 1
            try:
                stock = int((rows[row_idx - 2][i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                stock = 0
            try:
                curr = int((rows[row_idx - 2][i_objcol - 1] or '0').replace(' ', '') or 0)
            except:
                curr = 0
            if stock < effective_qty:
                bot.answer_callback_query(call.id, f'Недостаточно на складе: {stock} шт.')
                return
            updates.append((row_idx, i_stock, stock - effective_qty))
            updates.append((row_idx, i_objcol, curr + effective_qty))
            updates.append((row_idx, i_emp, master))
            updates.append((row_idx, i_dom, dom))
            batch_write(sheet_keys, updates)
        KG_SEL.pop(call.from_user.id, None)
        kind = 'Домофон' if dom else 'Дверь (физ.)'
        bot.edit_message_text(f'Выдано {kind} мастеру {master} по {obj}: {qty} шт. (зачёт: {effective_qty})', call.message.chat.id, call.message.message_id)
        bot.send_message(call.message.chat.id, 'Раздел ключей:', reply_markup=keys_menu(call.message))
        return
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_dom = idx.get('С домофоном')
    total = 0
    for r in rows:
        if (r[i_dom - 1] or '0').strip() == str(sel.get('domofon', 0)):
            try:
                total += int((r[i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                pass
    qty = sel.get('qty', 0)
    if call.data == 'kg_inc' and qty * (2 if sel.get('domofon') else 1) < total:
        sel['qty'] = qty + 1
    if call.data == 'kg_dec' and qty > 0:
        sel['qty'] = qty - 1
    KG_SEL[call.from_user.id] = sel
    dom = sel.get('domofon', 0)
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton(f'Текущее количество: {sel["qty"]} шт. (на складе: {total})', callback_data='noop'))
    mk.row(
        types.InlineKeyboardButton('−', callback_data='kg_dec'),
        types.InlineKeyboardButton('+', callback_data='kg_inc')
    )
    mk.add(
        types.InlineKeyboardButton('✅ Подтвердить', callback_data='kg_confirm'),
        types.InlineKeyboardButton('Отмена', callback_data='kg_cancel')
    )
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.message_handler(func=lambda m: m.text == 'Забрать ключ' and m.from_user.id in ADMIN_IDS)
@safe_operation
def kr_start(message):
    KR_SEL[message.from_user.id] = {'master': None, 'obj': None, 'qty': 0, 'domofon': None}
    bot.send_message(message.chat.id, 'У кого забираем?', reply_markup=master_inline(prefix='kr_ms_'))

@bot.callback_query_handler(func=lambda c: c.data.startswith('kr_ms_'))
def kr_master_select(call):
    idx = int(call.data.split('_')[-1])
    master = MASTER_NAMES[idx]
    KR_SEL[call.from_user.id]['master'] = master
    bot.edit_message_text(f'Забираем у {master}\nС какого объекта?', call.message.chat.id, call.message.message_id,
                         reply_markup=objects_inline(prefix='kr_obj_'))

@bot.callback_query_handler(func=lambda c: c.data.startswith('kr_obj_'))
def kr_obj_select(call):
    obj = call.data[len('kr_obj_'):]
    KR_SEL[call.from_user.id]['obj'] = obj
    mk = types.InlineKeyboardMarkup()
    mk.add(
        types.InlineKeyboardButton('Дверь (физ.)', callback_data='kr_dom_0'),
        types.InlineKeyboardButton('Домофон', callback_data='kr_dom_1')
    )
    bot.edit_message_text(f'Ключ с {obj}:', call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('kr_dom_'))
def kr_domofon_select(call):
    try:
        dom = int(call.data.split('_')[-1])
    except Exception:
        bot.answer_callback_query(call.id, 'Ошибка выбора типа')
        return
    sel = KR_SEL.get(call.from_user.id, {})
    sel['domofon'] = dom
    sel['qty'] = 0
    KR_SEL[call.from_user.id] = sel
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    i_emp = idx.get('Сотрудник')
    i_dom = idx.get('С домофоном')
    i_objcol = idx.get(sel['obj'])
    if not all([i_emp, i_objcol]):
        error_msg = f"Ошибка: не найдены колонки: {'Сотрудник ' if not i_emp else ''}{'Объект' if not i_objcol else ''}"
        logging.error(error_msg)
        bot.answer_callback_query(call.id, error_msg.strip())
        return
    available_qty = 0
    for r in rows:
        if (r[i_emp - 1] or '').strip() == sel['master'] and (r[i_dom - 1] or '0').strip() == str(dom):
            try:
                available_qty = int((r[i_objcol - 1] or '0').replace(' ', '') or 0)
            except:
                available_qty = 0
            break
    if available_qty == 0:
        bot.answer_callback_query(call.id, f'У мастера {sel["master"]} нет ключей для {sel["obj"]} ({"Домофон" if dom else "Дверь"}).')
        bot.edit_message_text('Операция отменена.', call.message.chat.id, call.message.message_id)
        bot.send_message(call.message.chat.id, 'Раздел ключей:', reply_markup=keys_menu(call.message))
        return
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton(f'Текущее количество: 0 шт. (доступно: {available_qty})', callback_data='noop'))
    mk.row(
        types.InlineKeyboardButton('−', callback_data='kr_dec'),
        types.InlineKeyboardButton('+', callback_data='kr_inc')
    )
    mk.add(
        types.InlineKeyboardButton('✅ Подтвердить', callback_data='kr_confirm'),
        types.InlineKeyboardButton('Отмена', callback_data='kr_cancel')
    )
    bot.edit_message_text(f'Укажите количество ключей для {sel["obj"]} ({"Домофон" if dom else "Дверь"}):', call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data in ('kr_inc', 'kr_dec', 'kr_cancel', 'kr_confirm'))
def kr_qty_update(call):
    sel = KR_SEL.get(call.from_user.id, {})
    if call.data == 'kr_cancel':
        KR_SEL.pop(call.from_user.id, None)
        bot.edit_message_text('Операция отменена.', call.message.chat.id, call.message.message_id)
        bot.send_message(call.message.chat.id, 'Раздел ключей:', reply_markup=keys_menu(call.message))
        return
    if call.data == 'kr_confirm':
        if not sel.get('master') or not sel.get('obj') or sel.get('qty', 0) <= 0 or sel.get('domofon') is None:
            bot.answer_callback_query(call.id, 'Выберите мастера, объект и задайте количество.')
            return
        header, rows = cache_get(sheet_keys)
        idx = header_map_from(header)
        i_stock = idx.get('Склад')
        i_emp = idx.get('Сотрудник')
        i_dom = idx.get('С домофоном')
        i_objcol = idx.get(sel['obj'])
        if not all([i_stock, i_emp, i_objcol]):
            error_msg = f"Ошибка: не найдены колонки: {'Склад ' if not i_stock else ''}{'Сотрудник ' if not i_emp else ''}{'Объект' if not i_objcol else ''}"
            logging.error(error_msg)
            bot.answer_callback_query(call.id, error_msg.strip())
            return
        qty = sel['qty']
        dom = sel['domofon']
        obj = sel['obj']
        master = sel['master']
        updates = []
        with sheet_lock:
            row_idx = None
            available_qty = 0
            for r_i, r in enumerate(rows, start=2):
                if (r[i_emp - 1] or '').strip() == master and (r[i_dom - 1] or '0').strip() == str(dom):
                    row_idx = r_i
                    try:
                        available_qty = int((r[i_objcol - 1] or '0').replace(' ', '') or 0)
                    except:
                        available_qty = 0
                    break
            if row_idx is None or available_qty < qty:
                bot.answer_callback_query(call.id, f'У мастера {master} недостаточно ключей: {available_qty} шт.')
                return
            try:
                stock = int((rows[row_idx - 2][i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                stock = 0
            try:
                curr = int((rows[row_idx - 2][i_objcol - 1] or '0').replace(' ', '') or 0)
            except:
                curr = 0
            effective_qty = qty * 2 if dom else qty
            updates.append((row_idx, i_stock, stock + effective_qty))
            updates.append((row_idx, i_objcol, curr - qty))
            batch_write(sheet_keys, updates)
        KR_SEL.pop(call.from_user.id, None)
        kind = 'Домофон' if dom else 'Дверь (физ.)'
        bot.edit_message_text(f'Забрано {kind} у {master} с {obj}: {qty} шт. (зачёт: {effective_qty})', call.message.chat.id, call.message.message_id)
        bot.send_message(call.message.chat.id, 'Раздел ключей:', reply_markup=keys_menu(call.message))
        return
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    i_emp = idx.get('Сотрудник')
    i_dom = idx.get('С домофоном')
    i_objcol = idx.get(sel['obj'])
    total = 0
    for r in rows:
        if (r[i_emp - 1] or '').strip() == sel.get('master') and (r[i_dom - 1] or '0').strip() == str(sel.get('domofon', 0)):
            try:
                total += int((r[i_objcol - 1] or '0').replace(' ', '') or 0)
            except:
                pass
    qty = sel.get('qty', 0)
    if call.data == 'kr_inc' and qty < total:
        sel['qty'] = qty + 1
    if call.data == 'kr_dec' and qty > 0:
        sel['qty'] = qty - 1
    KR_SEL[call.from_user.id] = sel
    dom = sel.get('domofon', 0)
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton(f'Текущее количество: {sel["qty"]} шт. (доступно: {total})', callback_data='noop'))
    mk.row(
        types.InlineKeyboardButton('−', callback_data='kr_dec'),
        types.InlineKeyboardButton('+', callback_data='kr_inc')
    )
    mk.add(
        types.InlineKeyboardButton('✅ Подтвердить', callback_data='kr_confirm'),
        types.InlineKeyboardButton('Отмена', callback_data='kr_cancel')
    )
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.message_handler(func=lambda m: m.text == '➕ Добавить ключ' and m.from_user.id in ADMIN_IDS)
@safe_operation
def add_key_start(message):
    K_SEL[message.from_user.id] = {'qty': 0, 'domofon': None}
    mk = types.InlineKeyboardMarkup()
    mk.add(
        types.InlineKeyboardButton('Дверь (физ.)', callback_data='add_dom_0'),
        types.InlineKeyboardButton('Домофон', callback_data='add_dom_1')
    )
    bot.send_message(message.chat.id, 'Тип ключа:', reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('add_dom_'))
def add_domofon_select(call):
    try:
        dom = int(call.data.split('_')[-1])
    except Exception:
        bot.answer_callback_query(call.id, 'Ошибка выбора типа')
        return
    sel = K_SEL.get(call.from_user.id, {})
    sel['domofon'] = dom
    sel['qty'] = 0
    K_SEL[call.from_user.id] = sel
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_dom = idx.get('С домофоном')
    if not i_stock:
        bot.answer_callback_query(call.id, 'Ошибка: не найдена колонка Склад')
        return
    total = 0
    for r in rows:
        if (r[i_dom - 1] or '0').strip() == str(dom):
            try:
                total += int((r[i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                pass
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton(f'Текущее количество: 0 шт. (на складе: {total})', callback_data='noop'))
    mk.row(
        types.InlineKeyboardButton('−', callback_data='add_dec'),
        types.InlineKeyboardButton('+', callback_data='add_inc')
    )
    mk.add(
        types.InlineKeyboardButton('✅ Подтвердить', callback_data='add_confirm'),
        types.InlineKeyboardButton('Отмена', callback_data='add_cancel')
    )
    bot.edit_message_text(f'Укажи количество ключей ({"Домофон" if dom else "Дверь"}):', call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data in ('add_inc', 'add_dec', 'add_cancel', 'add_confirm'))
def add_qty_update(call):
    sel = K_SEL.get(call.from_user.id, {})
    if call.data == 'add_cancel':
        K_SEL.pop(call.from_user.id, None)
        bot.edit_message_text('Операция отменена.', call.message.chat.id, call.message.message_id)
        bot.send_message(call.message.chat.id, 'Раздел ключей:', reply_markup=keys_menu(call.message))
        return
    if call.data == 'add_confirm':
        if sel.get('qty', 0) <= 0 or sel.get('domofon') is None:
            bot.answer_callback_query(call.id, 'Задайте количество и тип ключа.')
            return
        header, rows = cache_get(sheet_keys)
        idx = header_map_from(header)
        i_stock = idx.get('Склад')
        i_dom = idx.get('С домофоном')
        if not i_stock:
            logging.error('Ошибка: не найдена колонка Склад')
            bot.answer_callback_query(call.id, 'Ошибка: не найдена колонка Склад')
            return
        qty = sel['qty']
        dom = sel['domofon']
        effective_qty = qty * 2 if dom else qty
        key_name = f'Ключ ({"Домофон" if dom else "Дверь"})'
        with sheet_lock:
            sheet_keys.append_row([key_name, '', effective_qty, '', dom] + [0] * len(OBJECTS))
            cache_invalidate(sheet_keys)
        K_SEL.pop(call.from_user.id, None)
        kind = 'Домофон' if dom else 'Дверь (физ.)'
        bot.edit_message_text(f'Добавлено на склад {kind}: {qty} шт. (зачёт: {effective_qty})', call.message.chat.id, call.message.message_id)
        bot.send_message(call.message.chat.id, 'Раздел ключей:', reply_markup=keys_menu(call.message))
        return
    qty = sel.get('qty', 0)
    if call.data == 'add_inc':
        sel['qty'] = qty + 1
    if call.data == 'add_dec' and qty > 0:
        sel['qty'] = qty - 1
    K_SEL[call.from_user.id] = sel
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_dom = idx.get('С домофоном')
    total = 0
    for r in rows:
        if (r[i_dom - 1] or '0').strip() == str(sel.get('domofon', 0)):
            try:
                total += int((r[i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                pass
    dom = sel.get('domofon', 0)
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton(f'Текущее количество: {sel["qty"]} шт. (на складе: {total})', callback_data='noop'))
    mk.row(
        types.InlineKeyboardButton('−', callback_data='add_dec'),
        types.InlineKeyboardButton('+', callback_data='add_inc')
    )
    mk.add(
        types.InlineKeyboardButton('✅ Подтвердить', callback_data='add_confirm'),
        types.InlineKeyboardButton('Отмена', callback_data='add_cancel')
    )
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)

# ================== ДОСТУПНОСТИ =================
def fetch_available_unique():
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    res = []
    i_type = idx.get('Тип')
    i_name = idx.get('Наименование')
    i_id = idx.get('ID')
    i_stock = idx.get('Склад')
    i_obj = idx.get('Объект')
    if not i_type or not i_name or (not i_stock and not i_obj):
        return res
    stock_words = {'склад', 'офис', 'в офисе', 'warehouse', 'office'}
    for r_i, row in enumerate(rows, start=2):
        if normalize_type(row[i_type - 1]) != 'Уникальный':
            continue
        on_stock = False
        if i_stock:
            try:
                s = int((row[i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                s = 0
            if s > 0:
                on_stock = True
        if not on_stock and i_obj:
            loc = (row[i_obj - 1] or '').strip().lower()
            on_stock = loc in stock_words
        if on_stock:
            rid = row[i_id - 1].strip() if i_id and len(row) >= i_id else ''
            res.append((r_i, rid, row[i_name - 1].strip()))
    return res

def fetch_available_qty():
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    res = []
    i_type = idx.get('Тип')
    i_name = idx.get('Наименование')
    i_stock = idx.get('Склад')
    if not i_type or not i_name or not i_stock:
        return res
    for r_i, row in enumerate(rows, start=2):
        if normalize_type(row[i_type - 1]) != 'Количественный':
            continue
        try:
            stock = int((row[i_stock - 1] or '0').replace(' ', '') or 0)
        except:
            stock = 0
        if stock > 0:
            res.append((r_i, row[i_name - 1].strip(), stock))
    return res

def fetch_available_keys():
    header, rows = cache_get(sheet_keys)
    idx = header_map_from(header)
    res = []
    i_key = idx.get('Ключ')
    i_dom = idx.get('С домофоном')
    i_stock = idx.get('Склад')
    if not i_key or not i_stock:
        return res
    for r_i, row in enumerate(rows, start=2):
        key_name = (row[i_key - 1] or '').strip()
        dom = 1 if i_dom and (row[i_dom - 1] or '0').strip() == '1' else 0
        try:
            stock = int((row[i_stock - 1] or '0').replace(' ', '') or 0)
        except:
            stock = 0
        if key_name and stock > 0:
            res.append((r_i, key_name, stock, dom))
    return res

# ================== МЕНЮ / СТАРТ =================
def main_kb(uid):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add('📦 Взять инструмент', '🛠 Взять предмет')
    kb.add('↩️ Вернуть инструмент', '🔄 Вернуть предмет')
    kb.add('🚚 Переместить инст/предм.', '📋 Что у меня')
    kb.add('🔑 Учёт ключей')
    if uid in ADMIN_IDS:
        kb.add('Админ панель')
    kb.add('Назад')
    return kb

@bot.message_handler(commands=['start'])
def start(message):
    U_SEL.pop(message.from_user.id, None)
    U_CONFIRMED.pop(message.from_user.id, None)
    U_RET_SEL.pop(message.from_user.id, None)
    Q_SEL.pop(message.from_user.id, None)
    Q_CONFIRMED.pop(message.from_user.id, None)
    Q_RET_STATE.pop(message.from_user.id, None)
    Q_LIST.pop(message.from_user.id, None)
    K_SEL.pop(message.from_user.id, None)
    K_CONFIRMED.pop(message.from_user.id, None)
    K_RET_STATE.pop(message.from_user.id, None)
    MV_STATE.pop(message.from_user.id, None)
    KG_SEL.pop(message.from_user.id, None)
    KR_SEL.pop(message.from_user.id, None)
    bot.send_message(message.chat.id, "Главное меню:", reply_markup=main_kb(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == 'Админ панель' and m.from_user.id in ADMIN_IDS)
def admin_panel(message):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add('/apply_colors', '➕ Добавить ключ')
    kb.add('Назад')
    bot.send_message(message.chat.id, "Админ:", reply_markup=kb)

@bot.message_handler(func=lambda m: m.text == '📋 Что у меня')
def what_i_have(message):
    nick = get_tg_nick(message.from_user)
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_type = idx.get('Тип')
    i_name = idx.get('Наименование')
    i_id = idx.get('ID')
    i_emp = idx.get('Сотрудник')
    if not all([i_type, i_name, i_emp]):
        bot.send_message(message.chat.id, "Ошибка: таблица не содержит нужных колонок.", reply_markup=main_kb(message.from_user.id))
        return
    items = []
    for r_i, r in enumerate(rows, start=2):
        if (r[i_emp - 1] or '').strip() != nick:
            continue
        t = normalize_type(r[i_type - 1])
        nm = r[i_name - 1].strip()
        for obj in OBJECTS:
            col = idx.get(obj)
            if not col:
                continue
            try:
                qty = int((r[col - 1] or '0').replace(' ', '') or 0)
            except:
                qty = 0
            if qty > 0:
                if t == 'Уникальный':
                    cap = f"{nm} ({r[i_id - 1]})" if i_id else nm
                    items.append(f"{cap} — {obj}: {qty} шт.")
                else:
                    items.append(f"{nm} — {obj}: {qty} шт.")
    if not items:
        bot.send_message(message.chat.id, "У тебя ничего нет.", reply_markup=main_kb(message.from_user.id))
        return
    bot.send_message(message.chat.id, "У тебя:\n" + "\n".join(items), reply_markup=main_kb(message.from_user.id))

# ================== УНИКАЛЬНЫЕ — ВЗЯТЬ =================
def build_u_take_markup(uid: int):
    selected = U_SEL.get(uid, set())
    avail = fetch_available_unique()
    mk = types.InlineKeyboardMarkup()
    if not avail:
        mk.add(types.InlineKeyboardButton("Нет доступных на складе", callback_data="noop"))
        return mk, []
    for row_i, tid, name in avail:
        checked = ' ✅' if row_i in selected else ''
        cap = f"{name} ({tid}){checked}" if tid else f"{name}{checked}"
        mk.add(types.InlineKeyboardButton(cap, callback_data=f"utoggle_{row_i}"))
    mk.add(
        types.InlineKeyboardButton("✅ Подтвердить", callback_data="uconfirm"),
        types.InlineKeyboardButton("↩️ Отмена", callback_data="ucancel")
    )
    return mk, avail

@bot.message_handler(func=lambda m: m.text == '📦 Взять инструмент')
def take_tool(message):
    U_SEL[message.from_user.id] = set()
    mk, avail = build_u_take_markup(message.from_user.id)
    if not avail:
        bot.send_message(message.chat.id, "Нет уникальных инструментов на складе.", reply_markup=main_kb(message.from_user.id))
        return
    bot.send_message(message.chat.id, "Отметь инструменты (несколько), затем «✅ Подтвердить».", reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('utoggle_'))
def utoggle_cb(call):
    try:
        row_i = int(call.data.split('_')[-1])
    except:
        bot.answer_callback_query(call.id, "Некорректно.")
        return
    sel = U_SEL.setdefault(call.from_user.id, set())
    if row_i in sel:
        sel.remove(row_i)
    else:
        sel.add(row_i)
    mk, _ = build_u_take_markup(call.from_user.id)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)
    except Exception:
        bot.send_message(call.message.chat.id, "Обновлено:", reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data == 'ucancel')
def ucancel_cb(call):
    U_SEL.pop(call.from_user.id, None)
    bot.edit_message_text("Выбор отменён.", call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

@bot.callback_query_handler(func=lambda c: c.data == 'uconfirm')
def uconfirm_cb(call):
    selected = list(U_SEL.get(call.from_user.id, set()))
    if not selected:
        bot.answer_callback_query(call.id, "Выбери хоть один инструмент.")
        return
    U_CONFIRMED[call.from_user.id] = selected
    bot.edit_message_text("Куда выдать выбранные инструменты?",
                         call.message.chat.id, call.message.message_id,
                         reply_markup=objects_inline(prefix="uissue_"))

@bot.callback_query_handler(func=lambda c: c.data.startswith('uissue_'))
def uissue_cb(call):
    obj = call.data[len('uissue_'):]
    rows = U_CONFIRMED.get(call.from_user.id, [])
    if not rows:
        bot.answer_callback_query(call.id, "Список пуст.")
        return
    header, allrows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_obj = idx.get('Объект')
    i_emp = idx.get('Сотрудник')
    if obj not in idx:
        bot.answer_callback_query(call.id, "Нет такой колонки объекта.")
        return
    i_objcol = idx[obj]
    updates = []
    done = 0
    skipped = 0
    for row_i in rows:
        row = allrows[row_i - 2] if 0 <= row_i - 2 < len(allrows) else []
        on_stock = False
        if i_stock:
            try:
                s = int((row[i_stock - 1] or '0').replace(' ', '') or 0)
            except:
                s = 0
            if s > 0:
                updates.append((row_i, i_stock, s - 1))
                on_stock = True
        if not on_stock and i_obj:
            loc = (row[i_obj - 1] or '').strip().lower()
            if loc in {'склад', 'офис', 'в офисе', 'warehouse', 'office'}:
                on_stock = True
        if not on_stock:
            skipped += 1
            continue
        try:
            curr = int((row[i_objcol - 1] or '0').replace(' ', '') or 0)
        except:
            curr = 0
        updates.append((row_i, i_objcol, curr + 1))
        if i_obj:
            updates.append((row_i, i_obj, obj))
        if i_emp:
            updates.append((row_i, i_emp, get_tg_nick(call.from_user)))
        done += 1
    with sheet_lock:
        batch_write(sheet_log, updates)
    U_SEL.pop(call.from_user.id, None)
    U_CONFIRMED.pop(call.from_user.id, None)
    msg = []
    if done:
        msg.append(f"Выдано на {obj}: {done} шт.")
    if skipped:
        msg.append(f"Пропущено (не на складе): {skipped}")
    if not msg:
        msg = ["Ничего не выдано."]
    bot.edit_message_text("\n".join(msg), call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

# ================== УНИКАЛЬНЫЕ — ВОЗВРАТ =================
def build_u_ret_markup(uid, obj, user_only=False):
    nick = get_tg_nick(bot.get_chat_member(uid, uid).user)
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_type = idx.get('Тип')
    i_name = idx.get('Наименование')
    i_id = idx.get('ID')
    i_obj = idx.get('Объект')
    i_emp = idx.get('Сотрудник')
    if not all([i_type, i_name, i_obj, i_emp]):
        return []
    entries = []
    for r_i, r in enumerate(rows, start=2):
        if normalize_type(r[i_type - 1]) != 'Уникальный':
            continue
        if user_only and (r[i_emp - 1] or '').strip() != nick:
            continue
        obj_now = (r[i_obj - 1] or '').strip()
        if obj_now and obj_now.lower() == obj.lower():
            col = idx.get(obj)
            if col:
                try:
                    qty = int((r[col - 1] or '0').replace(' ', '') or 0)
                except:
                    qty = 0
                if qty > 0:
                    label = f"{r[i_name - 1]} ({r[i_id - 1]})" if i_id else r[i_name - 1]
                    emp = r[i_emp - 1] if i_emp else 'Неизвестно'
                    entries.append((r_i, label, obj, emp))
    return entries

def uret_markup(uid: int, obj: str, user_only: bool):
    chosen = U_RET_SEL.get(uid, {'obj': None, 'items': set()})['items']
    entries = build_u_ret_markup(uid, obj, user_only)
    mk = types.InlineKeyboardMarkup()
    if not entries:
        mk.add(types.InlineKeyboardButton("Нечего возвращать", callback_data="noop"))
        return mk, entries
    for r_i, label, obj, emp in entries:
        checked = ' ✅' if r_i in chosen else ''
        display_label = f"{label} с {obj}{checked}" if user_only else f"{label} с {obj} (у {emp}){checked}"
        mk.add(types.InlineKeyboardButton(display_label, callback_data=f"uret_toggle_{r_i}"))
    mk.add(
        types.InlineKeyboardButton("✅ Подтвердить возврат", callback_data="uret_confirm"),
        types.InlineKeyboardButton("↩️ Отмена", callback_data="uret_cancel")
    )
    return mk, entries

@bot.message_handler(func=lambda m: m.text == '↩️ Вернуть инструмент')
def ret_tool_menu(message):
    U_RET_SEL[message.from_user.id] = {'obj': None, 'items': set(), 'user_only': False}
    mk = types.InlineKeyboardMarkup()
    mk.add(
        types.InlineKeyboardButton("Что у меня", callback_data="uret_my"),
        types.InlineKeyboardButton("Что на объектах", callback_data="uret_all")
    )
    bot.send_message(message.chat.id, "Выбери, что показать:", reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data in ('uret_my', 'uret_all'))
def uret_choice(call):
    user_only = call.data == 'uret_my'
    U_RET_SEL[call.from_user.id] = {'obj': None, 'items': set(), 'user_only': user_only}
    bot.edit_message_text("С какого объекта возвращаем инструменты?",
                         call.message.chat.id, call.message.message_id,
                         reply_markup=objects_inline(prefix="uret_obj_"))

@bot.callback_query_handler(func=lambda c: c.data.startswith('uret_obj_'))
def uret_obj(call):
    obj = call.data[len('uret_obj_'):]
    st = U_RET_SEL.get(call.from_user.id, {'obj': None, 'items': set(), 'user_only': False})
    st['obj'] = obj
    st['items'] = set()
    U_RET_SEL[call.from_user.id] = st
    mk, entries = uret_markup(call.from_user.id, obj, st['user_only'])
    if not entries:
        bot.edit_message_text(f"На {obj} нет {'твоих ' if st['user_only'] else ''}инструментов.", call.message.chat.id, call.message.message_id)
        return
    bot.edit_message_text(f"Выбери инструменты для возврата с {obj} (➖/➕), затем «✅ Подтвердить».",
                         call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('uret_toggle_'))
def uret_toggle(call):
    try:
        row_i = int(call.data.split('_')[-1])
    except:
        bot.answer_callback_query(call.id, "Некорректно.")
        return
    st = U_RET_SEL.get(call.from_user.id, {'obj': None, 'items': set(), 'user_only': False})
    sel = st['items']
    if row_i in sel:
        sel.remove(row_i)
    else:
        sel.add(row_i)
    U_RET_SEL[call.from_user.id] = st
    mk, _ = uret_markup(call.from_user.id, st['obj'], st['user_only'])
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)
    except Exception:
        bot.send_message(call.message.chat.id, "Обновлено:", reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data == 'uret_cancel')
def uret_cancel(call):
    U_RET_SEL.pop(call.from_user.id, None)
    bot.edit_message_text("Возврат отменён.", call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

@bot.callback_query_handler(func=lambda c: c.data == 'uret_confirm')
def uret_confirm(call):
    st = U_RET_SEL.get(call.from_user.id, {'obj': None, 'items': set(), 'user_only': False})
    rows = list(st['items'])
    if not rows:
        bot.answer_callback_query(call.id, "Отметь хотя бы один инструмент.")
        return
    header, allrows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_obj = idx.get('Объект')
    i_emp = idx.get('Сотрудник')
    obj = st['obj']
    col_now = idx.get(obj)
    updates = []
    done = 0
    for row_i in rows:
        row = allrows[row_i - 2]
        if col_now:
            try:
                on_obj = int((row[col_now - 1] or '0').replace(' ', '') or 0)
            except:
                on_obj = 0
            if on_obj > 0:
                updates.append((row_i, col_now, on_obj - 1))
                if i_stock:
                    try:
                        s = int((row[i_stock - 1] or '0').replace(' ', '') or 0)
                    except:
                        s = 0
                    updates.append((row_i, i_stock, s + 1))
                updates.append((row_i, i_obj, 'Склад'))
                if st['user_only']:
                    updates.append((row_i, i_emp, ''))
                done += 1
    with sheet_lock:
        batch_write(sheet_log, updates)
    U_RET_SEL.pop(call.from_user.id, None)
    bot.edit_message_text(f"Возвращено на склад: {done} шт.", call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

# ================== КОЛИЧЕСТВЕННЫЕ — ВЗЯТЬ =================
def q_take_markup(uid: int):
    snapshot = Q_LIST.get(uid, [])
    sel = Q_SEL.setdefault(uid, {})
    mk = types.InlineKeyboardMarkup()
    if not snapshot:
        mk.add(types.InlineKeyboardButton("Нет доступных на складе", callback_data="noop"))
        return mk, snapshot
    for row_i, name, stock in snapshot:
        qty = sel.get(row_i, 0)
        mk.add(types.InlineKeyboardButton(f"{name} (на складе: {stock}) — {qty} шт.", callback_data="noop"))
        mk.row(
            types.InlineKeyboardButton("➖", callback_data=f"q_dec_{row_i}"),
            types.InlineKeyboardButton("➕", callback_data=f"q_inc_{row_i}")
        )
    mk.add(
        types.InlineKeyboardButton("✅ Подтвердить", callback_data="q_confirm"),
        types.InlineKeyboardButton("↩️ Отмена", callback_data="q_cancel")
    )
    return mk, snapshot

@bot.message_handler(func=lambda m: m.text == '🛠 Взять предмет')
def q_take(message):
    avail = fetch_available_qty()
    Q_LIST[message.from_user.id] = avail
    Q_SEL[message.from_user.id] = {}
    mk, snapshot = q_take_markup(message.from_user.id)
    if not snapshot:
        bot.send_message(message.chat.id, "Нет доступных предметов.", reply_markup=main_kb(message.from_user.id))
        return
    bot.send_message(message.chat.id, "Выбери количество для нескольких предметов (➖/➕), затем «✅ Подтвердить».",
                     reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('q_inc_') or c.data.startswith('q_dec_'))
def q_incdec(call):
    try:
        inc = call.data.startswith('q_inc_')
        row_i = int(call.data.split('_')[-1])
    except Exception:
        bot.answer_callback_query(call.id, "Ошибка данных.")
        return
    uid = call.from_user.id
    snapshot = Q_LIST.get(uid, [])
    sel = Q_SEL.setdefault(uid, {})
    stock_map = {r_i: stock for r_i, _, stock in snapshot}
    stock = stock_map.get(row_i, 0)
    cur = sel.get(row_i, 0)
    if inc and cur < stock:
        sel[row_i] = cur + 1
    elif (not inc) and cur > 0:
        sel[row_i] = cur - 1
    mk, _ = q_take_markup(uid)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)
    except Exception:
        bot.send_message(call.message.chat.id, "Обновлено:", reply_markup=mk)
    bot.answer_callback_query(call.id, f"Текущая корзина: {sum(sel.values())} шт.")

@bot.callback_query_handler(func=lambda c: c.data == 'q_cancel')
def q_cancel(call):
    Q_SEL.pop(call.from_user.id, None)
    Q_LIST.pop(call.from_user.id, None)
    bot.edit_message_text("Выбор отменён.", call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

@bot.callback_query_handler(func=lambda c: c.data == 'q_confirm')
def q_confirm(call):
    uid = call.from_user.id
    sel = {k: v for k, v in Q_SEL.get(uid, {}).items() if v > 0}
    if not sel:
        bot.answer_callback_query(call.id, "Нужно выбрать количество хотя бы для одного предмета.")
        return
    Q_CONFIRMED[uid] = sel
    bot.edit_message_text("Куда выдать выбранные предметы?",
                         call.message.chat.id, call.message.message_id,
                         reply_markup=objects_inline(prefix="q_issue_"))

@bot.callback_query_handler(func=lambda c: c.data.startswith('q_issue_'))
def q_issue(call):
    obj = call.data[len('q_issue_'):]
    sel = Q_CONFIRMED.get(call.from_user.id, {})
    if not sel:
        bot.answer_callback_query(call.id, "Список пуст.")
        return
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_obj = idx.get('Объект')
    i_emp = idx.get('Сотрудник')
    if obj not in idx:
        bot.answer_callback_query(call.id, "Нет колонки объекта.")
        return
    i_objcol = idx[obj]
    updates = []
    done = 0
    skipped = []
    for row_i, qty in sel.items():
        row = rows[row_i - 2]
        try:
            stock = int((row[i_stock - 1] or '0').replace(' ', '') or 0)
        except:
            stock = 0
        if stock < qty:
            skipped.append((row_i, qty))
            continue
        updates.append((row_i, i_stock, stock - qty))
        try:
            curr = int((row[i_objcol - 1] or '0').replace(' ', '') or 0)
        except:
            curr = 0
        updates.append((row_i, i_objcol, curr + qty))
        if i_obj:
            updates.append((row_i, i_obj, obj))
        if i_emp:
            updates.append((row_i, i_emp, get_tg_nick(call.from_user)))
        done += qty
    with sheet_lock:
        batch_write(sheet_log, updates)
    Q_SEL.pop(call.from_user.id, None)
    Q_CONFIRMED.pop(call.from_user.id, None)
    Q_LIST.pop(call.from_user.id, None)
    msg = []
    if done:
        msg.append(f"Выдано на {obj}: {done} шт.")
    if skipped:
        msg.append(f"Пропущено (нехватка на складе): {sum(qty for _, qty in skipped)} шт.")
    if not msg:
        msg = ["Ничего не выдано."]
    bot.edit_message_text("\n".join(msg), call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

# ================== КОЛИЧЕСТВЕННЫЕ — ВОЗВРАТ =================
def q_ret_markup(uid: int, obj: str, user_only: bool):
    nick = get_tg_nick(bot.get_chat_member(uid, uid).user)
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_type = idx.get('Тип')
    i_name = idx.get('Наименование')
    i_obj = idx.get('Объект')
    i_emp = idx.get('Сотрудник')
    i_objcol = idx.get(obj)
    if not all([i_type, i_name, i_obj, i_emp, i_objcol]):
        return [], []
    entries = []
    sel = Q_RET_STATE.get(uid, {'obj': None, 'items': {}}).get('items', {})
    for r_i, r in enumerate(rows, start=2):
        if normalize_type(r[i_type - 1]) != 'Количественный':
            continue
        if user_only and (r[i_emp - 1] or '').strip() != nick:
            continue
        obj_now = (r[i_obj - 1] or '').strip()
        if obj_now and obj_now.lower() == obj.lower():
            try:
                qty = int((r[i_objcol - 1] or '0').replace(' ', '') or 0)
            except:
                qty = 0
            if qty > 0:
                label = r[i_name - 1].strip()
                emp = r[i_emp - 1] if i_emp else 'Неизвестно'
                entries.append((r_i, label, qty, emp))
    mk = types.InlineKeyboardMarkup()
    if not entries:
        mk.add(types.InlineKeyboardButton("Нечего возвращать", callback_data="noop"))
        return mk, entries
    for r_i, label, qty, emp in entries:
        sel_qty = sel.get(r_i, 0)
        display_label = f"{label} ({qty} шт.) — вернуть: {sel_qty}" if user_only else f"{label} ({qty} шт.) у {emp} — вернуть: {sel_qty}"
        mk.add(types.InlineKeyboardButton(display_label, callback_data=f"qret_noop_{r_i}"))
        mk.row(
            types.InlineKeyboardButton("➖", callback_data=f"qret_dec_{r_i}"),
            types.InlineKeyboardButton("➕", callback_data=f"qret_inc_{r_i}")
        )
    mk.add(
        types.InlineKeyboardButton("✅ Подтвердить", callback_data="qret_confirm"),
        types.InlineKeyboardButton("↩️ Отмена", callback_data="qret_cancel")
    )
    return mk, entries

@bot.message_handler(func=lambda m: m.text == '🔄 Вернуть предмет')
def q_ret_menu(message):
    Q_RET_STATE[message.from_user.id] = {'obj': None, 'items': {}, 'user_only': False}
    mk = types.InlineKeyboardMarkup()
    mk.add(
        types.InlineKeyboardButton("Что у меня", callback_data="qret_my"),
        types.InlineKeyboardButton("Что на объектах", callback_data="qret_all")
    )
    bot.send_message(message.chat.id, "Выбери, что показать:", reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data in ('qret_my', 'qret_all'))
def qret_choice(call):
    user_only = call.data == 'qret_my'
    Q_RET_STATE[call.from_user.id] = {'obj': None, 'items': {}, 'user_only': user_only}
    bot.edit_message_text("С какого объекта возвращаем предметы?",
                         call.message.chat.id, call.message.message_id,
                         reply_markup=objects_inline(prefix="qret_obj_"))

@bot.callback_query_handler(func=lambda c: c.data.startswith('qret_obj_'))
def qret_obj(call):
    obj = call.data[len('qret_obj_'):]
    st = Q_RET_STATE.get(call.from_user.id, {'obj': None, 'items': {}, 'user_only': False})
    st['obj'] = obj
    st['items'] = {}
    Q_RET_STATE[call.from_user.id] = st
    mk, entries = q_ret_markup(call.from_user.id, obj, st['user_only'])
    if not entries:
        bot.edit_message_text(f"На {obj} нет {'твоих ' if st['user_only'] else ''}предметов.", call.message.chat.id, call.message.message_id)
        return
    bot.edit_message_text(f"Выбери количество для возврата с {obj} (➖/➕), затем «✅ Подтвердить».",
                         call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('qret_inc_') or c.data.startswith('qret_dec_') or c.data.startswith('qret_noop_'))
def qret_incdec(call):
    try:
        if call.data.startswith('qret_noop_'):
            return
        inc = call.data.startswith('qret_inc_')
        row_i = int(call.data.split('_')[-1])
    except Exception:
        bot.answer_callback_query(call.id, "Ошибка данных.")
        return
    uid = call.from_user.id
    st = Q_RET_STATE.get(uid, {'obj': None, 'items': {}, 'user_only': False})
    sel = st['items']
    obj = st['obj']
    user_only = st['user_only']
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_objcol = idx.get(obj)
    if not i_objcol:
        bot.answer_callback_query(call.id, "Колонка объекта не найдена.")
        return
    row = rows[row_i - 2]
    try:
        max_qty = int((row[i_objcol - 1] or '0').replace(' ', '') or 0)
    except:
        max_qty = 0
    cur = sel.get(row_i, 0)
    if inc and cur < max_qty:
        sel[row_i] = cur + 1
    elif (not inc) and cur > 0:
        sel[row_i] = cur - 1
    Q_RET_STATE[uid] = st
    mk, _ = q_ret_markup(uid, obj, user_only)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)
    except Exception:
        bot.send_message(call.message.chat.id, "Обновлено:", reply_markup=mk)
    bot.answer_callback_query(call.id, f"Текущая корзина: {sum(sel.values())} шт.")

@bot.callback_query_handler(func=lambda c: c.data == 'qret_cancel')
def qret_cancel(call):
    Q_RET_STATE.pop(call.from_user.id, None)
    bot.edit_message_text("Возврат отменён.", call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

@bot.callback_query_handler(func=lambda c: c.data == 'qret_confirm')
def qret_confirm(call):
    st = Q_RET_STATE.get(call.from_user.id, {'obj': None, 'items': {}, 'user_only': False})
    sel = {k: v for k, v in st['items'].items() if v > 0}
    if not sel:
        bot.answer_callback_query(call.id, "Выбери количество хотя бы для одного предмета.")
        return
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_stock = idx.get('Склад')
    i_obj = idx.get('Объект')
    i_emp = idx.get('Сотрудник')
    i_objcol = idx.get(st['obj'])
    if not all([i_stock, i_obj, i_emp, i_objcol]):
        bot.answer_callback_query(call.id, "Ошибка: таблица не содержит нужных колонок.")
        return
    updates = []
    done = 0
    for row_i, qty in sel.items():
        row = rows[row_i - 2]
        try:
            curr = int((row[i_objcol - 1] or '0').replace(' ', '') or 0)
        except:
            curr = 0
        if curr < qty:
            bot.answer_callback_query(call.id, f"Недостаточно на объекте: {curr} шт.")
            continue
        updates.append((row_i, i_objcol, curr - qty))
        try:
            stock = int((row[i_stock - 1] or '0').replace(' ', '') or 0)
        except:
            stock = 0
        updates.append((row_i, i_stock, stock + qty))
        updates.append((row_i, i_obj, 'Склад'))
        if st['user_only']:
            updates.append((row_i, i_emp, ''))
        done += qty
    with sheet_lock:
        batch_write(sheet_log, updates)
    Q_RET_STATE.pop(call.from_user.id, None)
    bot.edit_message_text(f"Возвращено на склад: {done} шт.", call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

# ================== ПЕРЕМЕЩЕНИЕ ИНСТРУМЕНТОВ/ПРЕДМЕТОВ =================
def mv_build_markup(uid: int, obj_from: str, user_only: bool):
    nick = get_tg_nick(bot.get_chat_member(uid, uid).user)
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_type = idx.get('Тип')
    i_name = idx.get('Наименование')
    i_id = idx.get('ID')
    i_obj = idx.get('Объект')
    i_emp = idx.get('Сотрудник')
    i_objcol = idx.get(obj_from)
    if not all([i_type, i_name, i_obj, i_emp, i_objcol]):
        return [], []
    entries = []
    sel = MV_STATE.get(uid, {'obj_from': None, 'obj_to': None, 'items': {}}).get('items', {})
    for r_i, r in enumerate(rows, start=2):
        obj_now = (r[i_obj - 1] or '').strip()
        if obj_now and obj_now.lower() == obj_from.lower():
            try:
                qty = int((r[i_objcol - 1] or '0').replace(' ', '') or 0)
            except:
                qty = 0
            if qty <= 0:
                continue
            if user_only and (r[i_emp - 1] or '').strip() != nick:
                continue
            t = normalize_type(r[i_type - 1])
            label = r[i_name - 1].strip()
            if t == 'Уникальный' and i_id:
                label += f" ({r[i_id - 1]})"
            emp = r[i_emp - 1] if i_emp else 'Неизвестно'
            entries.append((r_i, label, qty, t, emp))
    mk = types.InlineKeyboardMarkup()
    if not entries:
        mk.add(types.InlineKeyboardButton("Нечего перемещать", callback_data="noop"))
        return mk, entries
    for r_i, label, qty, t, emp in entries:
        sel_qty = sel.get(r_i, 0 if t == 'Количественный' else (1 if r_i in sel else 0))
        display_label = f"{label} ({qty} шт.) — взять: {sel_qty}" if user_only else f"{label} ({qty} шт.) у {emp} — взять: {sel_qty}"
        mk.add(types.InlineKeyboardButton(display_label, callback_data=f"mv_noop_{r_i}"))
        if t == 'Количественный':
            mk.row(
                types.InlineKeyboardButton("➖", callback_data=f"mv_dec_{r_i}"),
                types.InlineKeyboardButton("➕", callback_data=f"mv_inc_{r_i}")
            )
        else:
            mk.add(types.InlineKeyboardButton("✅" if r_i in sel else "⬜", callback_data=f"mv_toggle_{r_i}"))
    mk.add(
        types.InlineKeyboardButton("Дальше ➡️", callback_data="mv_next"),
        types.InlineKeyboardButton("↩️ Отмена", callback_data="mv_cancel")
    )
    return mk, entries

@bot.message_handler(func=lambda m: m.text == '🚚 Переместить инст/предм.')
def mv_start(message):
    MV_STATE[message.from_user.id] = {'obj_from': None, 'obj_to': None, 'items': {}, 'user_only': False}
    mk = types.InlineKeyboardMarkup()
    mk.add(
        types.InlineKeyboardButton("Что у меня", callback_data="mv_my"),
        types.InlineKeyboardButton("Что на объектах", callback_data="mv_all")
    )
    bot.send_message(message.chat.id, "Выбери, что показать:", reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data in ('mv_my', 'mv_all'))
def mv_choice(call):
    user_only = call.data == 'mv_my'
    MV_STATE[call.from_user.id] = {'obj_from': None, 'obj_to': None, 'items': {}, 'user_only': user_only}
    bot.edit_message_text("С какого объекта берём инструменты/предметы?",
                         call.message.chat.id, call.message.message_id,
                         reply_markup=objects_inline(prefix="mv_from_"))

@bot.callback_query_handler(func=lambda c: c.data.startswith('mv_from_'))
def mv_from(call):
    obj_from = call.data[len('mv_from_'):]
    st = MV_STATE.get(call.from_user.id, {'obj_from': None, 'obj_to': None, 'items': {}, 'user_only': False})
    st['obj_from'] = obj_from
    st['items'] = {}
    MV_STATE[call.from_user.id] = st
    mk, entries = mv_build_markup(call.from_user.id, obj_from, st['user_only'])
    if not entries:
        bot.edit_message_text(f"На {obj_from} нет {'твоих ' if st['user_only'] else ''}инструментов/предметов.", call.message.chat.id, call.message.message_id)
        return
    bot.edit_message_text(f"Выбери, что взять с {obj_from}, затем «Дальше ➡️».",
                         call.message.chat.id, call.message.message_id, reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('mv_toggle_'))
def mv_toggle(call):
    try:
        row_i = int(call.data.split('_')[-1])
    except:
        bot.answer_callback_query(call.id, "Некорректно.")
        return
    st = MV_STATE.get(call.from_user.id, {'obj_from': None, 'obj_to': None, 'items': {}, 'user_only': False})
    sel = st['items']
    if row_i in sel:
        sel.pop(row_i)
    else:
        sel[row_i] = 1
    MV_STATE[call.from_user.id] = st
    mk, _ = mv_build_markup(call.from_user.id, st['obj_from'], st['user_only'])
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)
    except Exception:
        bot.send_message(call.message.chat.id, "Обновлено:", reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith('mv_inc_') or c.data.startswith('mv_dec_') or c.data.startswith('mv_noop_'))
def mv_incdec(call):
    try:
        if call.data.startswith('mv_noop_'):
            return
        inc = call.data.startswith('mv_inc_')
        row_i = int(call.data.split('_')[-1])
    except Exception:
        bot.answer_callback_query(call.id, "Ошибка данных.")
        return
    st = MV_STATE.get(call.from_user.id, {'obj_from': None, 'obj_to': None, 'items': {}, 'user_only': False})
    sel = st['items']
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_objcol = idx.get(st['obj_from'])
    if not i_objcol:
        bot.answer_callback_query(call.id, "Колонка объекта не найдена.")
        return
    row = rows[row_i - 2]
    try:
        max_qty = int((row[i_objcol - 1] or '0').replace(' ', '') or 0)
    except:
        max_qty = 0
    cur = sel.get(row_i, 0)
    if inc and cur < max_qty:
        sel[row_i] = cur + 1
    elif (not inc) and cur > 0:
        sel[row_i] = cur - 1
    MV_STATE[call.from_user.id] = st
    mk, _ = mv_build_markup(call.from_user.id, st['obj_from'], st['user_only'])
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=mk)
    except Exception:
        bot.send_message(call.message.chat.id, "Обновлено:", reply_markup=mk)
    bot.answer_callback_query(call.id, f"Текущая корзина: {sum(sel.values())} шт.")

@bot.callback_query_handler(func=lambda c: c.data == 'mv_cancel')
def mv_cancel(call):
    MV_STATE.pop(call.from_user.id, None)
    bot.edit_message_text("Перемещение отменено.", call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

@bot.callback_query_handler(func=lambda c: c.data == 'mv_next')
def mv_next(call):
    st = MV_STATE.get(call.from_user.id, {'obj_from': None, 'obj_to': None, 'items': {}, 'user_only': False})
    if not st['items']:
        bot.answer_callback_query(call.id, "Выбери хотя бы один инструмент/предмет.")
        return
    bot.edit_message_text(f"Куда переместить с {st['obj_from']}?",
                         call.message.chat.id, call.message.message_id,
                         reply_markup=objects_inline(prefix="mv_to_", exclude=st['obj_from']))

@bot.callback_query_handler(func=lambda c: c.data.startswith('mv_to_'))
def mv_to(call):
    st = MV_STATE.get(call.from_user.id, {'obj_from': None, 'obj_to': None, 'items': {}, 'user_only': False})
    obj_to = call.data[len('mv_to_'):]
    st['obj_to'] = obj_to
    sel = {k: v for k, v in st['items'].items() if v > 0}
    if not sel:
        bot.answer_callback_query(call.id, "Список пуст.")
        return
    header, rows = cache_get(sheet_log)
    idx = header_map_from(header)
    i_obj_from = idx.get(st['obj_from'])
    i_obj_to = idx.get(obj_to)
    i_obj = idx.get('Объект')
    i_emp = idx.get('Сотрудник')
    if not all([i_obj_from, i_obj_to]):
        bot.answer_callback_query(call.id, "Ошибка: колонки объектов не найдены.")
        return
    updates = []
    done = 0
    skipped = []
    for row_i, qty in sel.items():
        row = rows[row_i - 2]
        try:
            curr_from = int((row[i_obj_from - 1] or '0').replace(' ', '') or 0)
        except:
            curr_from = 0
        if curr_from < qty:
            skipped.append((row_i, qty))
            continue
        try:
            curr_to = int((row[i_obj_to - 1] or '0').replace(' ', '') or 0)
        except:
            curr_to = 0
        updates.append((row_i, i_obj_from, curr_from - qty))
        updates.append((row_i, i_obj_to, curr_to + qty))
        if i_obj:
            updates.append((row_i, i_obj, obj_to))
        if st['user_only'] and i_emp:
            updates.append((row_i, i_emp, get_tg_nick(call.from_user)))
        done += qty
    with sheet_lock:
        batch_write(sheet_log, updates)
    MV_STATE.pop(call.from_user.id, None)
    msg = []
    if done:
        msg.append(f"Перемещено с {st['obj_from']} на {obj_to}: {done} шт.")
    if skipped:
        msg.append(f"Пропущено (нехватка): {sum(qty for _, qty in skipped)} шт.")
    if not msg:
        msg = ["Ничего не перемещено."]
    bot.edit_message_text("\n".join(msg), call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "Главное меню:", reply_markup=main_kb(call.from_user.id))

# ================== АДМИН — ПРИМЕНЕНИЕ ЦВЕТОВ =================
@bot.message_handler(commands=['apply_colors'])
def apply_colors(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "Доступно только админам.", reply_markup=main_kb(message.from_user.id))
        return
    if not HAVE_FMT:
        bot.send_message(message.chat.id, "Модуль gspread-formatting не установлен.", reply_markup=main_kb(message.from_user.id))
        return
    try:
        header, rows = cache_get(sheet_log)
        idx = header_map_from(header)
        i_type = idx.get('Тип')
        i_stock = idx.get('Склад')
        if not i_type or not i_stock:
            bot.send_message(message.chat.id, "Ошибка: колонки Тип или Склад не найдены.", reply_markup=main_kb(message.from_user.id))
            return
        unique_color = Color(0.9, 1.0, 0.9)  # Светло-зелёный
        qty_color = Color(1.0, 1.0, 0.8)    # Светло-жёлтый
        formats = []
        for r_i, row in enumerate(rows, start=2):
            t = normalize_type(row[i_type - 1])
            fmt = CellFormat(backgroundColor=unique_color if t == 'Уникальный' else qty_color)
            formats.append((r_i, 1, fmt))
        with sheet_lock:
            for row_i, col_i, fmt in formats:
                format_cell_range(sheet_log, f"{a1(row_i, col_i)}:{a1(row_i, len(header))}", fmt)
        cache_invalidate(sheet_log)
        bot.send_message(message.chat.id, "Цвета применены к строкам в таблице Log.", reply_markup=main_kb(message.from_user.id))
    except Exception as e:
        logging.error(f"Ошибка при применении цветов: {e}")
        bot.send_message(message.chat.id, f"Ошибка при применении цветов: {e}", reply_markup=main_kb(message.from_user.id))

# ================== ОБРАБОТКА НАЗАД =================
@bot.message_handler(func=lambda m: m.text == 'Назад' or m.text == '🔙 Назад')
def go_back(message):
    U_SEL.pop(message.from_user.id, None)
    U_CONFIRMED.pop(message.from_user.id, None)
    U_RET_SEL.pop(message.from_user.id, None)
    Q_SEL.pop(message.from_user.id, None)
    Q_CONFIRMED.pop(message.from_user.id, None)
    Q_RET_STATE.pop(message.from_user.id, None)
    Q_LIST.pop(message.from_user.id, None)
    K_SEL.pop(message.from_user.id, None)
    K_CONFIRMED.pop(message.from_user.id, None)
    K_RET_STATE.pop(message.from_user.id, None)
    MV_STATE.pop(message.from_user.id, None)
    KG_SEL.pop(message.from_user.id, None)
    KR_SEL.pop(message.from_user.id, None)
    bot.send_message(message.chat.id, "Главное меню:", reply_markup=main_kb(message.from_user.id))

# ================== ЗАПУСК БОТА =================
if __name__ == '__main__':
    logging.info("Запуск бота...")
    while True:
        try:
            bot.polling(none_stop=True, interval=0)
        except Exception as e:
            logging.error(f"Ошибка в polling: {e}")
            time.sleep(5)

