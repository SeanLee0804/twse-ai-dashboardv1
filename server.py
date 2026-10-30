"""TWSE Pro local data gateway. Python 3.9+ stdlib only. Binds to localhost.
Data: TWSE & TPEx public market snapshots; FinMind optional OHLC, TWSE historic OHLC fallback.
Never manufactures prices. This is NOT a trading or real-time quotation service.
"""
from __future__ import annotations
import json, os, re, sys, time, threading, traceback, socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, timezone
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

BASE = Path(__file__).resolve().parent
HOST = os.environ.get('HOST', '0.0.0.0' if os.environ.get('PORT') else '127.0.0.1')
PORT = int(os.environ.get('PORT', os.environ.get('TWSE_PORT', '8000')))
ALLOWED_ORIGINS = {v.strip() for v in os.environ.get('ALLOWED_ORIGINS', 'https://seanlee0804.github.io,http://localhost:8000,http://127.0.0.1:8000').split(',') if v.strip()}
FINMIND_TOKEN = os.environ.get('FINMIND_TOKEN', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '').strip()
AI_ALLOWED_EMAILS = {v.strip().lower() for v in os.environ.get('AI_ALLOWED_EMAILS', '').split(',') if v.strip()}
AI_LOCK = threading.Lock()
AI_LAST_CALL = {}
CACHE = {}
LOCK = threading.Lock()
MAX_BYTES = 30_000_000
USER_AGENT = 'TWSE-Research-Dashboard/1.1 (personal research; https://openapi.twse.com.tw/)'
REGISTRIES = {
    '上市': 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L',
    '上櫃': 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O',
}
MARKETS = {
    '上市': 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
    '上櫃': 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes',
}

class DataError(Exception):
    def __init__(self, message, status=502):
        super().__init__(message)
        self.status = status


def fetch_json(url, headers=None, timeout=14):
    req = Request(url, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json', **(headers or {})})
    try:
        with urlopen(req, timeout=timeout) as res:
            data = res.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise DataError('資料來源回應過大')
            return json.loads(data.decode('utf-8-sig'))
    except HTTPError as e:
        try:
            details = e.read(1024).decode('utf-8', 'replace')
        except Exception:
            details = ''
        raise DataError(f'資料來源 HTTP {e.code}: {details[:110]}', 502)
    except (URLError, TimeoutError) as e:
        raise DataError(f'資料來源連線逾時或被拒：{e}', 503)
    except (ValueError, UnicodeDecodeError) as e:
        raise DataError(f'資料來源 JSON 解析錯誤：{e}', 502)


def cached(key, ttl, loader):
    now = time.time()
    with LOCK:
        old = CACHE.get(key)
        if old and now - old[0] < ttl:
            return old[1]
    value = loader()
    with LOCK:
        CACHE[key] = (time.time(), value)
    return value


def num(value):
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value or '').strip().replace(',', '').replace('＋', '+').replace('－', '-')
    if not s or s in ('--', '---', 'N/A', '除權', '除息', '－'):
        return None
    try:
        value = float(s)
        return value if value == value and abs(value) < 1e17 else None
    except ValueError:
        return None


def iso_date(value):
    s = str(value or '').strip()
    m = re.fullmatch(r'(\d{3})(\d{2})(\d{2})', s)
    if m:
        return f'{int(m[1])+1911:04d}-{m[2]}-{m[3]}'
    m = re.fullmatch(r'(\d{2,3})/(\d{1,2})/(\d{1,2})', s)
    if m:
        return f'{int(m[1])+1911:04d}-{int(m[2]):02d}-{int(m[3]):02d}'
    m = re.fullmatch(r'(\d{4})-(\d{2})-(\d{2})', s)
    return s if m else None


def symbol_ok(code):
    # 4-digit common stocks; 00-prefixed ETFs (5/6 digit). Exclude warrants and bonds.
    return bool(re.fullmatch(r'\d{4}', code) or re.fullmatch(r'00\d{3,4}', code))


def normalize_quote(row, market):
    code = str(row.get('Code') or row.get('SecuritiesCompanyCode') or row.get('code') or '').strip()
    if not symbol_ok(code):
        return None
    name = str(row.get('Name') or row.get('CompanyName') or row.get('SecuritiesCompanyName') or '').strip()
    if not name:
        return None
    close = num(row.get('ClosingPrice') if market == '上市' else row.get('Close'))
    change = num(row.get('Change'))
    if market == '上市' and change is not None:
        # TWSE daily Change is signed. Some alternative schemas also supply ChangeSign.
        # Never turn a valid signed Change into missing data merely because Sign is absent.
        sign = str(row.get('ChangeSign') or row.get('Sign') or '')
        if '－' in sign or '-' in sign: change = -abs(change)
        elif '＋' in sign or '+' in sign: change = abs(change)
    day = iso_date(row.get('Date'))
    return {
        'code': code, 'name': name, 'market': market,
        'type': 'ETF' if code.startswith('00') else '股票',
        'price': close,
        'change': change,
        'open': num(row.get('OpeningPrice') if market == '上市' else row.get('Open')),
        'high': num(row.get('HighestPrice') if market == '上市' else row.get('High')),
        'low': num(row.get('LowestPrice') if market == '上市' else row.get('Low')),
        'volume': num(row.get('TradeVolume') if market == '上市' else row.get('TradingShares')),
        'date': day,
        'source': 'TWSE 官方盤後' if market == '上市' else 'TPEx 官方盤後',
    }


def universe():
    def loader():
        stocks = {}
        errors = []
        for market, url in MARKETS.items():
            try:
                data = fetch_json(url)
                if not isinstance(data, list):
                    raise DataError('股票行情清單不是陣列')
                for row in data:
                    if not isinstance(row, dict): continue
                    s = normalize_quote(row, market)
                    if s: stocks[s['code']] = s
            except Exception as e:
                errors.append(f'{market}：{str(e)[:140]}')
        # Merge master company lists. Companies without trades still appear in search, with price=null.
        for market, url in REGISTRIES.items():
            try:
                data = fetch_json(url)
                if not isinstance(data, list): raise DataError('公司基本資料格式錯誤')
                for row in data:
                    code = str(row.get('公司代號') or row.get('SecuritiesCompanyCode') or '').strip()
                    name = str(row.get('公司簡稱') or row.get('CompanyAbbreviation') or row.get('公司名稱') or row.get('CompanyName') or '').strip()
                    if not symbol_ok(code) or not name: continue
                    if code not in stocks:
                        stocks[code] = {'code':code,'name':name,'market':market,'type':'股票',
                            'price':None,'change':None,'open':None,'high':None,'low':None,
                            'volume':None,'date':None,'source':'公司名冊（尚無盤後行情）'}
            except Exception as e:
                errors.append(f'{market}公司名冊：{str(e)[:130]}')
        if not stocks:
            raise DataError('上市、上櫃資料皆無法取得：' + ' / '.join(errors), 503)
        return {'stocks': sorted(stocks.values(), key=lambda x: x['code']), 'total': len(stocks),
                'errors': errors, 'updatedAt': datetime.now().astimezone().isoformat(timespec='seconds')}
    return cached('universe', 600, loader)


# These official OpenAPI resources are daily closing snapshots, NOT live index or P/E.
# A missing source is intentionally null instead of a fabricated dashboard value.
def market_summary():
    def loader():
        result = {'index': None, 'turnoverHundredMillion': None,
                  'valuation': {}, 'sources': [], 'errors': []}
        urls = {
            'index': 'https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX',
            'turnover': 'https://openapi.twse.com.tw/v1/exchangeReport/FMTQIK',
            'pe': 'https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL',
        }
        responses = {}
        with ThreadPoolExecutor(max_workers=3) as pool:
            jobs = {pool.submit(fetch_json, url, timeout=12): key for key,url in urls.items()}
            for task in as_completed(jobs):
                key = jobs[task]
                try:
                    data = task.result()
                    if not isinstance(data, list): raise ValueError('不是陣列格式')
                    responses[key] = data
                except Exception as error:
                    result['errors'].append(key + '：' + str(error)[:100])
        for row in responses.get('index', []):
            if not isinstance(row, dict): continue
            name = str(row.get('指數') or row.get('指數/報酬指數') or '').strip()
            if name not in ('發行量加權股價指數', '發行量加權股價指數(臺灣加權指數)'): continue
            level = num(row.get('收盤指數'))
            if level is None or level <= 0: continue
            move = num(row.get('漲跌點數'))
            sign = str(row.get('漲跌') or row.get('漲跌(+/-)') or '').strip()
            if move is not None:
                if '-' in sign or '－' in sign or '跌' in sign: move=-abs(move)
                elif '+' in sign or '＋' in sign or '漲' in sign: move=abs(move)
                elif not (str(row.get('漲跌點數')).strip().startswith(('+','-','－','＋'))): move=None
            day = iso_date(row.get('日期'))
            result['index'] = {
                'level': level, 'change': move,
                'pct': (move / (level - move) * 100) if move is not None and level-move>0 else None,
                'date': day, 'source': 'TWSE 官方加權指數盤後資料', 'quality': 'official_daily_close'
            }
            result['sources'].append('TWSE MI_INDEX')
            break
        turnovers = [row for row in responses.get('turnover', []) if isinstance(row, dict)]
        for row in reversed(turnovers):
            day=iso_date(row.get('Date'))
            if result['index'] and day != result['index']['date']: continue
            value=num(row.get('TradeValue'))
            if value is not None and value>=0:
                result['turnoverHundredMillion'] = round(value/100_000_000,2)
                result['turnoverDate'] = day
                result['sources'].append('TWSE FMTQIK')
                break
        for row in responses.get('pe', []):
            if not isinstance(row, dict): continue
            code=str(row.get('Code') or '').strip()
            if not symbol_ok(code):continue
            val=num(row.get('PEratio'))
            if val is not None and val>0:
                result['valuation'][code] = {'pe': val, 'date': iso_date(row.get('Date')),
                                              'source': 'TWSE 官方本益比（日資料）'}
        if result['valuation']: result['sources'].append('TWSE BWIBBU_ALL')
        return result
    return cached('market-summary', 900, loader)


def finmind_candles(code, days, token=''):
    start = (date.today() - timedelta(days=days+12)).isoformat()
    params = {'dataset':'TaiwanStockPrice', 'data_id':code, 'start_date':start}
    if token: params['token'] = token
    uri = 'https://api.finmindtrade.com/api/v4/data?' + urlencode(params)
    data = fetch_json(uri, timeout=17)
    if not isinstance(data, dict) or not isinstance(data.get('data'), list):
        raise DataError(str(data.get('msg','FinMind 回應無資料')) if isinstance(data, dict) else 'FinMind 格式錯誤')
    if data.get('status') not in (None, 200) and data.get('msg') != 'success':
        raise DataError(str(data.get('msg') or data.get('status')))
    rows = []
    for x in data['data']:
        d = iso_date(x.get('date'))
        o, h, l, c = (num(x.get(k)) for k in ('open','max','min','close'))
        if d and all(v is not None and v > 0 for v in (o,h,l,c)) and l <= min(o,c) <= max(o,c) <= h:
            rows.append({'time': d, 'open': o, 'high': h, 'low': l, 'close': c,
                         'volume': num(x.get('Trading_Volume'))})
    return rows


def twse_month(code, year, month):
    # Official TWSE month-by-month history, format in ROC year.
    uri = 'https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?' + urlencode({
        'response':'json','date':f'{year:04d}{month:02d}01','stockNo':code})
    data = fetch_json(uri)
    rows = []
    for item in (data.get('data') or []):
        if not isinstance(item, list) or len(item) < 7: continue
        d = iso_date(item[0]); o=num(item[3]); h=num(item[4]); l=num(item[5]); c=num(item[6]); v=num(item[1])
        if d and all(x is not None and x > 0 for x in (o,h,l,c)) and l<=min(o,c)<=max(o,c)<=h:
            rows.append({'time':d, 'open':o, 'high':h, 'low':l, 'close':c, 'volume':v})
    return rows


def twse_history(code, months):
    now = date.today()
    outputs = []
    errors = []
    def fetch_month(back):
        value = now.year*12 + now.month - 1 - back
        y, m = divmod(value, 12)
        return cached(('twse-month',code,y,m+1), 3600 if back == 0 else 86400*7,
                      lambda: twse_month(code,y,m+1))
    # Parallelizes monthly endpoints; requests limited to 4 concurrent.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_month,i):i for i in range(months)}
        for future in as_completed(futures):
            try: outputs.extend(future.result())
            except Exception as e: errors.append(str(e)[:130])
    return outputs, errors


def candles(code, window, token=''):
    if not symbol_ok(code): raise DataError('股票代號格式不正確', 400)
    spans = {'5D': 12, '1M': 40, '3M': 108, '6M': 205, '1Y': 390}
    if window not in spans: raise DataError('不支援的時間範圍', 400)
    days = spans[window]
    key = ('candles',code,window, bool(token))
    def loader():
        issues = []
        result = []
        source = None
        market_item = next((x for x in universe()['stocks'] if x['code'] == code), None)
        # Listed history is officially available from TWSE without a FinMind account.
        if market_item and market_item['market'] == '上市':
            result, problems = twse_history(code, min(13, max(1, (days+31)//28)))
            issues.extend(problems[:2])
            if result: source = 'TWSE 官方歷史日 K'
        if not source:
            try:
                result = finmind_candles(code, days, token or FINMIND_TOKEN)
                if len(result) >= 2:
                    source = 'FinMind 日成交歷史'
            except Exception as e:
                issues.append('FinMind：' + str(e)[:130])
        if not source:
            return {'code':code, 'candles':[], 'source':None, 'errors':issues,
                    'message':'無法取得歷史 K 線；上櫃歷史資料可能需要 FinMind 有效存取權限。'}
        unique = {r['time']:r for r in result}
        ordered = sorted(unique.values(),key=lambda x:x['time'])
        ordered = ordered[-({'5D':5,'1M':25,'3M':70,'6M':135,'1Y':265}[window]):]
        return {'code':code,'candles':ordered,'source':source,'errors':issues,
                'asOf':ordered[-1]['time'] if ordered else None}
    return cached(key, 1800, loader)


TAIPEI_TZ = timezone(timedelta(hours=8))

def market_clock():
    now = datetime.now(TAIPEI_TZ)
    mins = now.hour * 60 + now.minute
    return now, now.weekday() < 5 and 540 <= mins <= 810

def intraday_quotes(codes):
    # This endpoint provides a best-effort MIS reference. It is NOT licensed tick-by-tick data.
    codes = list(dict.fromkeys(codes))
    if not 1 <= len(codes) <= 12 or not all(symbol_ok(c) for c in codes):
        raise DataError('僅支援每次 1～12 檔有效股票代號', 400)
    now, is_session = market_clock()
    base = {'quotes': [], 'requested': codes, 'checkedAt': now.isoformat(),
            'status': 'market_closed' if not is_session else 'unavailable',
            'source': 'TWSE MIS 盤中參考（非保證即時）', 'pollIntervalSeconds': 15}
    if not is_session:
        return {**base, 'message': '非平日一般交易時段，使用官方盤後資料'}

    def loader():
        try:
            lookup = {x['code']: x for x in universe()['stocks']}
            channels = [('otc_' if lookup[c]['market'] == '上櫃' else 'tse_') + c + '.tw'
                        for c in codes if c in lookup]
            if not channels:
                return {**base, 'message': '沒有符合條件的股票'}
            url = 'https://mis.twse.com.tw/stock/api/getStockInfo.jsp?' + urlencode({
                'ex_ch': '|'.join(channels), 'json': '1', 'delay': '0'})
            payload = fetch_json(url, timeout=10)
            result = []
            for raw in (payload.get('msgArray') or []):
                code = str(raw.get('c') or '')
                if code not in codes:
                    continue
                try:
                    dt = datetime.strptime(str(raw.get('d')) + ' ' + str(raw.get('t')),
                                           '%Y%m%d %H:%M:%S').replace(tzinfo=TAIPEI_TZ)
                except (ValueError, TypeError):
                    continue
                age = (now - dt).total_seconds()
                value, prev = num(raw.get('z')), num(raw.get('y'))
                if value is None or value <= 0 or age < -10 or age > 120:
                    continue
                result.append({'code': code, 'price': value, 'prevClose': prev,
                               'change': value-prev if prev and prev > 0 else None,
                               'open': num(raw.get('o')), 'high': num(raw.get('h')),
                               'low': num(raw.get('l')), 'asOf': dt.isoformat(),
                               'date': dt.date().isoformat(), 'source': 'TWSE MIS 盤中參考',
                               'quality': 'intraday_reference', 'ageSeconds': round(age)})
            return {**base, 'quotes': result, 'status': 'intraday_reference' if result else 'unavailable',
                    'message': '盤中參考行情' if result else '無足夠新的成交資料，保留官方盤後價格'}
        except (DataError, OSError, KeyError, ValueError):
            return {**base, 'message': '盤中來源暫不可用，保留官方盤後資料'}
    return cached(('intraday', tuple(codes)), 12, loader)

def verify_google_id_token(token):
    if not GOOGLE_CLIENT_ID:
        raise DataError('伺服器未設定 GOOGLE_CLIENT_ID', 503)
    if not token or len(token) > 8192:
        raise DataError('請先使用 Google 帳號登入', 401)
    # For low-volume deployments, validate with Google's tokeninfo endpoint.
    # Production should verify signatures locally using Google's official libraries.
    try:
        info = fetch_json('https://oauth2.googleapis.com/tokeninfo?' + urlencode({'id_token': token}), timeout=10)
    except DataError:
        raise DataError('Google 身分驗證失敗或逾時，請重新登入', 401)
    if (info.get('aud') != GOOGLE_CLIENT_ID or
        str(info.get('email_verified')).lower() != 'true' or
        info.get('iss') not in ('accounts.google.com','https://accounts.google.com') or
        int(info.get('exp',0)) <= time.time()):
        raise DataError('Google 憑證無效或已過期', 401)
    return info

def ask_gemini_json(prompt, system_prompt):
    if not GEMINI_API_KEY:
        raise DataError('伺服器未設定 GEMINI_API_KEY', 503)
    model = os.environ.get('GEMINI_MODEL','gemini-3.8-flash').strip()
    if not re.fullmatch(r'gemini-[a-z0-9.\-]+', model):
        raise DataError('GEMINI_MODEL 格式錯誤', 500)
    body = json.dumps({'contents':[{'role':'user','parts':[{'text':prompt}]}],
                       'systemInstruction':{'parts':[{'text':system_prompt}]},
                       'generationConfig':{'temperature':0.25,'responseMimeType':'application/json'}},
                      ensure_ascii=False).encode('utf-8')
    last_error = None
    for attempt in range(2):
        url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent'
        request = Request(url, data=body, method='POST', headers={
            'Content-Type':'application/json','x-goog-api-key':GEMINI_API_KEY})
        try:
            with urlopen(request, timeout=(48 if attempt == 0 else 30)) as response:
                data = json.loads(response.read(2_000_000).decode('utf-8'))
            candidates = data.get('candidates', [])
            parts = candidates[0].get('content',{}).get('parts',[]) if candidates else []
            content = '\n'.join(str(x.get('text','')) for x in parts).strip()
            if not content:
                raise DataError('AI 未回傳內容，請縮小分析範圍', 502)
            content = re.sub(r'^```(?:json)?\s*|\s*```$', '', content, flags=re.I).strip()
            result = json.loads(content)
            if not isinstance(result, dict):
                raise DataError('AI 回傳格式不是 JSON 物件',502)
            return result
        except HTTPError as e:
            try:
                detail = json.loads(e.read(4096).decode('utf-8','replace')).get('error',{}).get('message','API 拒絕請求')
            except (ValueError, KeyError, AttributeError):
                detail = 'API 拒絕請求'
            last_error = DataError(f'Gemini HTTP {e.code}：{str(detail)[:170]}', 429 if e.code==429 else 502)
            if e.code in (429,500,502,503,504) and attempt==0:
                time.sleep(1.5)
                continue
            raise last_error
        except (TimeoutError, URLError, socket.timeout, OSError):
            last_error = DataError('Gemini API 連線逾時，請重試或選用回應較快的模型', 504)
            if attempt == 0:
                time.sleep(1)
                continue
            raise last_error
        except (ValueError, UnicodeError):
            raise DataError('Gemini 回傳不是有效 JSON，請重試',502)
    raise last_error or DataError('Gemini 生成失敗', 502)

class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(BASE), **kw)

    def log_message(self, format, *args):
        # Do not log secrets. URLs contain only code/window; token is POST/body or header.
        print('%s - %s' % (self.address_string(), (format % args).split('?')[0]), flush=True)

    def _cors(self):
        origin = self.headers.get('Origin','').rstrip('/')
        if origin in ALLOWED_ORIGINS:
            self.send_header('Access-Control-Allow-Origin', origin)
            self.send_header('Vary', 'Origin')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')

    def do_OPTIONS(self):
        allowed = self.headers.get('Origin','').rstrip('/') in ALLOWED_ORIGINS
        self.send_response(204 if allowed else 403)
        self._cors()
        self.send_header('Content-Length','0')
        self.end_headers()

    def json_reply(self, code, payload):
        data = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        self.send_response(code)
        self._cors()
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Content-Length',str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlsplit(self.path)
        if parsed.path == '/api/quotes':
            args = parse_qs(parsed.query)
            codes = [c.strip() for c in (args.get('codes') or [''])[0].split(',') if c.strip()]
            try: return self.json_reply(200, intraday_quotes(codes))
            except DataError as e: return self.json_reply(e.status, {'error':str(e)})
        if parsed.path == '/api/summary':
            try: return self.json_reply(200, market_summary())
            except DataError as error: return self.json_reply(error.status, {'error': str(error)})
            except Exception: return self.json_reply(502, {'error': '大盤統計資料暫不可用'})
        if parsed.path == '/api/health':
            return self.json_reply(200, {'ok':True, 'backend':'TWSE Pro', 'time':datetime.now().isoformat(timespec='seconds')})
        if parsed.path == '/api/stocks':
            try: return self.json_reply(200, universe())
            except DataError as e: return self.json_reply(e.status, {'error':str(e)})
            except Exception as e: return self.json_reply(502, {'error':str(e)})
        if parsed.path == '/api/candles':
            args = parse_qs(parsed.query)
            code = (args.get('code') or [''])[0].strip()
            window = (args.get('range') or ['3M'])[0]
            try: return self.json_reply(200, candles(code, window))
            except DataError as e: return self.json_reply(e.status, {'error':str(e)})
            except Exception as e: return self.json_reply(502, {'error':str(e)})
        if parsed.path == '/':
            self.path = '/index.html'
        return super().do_GET()

    def do_POST(self):
        parsed = urlsplit(self.path)
        if parsed.path == '/api/ai/generate':
            try:
                size = int(self.headers.get('Content-Length','0'))
                if size < 2 or size > 16000: return self.json_reply(400, {'error':'AI 內容過長或格式錯誤'})
                credential = self.headers.get('Authorization','')
                if not credential.startswith('Bearer '): return self.json_reply(401, {'error':'請先使用 Google 登入'})
                user = verify_google_id_token(credential[7:].strip())
                email = str(user.get('email','')).lower()
                if email not in AI_ALLOWED_EMAILS: return self.json_reply(403, {'error':'AI 生成目前僅開放管理員'})
                with AI_LOCK:
                    now = time.time()
                    if now - AI_LAST_CALL.get(email,0) < 20:
                        return self.json_reply(429, {'error':'AI 請求過於頻繁，請稍候再試'})
                    AI_LAST_CALL[email] = now
                payload = json.loads(self.rfile.read(size))
                prompt, system = payload.get('prompt',''), payload.get('systemPrompt','')
                if not isinstance(prompt,str) or not isinstance(system,str) or not (0<len(prompt)<=11000) or not (0<len(system)<=4000):
                    return self.json_reply(400,{'error':'AI 提示文字格式錯誤'})
                return self.json_reply(200,{'result':ask_gemini_json(prompt,system)})
            except DataError as e: return self.json_reply(e.status, {'error':str(e)})
            except (ValueError,TypeError): return self.json_reply(400,{'error':'JSON 格式錯誤'})
            except Exception: return self.json_reply(502,{'error':'AI 後端發生未預期的錯誤'})
        if parsed.path != '/api/candles':
            return self.json_reply(404, {'error':'未知端點'})
        try:
            size = int(self.headers.get('Content-Length','0'))
            if size <= 0 or size > 8192: return self.json_reply(400, {'error':'請求格式錯誤'})
            payload = json.loads(self.rfile.read(size))
            code = str(payload.get('code','')).strip()
            window = str(payload.get('range','3M'))
            token = ''  # Secret must be read from Render env, never from the public client.
            if len(token) > 2048: return self.json_reply(400, {'error':'Token 過長'})
            # Token is never logged or persisted; cache only by presence, so don't cache authenticated results.
            if token:
                obj = candles_no_cache(code, window, token)
            else:
                obj = candles(code, window)
            return self.json_reply(200, obj)
        except DataError as e: return self.json_reply(e.status, {'error':str(e)})
        except Exception as e: return self.json_reply(502, {'error':str(e)})

def candles_no_cache(code, window, token):
    if not symbol_ok(code) or window not in ('5D','1M','3M','6M','1Y'):
        raise DataError('代號或時間範圍錯誤',400)
    spans = {'5D':12, '1M':40, '3M':108, '6M':205, '1Y':390}
    try:
        rows = finmind_candles(code,spans[window],token)
        if rows:
            rows = sorted({r['time']:r for r in rows}.values(),key=lambda r:r['time'])
            return {'code':code,'candles':rows[-({'5D':5,'1M':25,'3M':70,'6M':135,'1Y':265}[window]):], 'source':'FinMind 日成交歷史', 'asOf':rows[-1]['time']}
    except Exception:
        pass
    return candles(code,window)

if __name__ == '__main__':
    print(f'\nTWSE Pro 資料服務：http://{HOST}:{PORT}/')
    print('資料來源：TWSE / TPEx 官方公開資料；FinMind 歷史日 K 可選擇使用既有 Token')
    print('停止：Ctrl + C\n')
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
