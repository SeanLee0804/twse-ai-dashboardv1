/* TWSE Pro 全台股增強模組：官方股票清單 / 真實歷史日K / 本機 API。 */
(() => {
'use strict';
const $ = id => document.getElementById(id);
const safeText = v => String(v ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const number = (v, digits = 2) => v == null || !Number.isFinite(Number(v)) ? '—' : Number(v).toLocaleString('zh-TW',{maximumFractionDigits:digits,minimumFractionDigits:digits});
const API_BASE = String(window.TWSE_PUBLIC_CONFIG?.apiBaseUrl || '').trim().replace(/\/$/, '');
const apiUrl = path => API_BASE ? API_BASE + path : path;
const LIVE_REFRESH_MS = 15000;
let liveTimer = null;
const lastQuoteDates = new Map();
const latestIntradayQuotes = new Map();
const summaryState = { index: null, valuation: {}, lastRefresh: null, loading: false };
let quoteFetchBusy = false;
const pinned = { time: null, close: null };
const K_RANGES = {'1D':'5D','5D':'5D','1M':'1M','3M':'3M','6M':'6M','1Y':'1Y'};
const model = { market: [], map: new Map(), marketStatus: '載入中', query:'', kind:'ALL', page:1, pageSize:40,
   sort:'code', chart:null, candleSeries:null, volSeries:null, ma5:null, ma20:null,
   candleCache:new Map(), activeSeq:0, lastCandles:[], selectedRange:'3M', showMA:true, showVolume:true,
   chartObserver:null, loadingMarket:null, loadingChart:false, status:'', backendOk:false };
const localFetch = async (url, options={}) => {
   const ctl=new AbortController(); const t=setTimeout(()=>ctl.abort(),35000);
   try {
     const r=await fetch(apiUrl(url),{cache:'no-store',signal:ctl.signal,...options});
     let json; try{json=await r.json();}catch(_){ throw new Error('資料來源未回傳 JSON'); }
     if(!r.ok) throw new Error(json.error || `HTTP ${r.status}`);
     return json;
   } finally { clearTimeout(t); }
};
const FALLBACK = ['2308','2330','2454','3017','2317','0050'];
function isIntraday(item){return item?.quality === 'intraday_reference' && Boolean(item?.asOf);}
function asOfText(item){
 if(!item)return '來源未同步';
 if(!(item.price>0))return '尚無可用成交價格 · '+(item.date||'日期未知');
 const label=isIntraday(item)?'盤中參考':'官方盤後';
 const date=item.date||'日期未提供';
 const time=isIntraday(item)?' '+item.asOf.slice(11,16):'';
 return `${label} · ${date}${time} · ${item.source||'TWSE／TPEx'}`;
}
function textInto(id,val){const el=$(id);if(el)el.textContent=val;}
function mostRecentDay(){return model.market.map(x=>x.date).filter(Boolean).sort().at(-1)||null;}
function renderMarketSummary(){
 const x=summaryState.index;
 textInto('taiex-price',x?.level>0?number(x.level):'—');
 textInto('taiex-change',x?.change==null?'漲跌：—':`${x.change>0?'▲':x.change<0?'▼':'—'} ${number(Math.abs(x.change))}（${x.change>0?'+':''}${x.pct?.toFixed(2)??'—'}%）`);
 const rise=x?.change>0,fall=x?.change<0;
 const color=rise?'text-red-400':fall?'text-emerald-400':'text-slate-300';
 for(const id of ['taiex-price','taiex-change']){const el=$(id);if(el){el.classList.remove('text-emerald-400','text-red-400','text-slate-300');el.classList.add(color);}}
 textInto('taiex-vol',summaryState.turnoverHundredMillion==null?'—':`${number(summaryState.turnoverHundredMillion)} 億`);
 textInto('taiex-date',x?.date?`官方盤後 · ${x.date}`:'官方資料未同步');
 textInto('taiex-ma120','—');
}
async function loadMarketSummary(){
 if(summaryState.loading)return;
 summaryState.loading=true;
 try {
   const data=await localFetch('/api/summary');
   summaryState.index=data.index||null;
   summaryState.valuation=data.valuation||{};
   summaryState.turnoverHundredMillion=data.turnoverHundredMillion??null;
   summaryState.lastRefresh=Date.now();
   renderMarketSummary();refreshFocusCards();updateSelectedDetails(currentStockCode);
 }catch(err){renderMarketSummary();console.warn('大盤摘要尚無資料',err.message)}
 finally{summaryState.loading=false;}
}
function focusPriceTooltip(code){
 const item=model.map.get(code);return item?asOfText(item):'尚未取得真實行情';
}

function makeEmpty(code, name, market='未知', type='股票') {
 const existing=STOCK_DB[code];
 if(existing) { existing.market=market; existing.stockType=type; return existing; }
 const s={code,name,category:market,price:0,prevClose:0,open:0,high:0,low:0,volume:0,
    market,stockType:type,foreignPct:0,foreignNetToday:0,trustNetToday:0,dealerNetToday:0,
    foreignShares:0,foreign20dDiff:0,foreignNet5d:0,foreignNet20d:0,trustNet5d:0,trustNet20d:0,
    whale400Pct:0,marginUsage:0,marginChange:0,shortBalance:0,trustPct:0,
    isLimitDown:false,isLimitUp:false,dataSource:'尚未取得官方行情',
    desc:'資料以 TWSE／TPEx 官方公告為準',chipStreak:'待取得法人資料',yield:'—',
    fairRange:'—',targetPrice:0,supportText:'—',eps26:0,eps27:0,
    history1D:[],history5D:[],history1M:[],history1Y:[],foreignHist:[],
    thesis:'本系統不提供未經驗證的個股基本面預估。',stagePrices:[0,0,0],rating:'觀察',recType:'NONE'};
 STOCK_DB[code]=s; return s;
}
function ingest(row){
 const item={...row,code:String(row.code||'').trim(),name:String(row.name||'').trim()};
 if(!item.code || !item.name)return;
 // Keep a *newer dated* intraday transaction instead of replacing it with yesterday's
 // official daily snapshot during the exchange's post-close publication gap.
 const latest=latestIntradayQuotes.get(item.code);
 if(latest?.price>0 && latest?.date && (!item.date || latest.date>item.date)){
   Object.assign(item,{price:latest.price,prevClose:latest.prevClose,change:latest.change,
     open:latest.open??null,high:latest.high??null,low:latest.low??null,volume:null,
     date:latest.date,source:latest.source,quality:latest.quality,asOf:latest.asOf});
 }
 model.map.set(item.code,item);
 const s=makeEmpty(item.code,item.name,item.market,item.type);
 s.name=item.name; s.market=item.market; s.stockType=item.type;
 if(Number(item.price)>0){
   s.price=Number(item.price);
   s.open=Number(item.open)||0; s.high=Number(item.high)||0; s.low=Number(item.low)||0;
   s.volume= Number(item.volume)>0 ? Math.round(Number(item.volume)/1000):0;
   if(Number.isFinite(Number(item.change)) && item.change !==null) s.prevClose=s.price-Number(item.change);
   else s.prevClose=0;
   s.dataSource=item.source || '官方公開資料';
 }
}
async function loadUniverse(refresh=false){
 if(model.loadingMarket) return model.loadingMarket;
 model.loadingMarket=(async()=>{
   const info=$('market-catalog-status'); if(info) info.textContent='正在取得 TWSE + TPEx 全市場名單…';
   try{
     const json=await localFetch('/api/stocks');
     if(!Array.isArray(json.stocks)||!json.stocks.length) throw new Error('股票清單是空的');
     model.market=json.stocks;
     model.map.clear();
     for(const item of json.stocks) ingest(item);
     model.backendOk=true;
     model.marketStatus=`已載入 ${json.total.toLocaleString('zh-TW')} 檔上市／上櫃證券`;
     if(json.errors?.length) model.marketStatus+=`（部分市場資料失敗：${json.errors.join('、')}）`;
     if(info)info.textContent=model.marketStatus;
     const c=$('market-total-count');if(c)c.textContent=json.total.toLocaleString('zh-TW');
     updateFinMindStatusUI(!json.errors?.length, json.errors?.length?'部分市場同步：官方快照':'TWSE / TPEx 官方行情快照');
     // 表格、側邊自選、圖表與搜尋聯動
     model.page=1; renderEnhancedTable(); renderWatchlistPage(); renderTickerTapeAndPills();
     refreshFocusCards();
     if(model.map.has(currentStockCode)) updateSelectedDetails(currentStockCode);
     const newest = mostRecentDay();
     quoteStatus(`官方最近交易日 ${newest||'日期未提供'} · 盤中僅追蹤焦點與自選股（最多12檔）`, 'text-slate-400');
     void loadMarketSummary();
     return true;
   }catch(err){
     model.backendOk=false;
     model.marketStatus=`全市場名單載入失敗：${err.message}`;
     if(info) info.textContent=model.marketStatus + '。請用 start.bat 啟動網站，勿使用 py -m http.server。';
     updateFinMindStatusUI(false,'股票資料服務未連線');
     renderEnhancedTable();
     return false;
   } finally {model.loadingMarket=null;}
 })();
 return model.loadingMarket;
}
window.syncAllRealFinMindData=async function(notify=false){
 const ok=await loadUniverse(true);
 if(notify)showToast(ok?'已更新 TWSE／TPEx 官方盤後行情名單':'資料更新失敗；請查看「全市場」狀態提示');
 return ok;
};
window.fetchRealStockDataFromFinMind=async function(code){
 const item=model.map.get(String(code));if(!item)throw new Error(`${code} 未出現在最新名單`);
 ingest(item);return true;
};
function quoteStatus(message, tone='text-slate-400') {
  const el=$('quote-sync-status');
  if(!el)return;
  el.className='text-xs '+tone;
  el.textContent=message;
}
function getQuoteCodes(){
  const ids=[currentStockCode,...FALLBACK,...(userState?.starredCodes||[])];
  return [...new Set(ids.filter(x=>/^(?:\d{4}|00\d{3,4})$/.test(String(x))))].slice(0,12);
}
async function refreshIntradayQuotes(){
  if(!model.backendOk || document.hidden || !navigator.onLine || quoteFetchBusy)return;
  quoteFetchBusy=true;
  const codes=getQuoteCodes();if(!codes.length)return;
  try{
    const result=await localFetch('/api/quotes?codes='+encodeURIComponent(codes.join(',')));
    if(result.status==='market_closed'){
      quoteStatus(`非盤中 · 最近官方盤後 ${mostRecentDay()||'日期未知'}（不是即時報價）`,'text-slate-400');return;
    }
    if(result.status!=='intraday_reference' || !result.quotes?.length){
      quoteStatus(result.message || '無新盤中行情，保留官方盤後資料','text-amber-300');return;
    }
    let changed=false;
    for(const q of result.quotes){
      const prev=lastQuoteDates.get(q.code);
      if(prev && q.asOf<=prev)continue;
      if(!model.map.has(q.code) || !(Number(q.price)>0))continue;
      const v=model.map.get(q.code);
      v.price=Number(q.price);v.prevClose=q.prevClose;
      v.change=q.change;v.source=q.source;v.asOf=q.asOf;
      v.date=q.date;v.quality=q.quality;
      v.open=q.open??null;v.high=q.high??null;v.low=q.low??null;v.volume=null;
      latestIntradayQuotes.set(q.code,{...q});
      const stock=STOCK_DB[q.code];
      if(stock){stock.price=v.price; stock.prevClose=q.prevClose||0; stock.dataSource=q.source;}
      lastQuoteDates.set(q.code,q.asOf);
      changed=true;
    }
    if(changed){
      refreshFocusCards();renderTickerTapeAndPills();renderEnhancedTable();renderWatchlistPage();
      updateSelectedDetails(currentStockCode);
    }
    const mostRecent=result.quotes.map(q=>q.asOf).filter(Boolean).sort().at(-1);
    quoteStatus('盤中參考（非逐筆即時）· '+(mostRecent||'時間未知')+' · 15秒輪詢','text-cyan-300');
  }catch(err){
    quoteStatus('盤中資料暫無法更新：'+err.message+'；保留最近官方盤後價格','text-amber-300');
  }finally{quoteFetchBusy=false;}
}
function startQuotePolling(){
  if(liveTimer)clearInterval(liveTimer);
  liveTimer=setInterval(()=>{
    void refreshIntradayQuotes();
    if(!summaryState.lastRefresh || Date.now()-summaryState.lastRefresh>15*60*1000) void loadMarketSummary();
  },LIVE_REFRESH_MS);
  document.addEventListener('visibilitychange',()=>{if(!document.hidden)refreshIntradayQuotes();});
  refreshIntradayQuotes();
}
function refreshFocusCards(){
 for(const code of ['2308','2330']){
   const item=model.map.get(code);
   const price=$('card-'+code+'-price');
   if(price){price.textContent=item?.price>0?number(item.price):'—';price.title=focusPriceTooltip(code);
     price.classList.remove('text-red-400','text-emerald-400','text-slate-300');
     price.classList.add(item?.change>0?'text-red-400':item?.change<0?'text-emerald-400':'text-slate-300');}
   const change=$('card-'+code+'-change');
   if(change)change.textContent=item?.change==null?'—':`${item.change>0?'▲':item.change<0?'▼':'—'} ${number(Math.abs(item.change))}`;
   const pct=$('card-'+code+'-pct');
   if(pct){const prev=item?.price>0&&item?.change!=null?item.price-item.change:null;
       pct.textContent=prev>0?`${item.change>0?'+':''}${(item.change/prev*100).toFixed(2)}%`:'—';
       pct.classList.remove('bg-emerald-500','bg-red-500','bg-slate-700','text-slate-950','text-white');
       pct.classList.add(prev>0?(item.change>0?'bg-red-500':item.change<0?'bg-emerald-500':'bg-slate-700'):'bg-slate-700', prev>0&&item.change!==0?'text-slate-950':'text-white');
   }
   textInto('card-'+code+'-fpct','—');
   textInto('card-'+code+'-inst','法人資料尚未同步');
   const pe=summaryState.valuation[code];
   textInto('card-'+code+'-pe',pe?.pe>0?`${number(pe.pe)} 倍`:'—');
   const pel=$('card-'+code+'-pe');if(pel)pel.title=pe?.date?`${pe.source} · ${pe.date}`:'本益比尚未同步';
   textInto('card-'+code+'-asof',item?asOfText(item):'行情未同步');
 }
 for(const code of ['2330','2454','2308','2317']){
   const item=model.map.get(code);
   const el=$('spotlight-'+code+'-price');
   if(!el)continue;
   el.textContent=item?.price>0?`${number(item.price)} 元`:'—';
   el.title=item?asOfText(item):'尚未同步行情';
   el.classList.remove('text-red-400','text-emerald-400','text-slate-300');
   el.classList.add(item?.change>0?'text-red-400':item?.change<0?'text-emerald-400':'text-slate-300');
 }
}
function getFiltered(){
 const q=model.query.trim().toLowerCase();
 let rows=model.market.filter(x=> (!q || x.code.toLowerCase().includes(q) || x.name.toLowerCase().includes(q))
    && (model.kind==='ALL'|| (model.kind==='STARRED' ? isStarred(x.code): model.kind==='ETF'? x.type==='ETF': x.market===model.kind && x.type!=='ETF')));
 if(model.sort==='name')rows=rows.slice().sort((a,b)=>a.name.localeCompare(b.name,'zh-Hant'));
 if(model.sort==='price')rows=rows.slice().sort((a,b)=>(b.price??-1)-(a.price??-1));
 return rows;
}
function renderEnhancedTable(){
 const box=$('dashboard-market-tbody');if(!box)return;
 const rows=getFiltered(), count=rows.length;
 const pages=Math.max(1,Math.ceil(count/model.pageSize));model.page=Math.min(model.page,pages);
 const current=rows.slice((model.page-1)*model.pageSize,model.page*model.pageSize);
 const meta=$('market-page-indicator');if(meta)meta.textContent=`${count.toLocaleString()} 檔 · 第 ${model.page}/${pages} 頁`;
 const older=$('market-prev');if(older)older.disabled=model.page<=1;
 const newer=$('market-next');if(newer)newer.disabled=model.page>=pages;
 const loaded=model.market.length>0;
 if(!loaded){ box.innerHTML=`<tr><td colspan="10" class="py-8 text-center text-amber-300">${safeText(model.marketStatus)} — 若為離線環境，不會顯示示範股價。</td></tr>`;return; }
 box.innerHTML=current.map(x=>{
   const diff=x.change, prev= Number(x.price)>0 && diff!==null ? x.price-diff: null;
   const pct=diff!==null && prev>0 ? `${(diff/prev*100).toFixed(2)}%`:'—';
   const cls=diff===null?'text-slate-400':diff>0?'text-red-400':diff<0?'text-emerald-400':'text-slate-300';
   return `<tr data-stock="${safeText(x.code)}" class="hover:bg-slate-800/60 cursor-pointer transition border-b border-slate-800/50">
     <td class="py-3 px-3 text-center"><button data-star="${safeText(x.code)}" title="切換自選" class="text-amber-400">${isStarred(x.code)?'★':'☆'}</button></td>
     <td class="py-3 px-3"><strong>${safeText(x.code)}</strong> <span class="text-slate-200">${safeText(x.name)}</span><div class="text-[10px] text-slate-500">${safeText(x.market)} · ${safeText(x.type)}</div></td>
     <td class="py-3 px-3 text-right num-mono font-bold ${cls}">${number(x.price)}</td>
     <td class="py-3 px-3 text-right num-mono ${cls}">${pct}</td>
     <td class="py-3 px-3 text-right text-slate-500">—</td><td class="py-3 px-3 text-right text-slate-500">—</td><td class="py-3 px-3 text-right text-slate-500">—</td>
     <td class="py-3 px-3 text-right text-[10px] text-slate-400">${safeText(asOfText(x))}</td>
     <td class="py-3 px-3 text-right text-slate-500">—</td>
     <td class="py-3 px-3 text-center text-indigo-400 whitespace-nowrap">開啟 K 線 →</td>
   </tr>`;
 }).join('') || '<tr><td colspan="10" class="py-7 text-center text-slate-400">沒有符合條件的股票</td></tr>';
}
// All six focus ticker values are sourced from the same market map as the detail page.
window.renderTickerTapeAndPills=function(){
  const tape=$('ticker-tape');
  if(tape)tape.innerHTML=FALLBACK.map(code=>{
    const item=model.map.get(code);
    const name=item?.name||STOCK_DB[code]?.name||code;
    const value=item?.price>0?number(item.price):'—';
    const cls=item?.change>0?'text-red-400':item?.change<0?'text-emerald-400':'text-slate-300';
    const label=focusPriceTooltip(code);
    return `<span title="${safeText(label)}" class="inline-flex items-center gap-1.5 px-2 py-1 rounded bg-slate-900 border border-slate-800"><span class="font-bold text-slate-200">${safeText(code)} ${safeText(name)}</span><span class="num-mono font-bold ${cls}">${value}</span></span>`;
  }).join('');
  const quick=$('quick-stock-pills');
  if(quick)quick.innerHTML=FALLBACK.map(code=>`<button onclick="selectStock('${code}',true)" class="px-3 py-1.5 rounded-xl text-xs font-semibold ${code===currentStockCode?'bg-indigo-600 text-white':'bg-slate-900 text-slate-300'}">${safeText(code)} ${safeText(model.map.get(code)?.name||STOCK_DB[code]?.name||'')}</button>`).join('');
};
window.renderDashboardMarketTable=renderEnhancedTable;
window.filterMarketTable=function(kind){model.kind=kind==='STARRED'?'STARRED':kind==='HIGH_FOREIGN'?'ALL':kind==='TRUST_BUY'?'ALL':kind;model.page=1;renderEnhancedTable();};
function searchResults(q){
 const query=q.trim().toLowerCase();
 if(!query || !model.market.length)return [];
 const matched=model.market.filter(x=>x.code.startsWith(query)||x.name.toLowerCase().startsWith(query));
 if(matched.length<12)matched.push(...model.market.filter(x=>!(x.code.startsWith(query)||x.name.toLowerCase().startsWith(query)) && (x.code.includes(query)||x.name.toLowerCase().includes(query))).slice(0,12-matched.length));
 return matched.slice(0,12);
}
function showDropdown(){
 const input=$('stock-search-input'),dd=$('search-dropdown');if(!input||!dd)return;
 const rows=searchResults(input.value);
 dd.classList.toggle('hidden',!rows.length);
 dd.innerHTML=rows.map(x=>`<button type="button" data-result="${safeText(x.code)}" class="block w-full text-left px-4 py-2.5 hover:bg-slate-800 text-xs border-b border-slate-800">
 <span class="font-bold text-indigo-300">${safeText(x.code)}</span> <span class="text-white">${safeText(x.name)}</span> <span class="float-right text-slate-500">${safeText(x.market)}</span></button>`).join('');
}
window.setupSearchEvents=function(){
 const input=$('stock-search-input');if(!input)return;
 input.addEventListener('input',showDropdown);
 input.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();handleSearchSubmit();}if(e.key==='Escape')$('search-dropdown')?.classList.add('hidden');});
 $('search-dropdown')?.addEventListener('click',e=>{const button=e.target.closest('[data-result]');if(!button)return;selectStock(button.dataset.result,true);$('search-dropdown').classList.add('hidden');});
 document.addEventListener('click',e=>{if(!e.target.closest('#stock-search-input')&&!e.target.closest('#search-dropdown'))$('search-dropdown')?.classList.add('hidden');});
};
window.handleSearchSubmit=function(){
 const q=$('stock-search-input')?.value.trim();if(!q)return;
 const results=searchResults(q), match=model.map.get(q)||results[0];
 if(!match){showToast('找不到這檔股票，請確認代號／名稱及市場清單已同步');return;}
 selectStock(match.code,true);$('search-dropdown')?.classList.add('hidden');
};
function updateSelectedDetails(code){
 const item=model.map.get(code); const stock=STOCK_DB[code];if(!stock)return;
 $('detail-code').textContent=code;
 $('detail-name').textContent=item?.name || stock.name;
 $('detail-category').textContent=`${item?.market||'資料待確認'} · ${item?.type||'股票'}`;
 $('detail-price').textContent=number(item?.price);
 $('detail-price').className='num-mono text-3xl sm:text-4xl font-black '+(item?.change>0?'text-red-400':item?.change<0?'text-emerald-400':'text-slate-200');
 $('detail-change').textContent=item?.change==null?'漲跌：—':`${item.change>0?'▲':item.change<0?'▼':'—'} ${number(Math.abs(item.change))}（${isIntraday(item)?'盤中參考':'盤後'}）`;
 $('detail-desc').textContent=item?`${asOfText(item)}；${isIntraday(item)?'非授權逐筆串流，可能延遲':'非盤中即時報價'}`:'尚未同步官方資料；未顯示示範股價';
 $('detail-api-badge').textContent=item?.source||'尚無官方行情';
 $('detail-limit-badge')?.classList.add('hidden');
 const data={'detail-prev':item?.price != null&&item?.change!=null?item.price-item.change:null,
 'detail-open':item?.open,'detail-hl':item?.high!=null&&item?.low!=null?`${number(item.high)} / ${number(item.low)}`:null,
 'detail-vol':!isIntraday(item)&&item?.volume!=null?item.volume/1000:null};
 for(const [id,v]of Object.entries(data))$(id).textContent=typeof v==='string'?v:number(v,id==='detail-vol'?0:2);
 for(const id of ['header-foreign-pct','detail-foreign-pct','detail-foreign-net','detail-trust-net','detail-whale-pct','detail-pe','detail-support','chip-foreign-pct-big','chip-foreign-shares','chip-foreign-20d-diff']){
   const el=$(id);if(el)el.textContent='—';
 }
 const pe=summaryState.valuation[code];
 if(pe?.pe>0)textInto('detail-pe',`${number(pe.pe)} 倍（${pe.date||'日期未提供'}）`);
 textInto('detail-quote-asof',item?asOfText(item):'最新行情尚未同步');
 textInto('candle-live-quote',item?.price>0?`當前可得價 ${number(item.price)} · ${isIntraday(item)?'盤中參考':'最近收盤'} · ${item.asOf||item.date||'—'}`:'尚無報價');
 $('chip-streak-badge') && ($('chip-streak-badge').textContent='此股法人資料尚未驗證');
 $('nav-current-stock-badge')&&($('nav-current-stock-badge').textContent=code);
 updateTerminalStarButton();
}
window.selectStock=function(code, goPage=false){
 const clean=String(code||'').trim();
 if(!model.map.has(clean) && !STOCK_DB[clean]){showToast('查無股票代號：'+clean);return;}
 currentStockCode=clean;
 updateSelectedDetails(clean);
 if(model.backendOk)void refreshIntradayQuotes();
 if(goPage) switchPage('terminal');
 else if(currentPage==='terminal') loadCandles();
};
window.openStockInTerminal=function(code){selectStock(code,true);};
const originalSwitchPage=window.switchPage;
window.switchPage=function(name){
 originalSwitchPage(name);
 if(name==='terminal'){
   requestAnimationFrame(()=>{setTimeout(()=>{resizeChart();},55);});
 }
};
window.switchTimeframe=function(tf){
 if(!K_RANGES[tf])return;
 model.selectedRange=K_RANGES[tf];
 const names=['1D','5D','1M','3M','6M','1Y'];
 for(const s of names){const b=$('tf-'+s);if(!b)continue;
 b.classList.toggle('bg-indigo-600', s===tf);b.classList.toggle('text-white',s===tf);
 b.classList.toggle('text-slate-400',s!==tf);}
 loadCandles();
};
window.toggleChartOverlay=function(type){
 if(type==='MA')model.showMA=!model.showMA;
 else model.showVolume=!model.showVolume;
 $('overlay-btn-MA')?.classList.toggle('opacity-40',!model.showMA);
 $('overlay-btn-VOLUME')?.classList.toggle('opacity-40',!model.showVolume);
 drawChart();
};
function setChartStatus(text,isError=false){
 const el=$('candle-status');if(el){el.textContent=text;el.className='text-xs '+(isError?'text-amber-300':'text-slate-400');}
}
async function loadCandles(){
 const code=currentStockCode, range=model.selectedRange, seq=++model.activeSeq;
 if(currentPage!=='terminal')return;
 const key=code+':'+range;
 if(model.candleCache.has(key)){
    const value=model.candleCache.get(key);model.lastCandles=value.candles;model.status=value.source||'';
    drawChart();setChartStatus(`${code} · ${value.source||'無資料'} · 更新至 ${value.asOf||'—'} · 非即時`);return;
 }
 model.lastCandles=[];drawChart();setChartStatus(`正在取得 ${code} 的 ${range} 歷史日 K 線…`);
 try{
   const body={code,range}; // Never transmit browser-held FinMind secrets to a public API.
   const result=await localFetch('/api/candles',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
   if(seq!==model.activeSeq)return;
   if(!result.candles?.length){model.lastCandles=[];drawChart();setChartStatus(result.message||`無法取得 ${code} 的 K 線；請確認 API 額度。`,true);return;}
   model.candleCache.set(key,result);
   model.lastCandles=result.candles;model.status=result.source;
   setChartStatus(`${code} · ${result.source} · 更新至 ${result.asOf||result.candles.at(-1).time} · 日 K、非盤中即時`);
   drawChart();
 }catch(err){
   if(seq!==model.activeSeq)return;
   model.lastCandles=[];drawChart();setChartStatus(`K 線載入失敗：${err.message}。請確認 server.py 已啟動。`,true);
 }
}
function ensureChart(){
 const el=$('stockKChart');if(!el)return false;
 if(!window.LightweightCharts){setChartStatus('K 線圖函式庫載入失敗，請確認網際網路可連到 CDN。',true);return false;}
 if(model.chart)return true;
 const l=window.LightweightCharts;
 model.chart=l.createChart(el,{width:Math.max(300,el.clientWidth),height:Math.max(300,el.clientHeight),
   layout:{background:{type:l.ColorType.Solid,color:'#0b1220'},textColor:'#94a3b8',attributionLogo:false},
   grid:{vertLines:{color:'rgba(51,65,85,.25)'},horzLines:{color:'rgba(51,65,85,.25)'}},
   crosshair:{mode:l.CrosshairMode.Normal},
   rightPriceScale:{borderColor:'#334155'},timeScale:{borderColor:'#334155',timeVisible:false,rightOffset:5},
   localization:{locale:'zh-TW'},handleScroll:true,handleScale:true});
 model.candleSeries=model.chart.addSeries(l.CandlestickSeries,{upColor:'#ef4444',downColor:'#10b981',borderUpColor:'#ef4444',borderDownColor:'#10b981',wickUpColor:'#ef4444',wickDownColor:'#10b981',priceFormat:{type:'price',precision:2,minMove:.01}});
 model.ma5=model.chart.addSeries(l.LineSeries,{color:'#fbbf24',lineWidth:1.4,lastValueVisible:false,priceLineVisible:false});
 model.ma20=model.chart.addSeries(l.LineSeries,{color:'#38bdf8',lineWidth:1.4,lastValueVisible:false,priceLineVisible:false});
 model.volSeries=model.chart.addSeries(l.HistogramSeries,{priceFormat:{type:'volume'},priceScaleId:'',lastValueVisible:false,priceLineVisible:false});
 model.volSeries.priceScale().applyOptions({scaleMargins:{top:.78,bottom:0}});
 function showCandleInfo(time, point, locked=false){
   const x=$('candle-ohlc'), panel=$('candle-detail-tooltip');
   if(!point||!time){if(!pinned.time && x)x.textContent='滑鼠移到 K 線查看；點一下可固定，按 Esc 解除'; if(panel&&!locked&&!pinned.time)panel.classList.add('hidden');return;}
   const key=typeof time==='string'?time:(time.year?`${time.year}-${String(time.month).padStart(2,'0')}-${String(time.day).padStart(2,'0')}`:String(time));
   const i=model.lastCandles.findIndex(r=>r.time===key), bar=i<0?null:model.lastCandles[i];
   if(x)x.textContent=`${locked?'已鎖定 • ':''}${key}　開 ${number(point.open)}　高 ${number(point.high)}　低 ${number(point.low)}　收 ${number(point.close)}`;
   if(panel){
     panel.classList.remove('hidden');
     const change=bar&&i>0?bar.close-model.lastCandles[i-1].close:null;
     panel.textContent=`${locked?'📌 已固定 · ':''}${key}  開 ${number(point.open)}  高 ${number(point.high)}  低 ${number(point.low)}  收 ${number(point.close)}  量 ${number(bar?.volume,0)} 股  漲跌 ${change==null?'—':(change>0?'+':'')+number(change)}`;
   }
 }
 function releasePinned(){
   pinned.time=null;pinned.close=null;
   if(model.chart.clearCrosshairPosition)model.chart.clearCrosshairPosition();
   $('candle-detail-tooltip')?.classList.add('hidden');
   const status=$('candle-lock-status');if(status)status.textContent='移動滑鼠定位；單擊固定 K 棒';
 }
 model.chart.subscribeCrosshairMove(param=>{
   if(pinned.time)return;
   const point=param.seriesData?.get(model.candleSeries);
   showCandleInfo(param.time,point);
 });
 model.chart.subscribeClick(param=>{
   const point=param.seriesData?.get(model.candleSeries);
   if(!point||!param.time){releasePinned();return;}
   const key=typeof param.time==='string'?param.time:(param.time.year?`${param.time.year}-${String(param.time.month).padStart(2,'0')}-${String(param.time.day).padStart(2,'0')}`:String(param.time));
   if(pinned.time===key){releasePinned();return;}
   pinned.time=key;pinned.close=point.close;
   if(model.chart.setCrosshairPosition)model.chart.setCrosshairPosition(point.close,param.time,model.candleSeries);
   showCandleInfo(param.time,point,true);
   const status=$('candle-lock-status');if(status)status.textContent='已鎖定 '+key+'；再點一次或按 Esc 解除';
 });
 document.addEventListener('keydown',e=>{if(e.key==='Escape'&&pinned.time)releasePinned();});
 if(typeof ResizeObserver!=='undefined'){
    model.chartObserver=new ResizeObserver(()=>resizeChart());model.chartObserver.observe(el);
 }
 return true;
}
function resizeChart(){
 const el=$('stockKChart');if(model.chart&&el&&el.clientWidth>0){model.chart.applyOptions({width:el.clientWidth,height:el.clientHeight||420});}
}
function ma(rows,n){return rows.map((x,i)=>i>=n-1?{time:x.time,value:rows.slice(i-n+1,i+1).reduce((a,b)=>a+b.close,0)/n}:null).filter(Boolean);}
function drawChart(){
 if(currentPage!=='terminal')return;
 if(!ensureChart())return;
 const rows=model.lastCandles;
 pinned.time=null; pinned.close=null; $('candle-detail-tooltip')?.classList.add('hidden');
 model.candleSeries.setData(rows.map(x=>({time:x.time,open:x.open,high:x.high,low:x.low,close:x.close})));
 model.ma5.setData(model.showMA?ma(rows,5):[]);
 model.ma20.setData(model.showMA?ma(rows,20):[]);
 model.volSeries.setData(model.showVolume?rows.filter(x=>Number(x.volume)>0).map(x=>({time:x.time,value:x.volume,color:x.close>=x.open?'rgba(239,68,68,.45)':'rgba(16,185,129,.45)'})):[]);
 resizeChart();
 if(rows.length){model.chart.timeScale().fitContent();
 const last=rows[rows.length-1];
 const extra=$('candle-latest');if(extra)extra.textContent=`最新日 K：${last.time}　O ${number(last.open)}　H ${number(last.high)}　L ${number(last.low)}　C ${number(last.close)}`;
 }else{const extra=$('candle-latest');if(extra)extra.textContent='尚未取得真實歷史日 K';}
}
window.renderStockChart=()=>{if(currentPage==='terminal')loadCandles();};
window.renderWatchlistPage=function(){
 const el=$('watchlist-cards-container');if(!el)return;
 if(!userState?.starredCodes?.length){el.innerHTML='<p class="text-slate-400">目前沒有自選股。可在全市場表格點星號加入。</p>';return;}
 el.innerHTML=userState.starredCodes.map(c=>{
 const x=model.map.get(c),s=STOCK_DB[c],name=x?.name||s?.name||'資料待同步';
 return `<div class="glass-panel rounded-2xl p-4 flex items-center justify-between gap-2"><div><div class="font-bold text-white">${safeText(c)} ${safeText(name)}</div>
 <div class="text-xs text-cyan-400 mt-1">${x?.price!=null?number(x.price)+' 元 · '+(isIntraday(x)?'盤中參考':'官方盤後'):'尚無可靠行情'}</div>
 <div class="text-[10px] text-slate-500">${safeText(x?asOfText(x):'')}</div></div><button data-watch-open="${safeText(c)}" class="px-3 py-1 bg-indigo-600 rounded text-xs text-white shrink-0">查看 K 線</button></div>`;
 }).join('');
};
function installEvents(){
 setupSearchEvents();
 $('market-search')?.addEventListener('input',e=>{model.query=e.target.value;model.page=1;renderEnhancedTable();});
 $('market-kind')?.addEventListener('change',e=>{model.kind=e.target.value;model.page=1;renderEnhancedTable();});
 $('market-sort')?.addEventListener('change',e=>{model.sort=e.target.value;model.page=1;renderEnhancedTable();});
 $('market-prev')?.addEventListener('click',()=>{model.page=Math.max(1,model.page-1);renderEnhancedTable();});
 $('market-next')?.addEventListener('click',()=>{model.page++;renderEnhancedTable();});
 $('market-reload')?.addEventListener('click',async()=>{model.loadingMarket=null;await loadUniverse(true);});
 $('dashboard-market-tbody')?.addEventListener('click',e=>{
 const star=e.target.closest('[data-star]');
 if(star){e.stopPropagation();toggleStar(star.dataset.star,e);renderEnhancedTable();return;}
 const row=e.target.closest('[data-stock]');if(row)openStockInTerminal(row.dataset.stock);
 });
 $('watchlist-cards-container')?.addEventListener('click',e=>{const button=e.target.closest('[data-watch-open]');if(button)openStockInTerminal(button.dataset.watchOpen);});
}
const previousOnload=window.onload;
window.onload=async function(){
 // Do not run the old code's simulated 6-stock auto refresh / old line chart; its wrapper now queries official local gateway.
 try{await previousOnload?.();}catch(err){console.error('初始化原有功能時發生錯誤',err);}
 installEvents();
 model.selectedRange='3M';
 model.marketStatus='正在載入 TWSE／TPEx 股票名單…';
 renderEnhancedTable();
 await loadUniverse();
 startQuotePolling();
};
})();
