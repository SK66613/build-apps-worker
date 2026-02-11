// src/services/templates.ts

// ================================
// Templates (каталог 3–5 шаблонов)
// ================================
// Шаблоны живут в воркере для старта (дальше можно вынести в KV/D1).
export const TEMPLATE_CATALOG = [
  { id: 'blank',        title: 'Пустой',        desc: 'Чистый проект с одной страницей' },
  { id: 'beer_club',     title: 'Craft Beer',   desc: 'Главная + Игры + Бонусы + Профиль' },
  { id: 'coffee_loyalty',title: 'Coffee',       desc: 'Лояльность + меню + карта' },
  { id: 'quiz_lead',     title: 'Квиз',         desc: 'Квиз-воронка для лидов' },
];

export function getSeedConfig(templateId) {
  const id = (templateId || 'blank').toString();

  // helper: deep clone defaults + overrides
  const clone = (x)=> JSON.parse(JSON.stringify(x||{}));
  const rnd = ()=> Math.random().toString(36).slice(2,9);
  const mkId = ()=> 'b_' + rnd();

  // ВАЖНО: ключи блоков должны существовать в templates.js (BlockRegistry)
  // (promo, infoCardPlain, gamesList, infoCardChevron, spacer, beerHero, beerIntroSlider, beerStartList)
  const mkBlock = (key, defaults, overrides={})=>{
    const bid = mkId();
    return {
      ref: { id: bid, key, type: key },     // то что лежит в route.blocks[]
      props: { [bid]: { ...clone(defaults), ...clone(overrides) } } // то что лежит в BP.blocks[id]
    };
  };

  // базовая структура BP
  const base = {
    theme: {},
    nav: { routes: [{ path: '/', title: 'Главная', id: 'home', icon: 'home' }] },
    routes: [{ path: '/', id: 'home', title: 'Главная', blocks: [] }],
    blocks: {} // <--- обязательно
  };

  // helper: применить набор блоков на страницу
  const setRouteBlocks = (routePath, blocksArr)=>{
    const route = base.routes.find(r=>r.path===routePath);
    if(!route) return;
    route.blocks = blocksArr.map(b=>b.ref);
    blocksArr.forEach(b=> Object.assign(base.blocks, b.props));
  };

  // ===== templates nav/routes =====
  if (id === 'beer_club') {
    base.nav.routes = [
      { path: '/',          title: 'Главная',  id: 'home',      icon: 'home' },
      { path: '/play',      title: 'Играть',   id: 'play',      icon: 'game' },
      { path: '/bonuses',   title: 'Бонусы',   id: 'bonuses',   icon: 'gift' },
      { path: '/profile',   title: 'Профиль',  id: 'profile',   icon: 'user' },
    ];
    base.routes = base.nav.routes.map(r => ({ path: r.path, id: r.id, title: r.title, blocks: [] }));
  }

  if (id === 'coffee_loyalty') {
    base.nav.routes = [
      { path: '/',          title: 'Главная',     id: 'home',    icon: 'home' },
      { path: '/menu',      title: 'Меню',        id: 'menu',    icon: 'menu' },
      { path: '/loyalty',   title: 'Лояльность',  id: 'loyalty', icon: 'star' },
      { path: '/profile',   title: 'Профиль',     id: 'profile', icon: 'user' },
    ];
    base.routes = base.nav.routes.map(r => ({ path: r.path, id: r.id, title: r.title, blocks: [] }));
  }

  if (id === 'quiz_lead') {
    base.nav.routes = [
      { path: '/',        title: 'Квиз',      id: 'quiz',     icon: 'quiz' },
      { path: '/result',  title: 'Результат', id: 'result',   icon: 'check' },
    ];
    base.routes = base.nav.routes.map(r => ({ path: r.path, id: r.id, title: r.title, blocks: [] }));
  }

  // ===== seed blocks per template (рандомно, но различимо) =====
  // Дефолты мы НЕ тянем с фронта — просто задаём минимум props, остальное заполнится в редакторе
  if (id === 'blank') {
    const b1 = mkBlock('infoCardPlain',
      { icon:'', title:'Пустой проект', sub:'Добавь блоки из библиотеки', imgSide:'left', action:'none', link:'', sheet_id:'', sheet_path:'' }
    );
    const sp = mkBlock('spacer', { size: 12 });
    setRouteBlocks('/', [b1, sp]);
  }

  if (id === 'coffee_loyalty') {
    const promo = mkBlock('promo', { interval: 3200, slides: [
      { img:'', action:'link', link:'#menu', sheet_id:'', sheet_path:'' },
      { img:'', action:'link', link:'#loyalty', sheet_id:'', sheet_path:'' },
      { img:'', action:'link', link:'#profile', sheet_id:'', sheet_path:'' }
    ]});
    const card = mkBlock('infoCardPlain',
      { icon:'beer/img/beer_hero.jpg', title:'Coffee', sub:'Шаблон Coffee', imgSide:'left', action:'none', link:'', sheet_id:'', sheet_path:'' }
    );
    setRouteBlocks('/', [promo, card]);

    const menu = mkBlock('gamesList', { title:'Меню (демо)', cards: [
      { icon:'beer/img/game1.png', title:'Эспрессо', sub:'120₽', btn:'Добавить', action:'none', link:'', sheet_id:'', sheet_path:'' },
      { icon:'beer/img/game2.png', title:'Капучино', sub:'180₽', btn:'Добавить', action:'none', link:'', sheet_id:'', sheet_path:'' }
    ]});
    setRouteBlocks('/menu', [menu]);

    const loyalty = mkBlock('infoCardChevron',
      { icon:'beer/img/beer_hero.jpg', title:'Лояльность', sub:'Штампы / баллы', action:'none', link:'#', sheet_id:'', sheet_path:'' }
    );
    setRouteBlocks('/loyalty', [loyalty]);
  }

  if (id === 'beer_club') {
    const hero = mkBlock('beerHero', { title:'Craft Beer Club', text:'Шаблон Beer', img:'beer/img/beer_hero.jpg' });
    const intro = mkBlock('beerIntroSlider', { slides: [
      { title:'Как работает', text:'Играй и копи монеты', primary:'Продолжить', ghost:'' },
      { title:'Погнали', text:'Первый спин — подарок', primary:'Играть', ghost:'' }
    ]});
    const start = mkBlock('beerStartList', { title:'С чего начать' });
    setRouteBlocks('/', [hero, intro, start]);

    const play = mkBlock('gamesList', { title:'Игры', cards: [
      { icon:'beer/img/game1.png', title:'Bumblebee', sub:'Долети — получи приз', btn:'Играть', action:'link', link:'#play_bumble', sheet_id:'', sheet_path:'' }
    ]});
    setRouteBlocks('/play', [play]);

    const wheel = mkBlock('bonus_wheel_one', { title:'Колесо бонусов', spin_cost: 10, prizes: [] });
    setRouteBlocks('/bonuses', [wheel]);
  }

  if (id === 'quiz_lead') {
    const h = mkBlock('infoCardChevron', { icon:'', title:'Квиз', sub:'Шаблон Quiz', action:'none', link:'#', sheet_id:'', sheet_path:'' });
    const sp = mkBlock('spacer', { size: 10 });
    const c = mkBlock('infoCardPlain', { icon:'', title:'Вопрос 1', sub:'Пока демо', imgSide:'right', action:'none', link:'', sheet_id:'', sheet_path:'' });
    setRouteBlocks('/', [h, sp, c]);

    const res = mkBlock('infoCardPlain', { icon:'', title:'Результат', sub:'Спасибо! Мы свяжемся.', imgSide:'left', action:'none', link:'', sheet_id:'', sheet_path:'' });
    setRouteBlocks('/result', [res]);
  }

  return base;
}


// ================== LEGACY HELPERS: JSON ==================
// ВАЖНО: CORS НЕ делаем внутри legacy.
// CORS навешивается только один раз в src/index.ts через withCors().
// Это убирает вечные TDZ/ReferenceError после бандлинга.

function legacyCorsHeaders(_request: any) {
  // оставляем только Vary, чтобы не ломать существующие вызовы
  return { "Vary": "Origin" };
}


function json(obj, status = 200, request = null) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json; charset=UTF-8",
      // legacyCorsHeaders() теперь безопасен и не делает allowlist
      ...legacyCorsHeaders(request),
    },
  });
}






// ================== BLOCKS PROXY ==================
// Вынесено в src/routes/blocksProxy.ts (fast path в index.ts)
// Чтобы не было дублирования логики и конфликтов после бандлинга.


// ================== AUTH: email + password ==================

// простой base64url для JWT-подобных токенов
function base64UrlEncode(bytes) {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  let base64 = btoa(binary);
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecode(str) {
  let base64 = str.replace(/-/g, "+").replace(/_/g, "/");
  const pad = base64.length % 4;
  if (pad) {
    base64 += "=".repeat(4 - pad);
  }
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function randomToken(len = 32) {
  const buf = new Uint8Array(len);
  crypto.getRandomValues(buf);
  return base64UrlEncode(buf);
}

