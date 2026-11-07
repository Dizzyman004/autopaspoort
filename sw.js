// Autopaspoort service worker: keeps the app itself on the device so it also opens offline.
// Your data is not here; it lives in the browser's own storage (IndexedDB).
// Online it always takes the newest version from the site; offline it uses the copy.
const VERSION = "6fbec878f5";
const CACHE = 'autopaspoort-' + VERSION;
const FILES = ["./", "index.html", "app.css?v=6fbec878f5", "app.js?v=6fbec878f5", "manifest.webmanifest", "favicon.svg", "favicon-32.png", "apple-touch-icon.png", "icon-192.png", "icon-512.png", "react.production.min.js", "react-dom.production.min.js", "jspdf.umd.min.js", "jspdf.plugin.autotable.min.js", "xlsx.full.min.js", "pdf.min.js", "pdf.worker.min.js", "barlow-latin-400-normal.woff2", "barlow-latin-500-normal.woff2", "barlow-latin-600-normal.woff2", "barlow-latin-700-normal.woff2", "barlow-semi-condensed-latin-500-normal.woff2", "barlow-semi-condensed-latin-600-normal.woff2", "barlow-semi-condensed-latin-700-normal.woff2", "barlow-semi-condensed-latin-800-normal.woff2"];
const SCOPE = self.registration.scope;
const KNOWN = new Set(FILES.map((f) => new URL(f, SCOPE).href.split('?')[0]));

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches
      .open(CACHE)
      .then((cache) => cache.addAll(FILES.map((f) => new Request(f, { cache: 'reload' }))))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((k) => k.startsWith('autopaspoort-') && k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

function timeout(promise, ms) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout')), ms);
    promise.then(
      (v) => {
        clearTimeout(t);
        resolve(v);
      },
      (e) => {
        clearTimeout(t);
        reject(e);
      }
    );
  });
}

async function respond(request) {
  const cache = await caches.open(CACHE);
  const url = request.url.split('?')[0];
  try {
    const res = await timeout(fetch(request), request.mode === 'navigate' ? 5000 : 10000);
    if (res && res.ok && res.type === 'basic' && (KNOWN.has(url) || request.mode === 'navigate')) cache.put(request, res.clone());
    return res;
  } catch (e) {
    const hit = (await cache.match(request, { ignoreSearch: true })) || (request.mode === 'navigate' ? (await cache.match('./')) || (await cache.match('index.html')) : null);
    if (hit) return hit;
    throw e;
  }
}

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET' || request.headers.has('range')) return;
  if (!request.url.startsWith(SCOPE)) return; // the RDW and other sites go straight to the network
  event.respondWith(respond(request));
});
