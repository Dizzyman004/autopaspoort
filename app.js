(() => {
  // src/web/shim.js
  var DB_NAME = "autopaspoort";
  var DB_VERSION = 1;
  var META = Object.freeze({ fromCache: false, hasPendingWrites: false });
  var state = {
    idb: null,
    colls: /* @__PURE__ */ new Map(),
    // collection → Map(id → data)
    listeners: /* @__PURE__ */ new Set(),
    dirty: /* @__PURE__ */ new Set(),
    flushTimer: null,
    blobUrls: {},
    blobMeta: /* @__PURE__ */ new Map(),
    // id → { type, size, createdAt }
    inbox: Promise.resolve()
  };
  var channel = null;
  var clone = (x) => x === void 0 ? void 0 : JSON.parse(JSON.stringify(x));
  var isObj = (v) => v && typeof v === "object" && !Array.isArray(v);
  function merge(a, b) {
    for (const k of Object.keys(b)) {
      const v = b[k];
      if (isObj(v) && isObj(a[k])) a[k] = merge(a[k], v);
      else a[k] = v;
    }
    return a;
  }
  function fail(code, message) {
    const e = new Error(message || code);
    e.code = code;
    return e;
  }
  function mapErr(e) {
    if (e && e.code && typeof e.code === "string") return e;
    const name = e && e.name || "";
    if (name === "QuotaExceededError") return fail("quota_exceeded", "Opslag vol");
    if (name === "DataCloneError") return fail("invalid_argument", "Kan dit niet opslaan");
    return fail("unavailable", e && e.message || "Opslag niet beschikbaar");
  }
  var randomId = (bytes = 16) => Array.from(crypto.getRandomValues(new Uint8Array(bytes))).map((b) => b.toString(16).padStart(2, "0")).join("");
  function openDb() {
    return new Promise((resolve, reject) => {
      let req;
      try {
        req = indexedDB.open(DB_NAME, DB_VERSION);
      } catch (e) {
        reject(e);
        return;
      }
      req.onupgradeneeded = () => {
        const db2 = req.result;
        if (!db2.objectStoreNames.contains("docs")) db2.createObjectStore("docs", { keyPath: "path" });
        if (!db2.objectStoreNames.contains("blobs")) db2.createObjectStore("blobs", { keyPath: "id" });
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
      req.onblocked = () => reject(fail("unavailable", "Opslag is bezet door een ander tabblad"));
    });
  }
  function getAll(store) {
    return new Promise((resolve, reject) => {
      let t;
      try {
        t = state.idb.transaction(store, "readonly");
      } catch (e) {
        reject(mapErr(e));
        return;
      }
      const r = t.objectStore(store).getAll();
      r.onsuccess = () => resolve(r.result || []);
      r.onerror = () => reject(mapErr(r.error));
    });
  }
  function getOne(store, key) {
    return new Promise((resolve, reject) => {
      let t;
      try {
        t = state.idb.transaction(store, "readonly");
      } catch (e) {
        reject(mapErr(e));
        return;
      }
      const r = t.objectStore(store).get(key);
      r.onsuccess = () => resolve(r.result || null);
      r.onerror = () => reject(mapErr(r.error));
    });
  }
  function write(store, ops) {
    return new Promise((resolve, reject) => {
      let t;
      try {
        t = state.idb.transaction(store, "readwrite");
      } catch (e) {
        reject(mapErr(e));
        return;
      }
      t.oncomplete = () => resolve();
      t.onabort = () => reject(mapErr(t.error));
      const s = t.objectStore(store);
      try {
        for (const [op, v] of ops) {
          if (op === "put") s.put(v);
          else if (op === "delete") s.delete(v);
          else if (op === "clear") s.clear();
        }
      } catch (e) {
        try {
          t.abort();
        } catch (e2) {
        }
        reject(mapErr(e));
      }
    });
  }
  var recBlob = (r) => r.blob instanceof Blob ? r.blob : new Blob([r.buf || new ArrayBuffer(0)], { type: r.type || "" });
  async function putBlob(id, blob, type) {
    const rec = { id, type, size: blob.size, createdAt: (/* @__PURE__ */ new Date()).toISOString() };
    try {
      await write("blobs", [["put", Object.assign({ blob }, rec)]]);
    } catch (e) {
      if (e.code !== "invalid_argument" && e.code !== "unavailable") throw e;
      await write("blobs", [["put", Object.assign({ buf: await blob.arrayBuffer() }, rec)]]);
    }
    return rec;
  }
  function collMap(c) {
    let m = state.colls.get(c);
    if (!m) state.colls.set(c, m = /* @__PURE__ */ new Map());
    return m;
  }
  function applyLocal(c, id, data) {
    const m = collMap(c);
    if (data === null || data === void 0) m.delete(id);
    else m.set(id, data);
    state.dirty.add(c);
    if (!state.flushTimer) state.flushTimer = setTimeout(flush, 0);
  }
  function snapOf(c) {
    const docs = [];
    for (const [id, data] of collMap(c)) {
      const body = clone(data);
      docs.push({ id, exists: true, data: () => body, metadata: META });
    }
    docs.sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
    return { docs, size: docs.length, empty: docs.length === 0, metadata: META, docChanges: () => [] };
  }
  function docSnap(c, id) {
    const d = collMap(c).get(id);
    return { id, exists: d !== void 0, data: () => clone(d), metadata: META };
  }
  function flush() {
    state.flushTimer = null;
    const dirty = [...state.dirty];
    state.dirty.clear();
    for (const l of [...state.listeners]) {
      if (!dirty.includes(l.coll) || !state.listeners.has(l)) continue;
      try {
        l.next(l.id ? docSnap(l.coll, l.id) : snapOf(l.coll));
      } catch (e) {
        setTimeout(() => {
          throw e;
        });
      }
    }
  }
  async function writeDocs(changes) {
    const prev = changes.map(([c, id]) => [c, id, collMap(c).has(id) ? collMap(c).get(id) : null]);
    for (const [c, id, data] of changes) applyLocal(c, id, data);
    try {
      await write(
        "docs",
        changes.map(([c, id, data]) => data === null ? ["delete", c + "/" + id] : ["put", { path: c + "/" + id, coll: c, id, data }])
      );
    } catch (e) {
      for (const [c, id, data] of prev) applyLocal(c, id, data);
      throw e;
    }
    if (channel) channel.postMessage({ t: "docs", changes });
    askPersist();
  }
  var persistAsked = false;
  function askPersist() {
    if (persistAsked) return;
    persistAsked = true;
    try {
      if (navigator.storage && navigator.storage.persist) navigator.storage.persist().catch(() => {
      });
    } catch (e) {
    }
  }
  function checkData(data) {
    if (!isObj(data)) throw fail("invalid_argument", "Een document is een object");
    return clone(data);
  }
  function docRef(c, id) {
    const path = c + "/" + id;
    return {
      id,
      path,
      async get() {
        return docSnap(c, id);
      },
      async set(data) {
        await writeDocs([[c, id, checkData(data)]]);
      },
      async update(data) {
        const cur = collMap(c).get(id);
        if (cur === void 0) throw fail("invalid_argument", "Document bestaat niet: " + path);
        await writeDocs([[c, id, merge(clone(cur), checkData(data))]]);
      },
      async delete() {
        if (!collMap(c).has(id)) return;
        await writeDocs([[c, id, null]]);
      },
      onSnapshot(next) {
        const l = { coll: c, id, next };
        state.listeners.add(l);
        setTimeout(() => state.listeners.has(l) && next(docSnap(c, id)), 0);
        return () => state.listeners.delete(l);
      }
    };
  }
  function splitPath(p) {
    const parts = String(p || "").split("/");
    if (parts.length !== 2 || !parts[0] || !parts[1]) throw fail("invalid_argument", "Onbekend pad " + p);
    return parts;
  }
  var db = Object.freeze({
    doc: (path) => {
      const [c, id] = splitPath(path);
      return docRef(c, id);
    },
    collection: (c) => ({
      path: c,
      doc: (id) => docRef(c, id || randomId(10)),
      async get() {
        return snapOf(c);
      },
      async add(data) {
        const r = docRef(c, randomId(10));
        await r.set(data);
        return r;
      },
      onSnapshot(next) {
        const l = { coll: c, next };
        state.listeners.add(l);
        setTimeout(() => state.listeners.has(l) && next(snapOf(c)), 0);
        return () => state.listeners.delete(l);
      }
    })
  });
  function remember(id, blob, meta) {
    if (state.blobUrls[id]) URL.revokeObjectURL(state.blobUrls[id]);
    state.blobUrls[id] = URL.createObjectURL(blob);
    state.blobMeta.set(id, meta);
  }
  function forget(id) {
    if (state.blobUrls[id]) URL.revokeObjectURL(state.blobUrls[id]);
    delete state.blobUrls[id];
    state.blobMeta.delete(id);
  }
  var assets = Object.freeze({
    async upload(blob, opts) {
      if (!(blob instanceof Blob)) throw fail("invalid_request", "Geen bestand");
      const type = opts && opts.type || blob.type || "application/octet-stream";
      const id = randomId(16);
      const typed = blob.type === type ? blob : blob.slice(0, blob.size, type);
      const rec = await putBlob(id, typed, type);
      remember(id, typed, rec);
      if (channel) channel.postMessage({ t: "blob", id });
      return { id, url: state.blobUrls[id], sizeBytes: blob.size, contentType: type };
    },
    async list() {
      const list = [...state.blobMeta].map(([id, m]) => ({ id, contentType: m.type, sizeBytes: m.size, createdAt: m.createdAt }));
      return { assets: list, usage: { files: list.length, bytes: list.reduce((a, x) => a + (x.sizeBytes || 0), 0) } };
    },
    async delete(id) {
      if (!id) return { deleted: false };
      await write("blobs", [["delete", id]]);
      forget(id);
      if (channel) channel.postMessage({ t: "blobdel", id });
      return { deleted: true };
    }
  });
  var IS_IOS = /iPad|iPhone|iPod/.test(navigator.userAgent) || navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1;
  var TYPES = { pdf: "application/pdf", xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", zip: "application/zip", json: "application/json", jpg: "image/jpeg", jpeg: "image/jpeg", png: "image/png", txt: "text/plain" };
  var typeFor = (name) => TYPES[String(name || "").split(".").pop().toLowerCase()] || "application/octet-stream";
  var kb = (n) => n >= 1048576 ? (n / 1048576).toFixed(1).replace(".", ",") + " MB" : Math.max(1, Math.round(n / 1024)) + " kB";
  function linkDownload(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.rel = "noopener";
    a.style.display = "none";
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 12e4);
  }
  function shareCard(file) {
    return new Promise((resolve, reject) => {
      const wrap = document.createElement("div");
      wrap.className = "dl-wrap";
      wrap.setAttribute("role", "dialog");
      wrap.setAttribute("aria-modal", "true");
      wrap.setAttribute("aria-labelledby", "dl-title");
      wrap.innerHTML = '<div class="dl-card"><span class="dl-icon" aria-hidden="true"><svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/><path d="M14 3v5h5"/><path d="M12 12v6"/><path d="m9 15 3 3 3-3"/></svg></span><strong id="dl-title" class="dl-title">Je bestand is klaar</strong><span class="dl-name"></span><button type="button" class="btn btn-primary btn-block dl-go">Bewaar of deel</button><button type="button" class="btn btn-secondary btn-block dl-close">Sluiten</button><p class="dl-tip">Kies Bewaar in Bestanden, of stuur het meteen door.</p></div>';
      wrap.querySelector(".dl-name").textContent = file.name + " · " + kb(file.size);
      const done = (fn, v) => {
        wrap.remove();
        document.removeEventListener("keydown", onKey);
        fn(v);
      };
      const onKey = (e) => e.key === "Escape" && done(reject, fail("declined", "Gesloten"));
      wrap.querySelector(".dl-close").onclick = () => done(reject, fail("declined", "Gesloten"));
      wrap.querySelector(".dl-go").onclick = async () => {
        try {
          await navigator.share({ files: [file] });
          done(resolve, { status: "saved" });
        } catch (e) {
          if (e && e.name === "AbortError") return;
          linkDownload(file, file.name);
          done(resolve, { status: "saved" });
        }
      };
      document.addEventListener("keydown", onKey);
      document.body.appendChild(wrap);
      setTimeout(() => wrap.querySelector(".dl-go").focus(), 30);
    });
  }
  var downloads = Object.freeze({
    async save({ filename, data }) {
      const name = String(filename || "bestand").replace(/[\\/:*?"<>|]+/g, "-");
      const type = data && data.type || typeFor(name);
      const blob = data instanceof Blob ? data.type ? data : data.slice(0, data.size, type) : new Blob([data], { type });
      if (IS_IOS && navigator.share && navigator.canShare) {
        const file = new File([blob], name, { type: blob.type || type });
        let ok = false;
        try {
          ok = navigator.canShare({ files: [file] });
        } catch (e) {
          ok = false;
        }
        if (ok) return shareCard(file);
      }
      linkDownload(blob, name);
      return { status: "saved" };
    }
  });
  function onMessage(msg) {
    state.inbox = state.inbox.then(async () => {
      if (!msg || !msg.t) return;
      if (msg.t === "reload") return window.location.reload();
      if (msg.t === "docs") for (const [c, id, data] of msg.changes || []) applyLocal(c, id, data);
      if (msg.t === "blob") {
        const r = await getOne("blobs", msg.id).catch(() => null);
        if (r) remember(r.id, recBlob(r), { type: r.type, size: r.size, createdAt: r.createdAt });
      }
      if (msg.t === "blobdel") forget(msg.id);
    });
  }
  async function exportAll() {
    const docs = (await getAll("docs")).map((r) => ({ path: r.path, data: r.data }));
    const blobs = (await getAll("blobs")).map((r) => ({ id: r.id, type: r.type, size: r.size, createdAt: r.createdAt, blob: recBlob(r) }));
    return { docs, blobs };
  }
  function getAllKeys(store) {
    return new Promise((resolve, reject) => {
      let t;
      try {
        t = state.idb.transaction(store, "readonly");
      } catch (e) {
        reject(mapErr(e));
        return;
      }
      const r = t.objectStore(store).getAllKeys();
      r.onsuccess = () => resolve(r.result || []);
      r.onerror = () => reject(mapErr(r.error));
    });
  }
  async function importAll({ docs, blobs }) {
    const ops = [["clear"]];
    for (const d of docs) {
      const [c, id] = splitPath(d.path);
      ops.push(["put", { path: d.path, coll: c, id, data: clone(d.data) }]);
    }
    const keep = /* @__PURE__ */ new Set();
    for (const b of blobs) {
      await putBlob(b.id, b.blob, b.type || b.blob.type || "application/octet-stream");
      keep.add(b.id);
    }
    await write("docs", ops);
    const old = (await getAllKeys("blobs")).filter((id) => !keep.has(id));
    if (old.length) await write("blobs", old.map((id) => ["delete", id]));
    if (channel) channel.postMessage({ t: "reload" });
  }
  async function storageInfo() {
    let estimate = null;
    try {
      if (navigator.storage && navigator.storage.estimate) estimate = await navigator.storage.estimate();
    } catch (e) {
      estimate = null;
    }
    let persisted = null;
    try {
      if (navigator.storage && navigator.storage.persisted) persisted = await navigator.storage.persisted();
    } catch (e) {
      persisted = null;
    }
    const files = [...state.blobMeta.values()];
    return { files: files.length, bytes: files.reduce((a, m) => a + (m.size || 0), 0), usage: estimate ? estimate.usage : null, quota: estimate ? estimate.quota : null, persisted };
  }
  async function boot() {
    if (typeof indexedDB === "undefined") throw fail("no_storage", "Geen opslag");
    state.idb = await openDb();
    state.idb.onversionchange = () => {
      state.idb.close();
      window.location.reload();
    };
    for (const r of await getAll("docs")) {
      const [c, id] = String(r.path).split("/");
      if (c && id && r.data) collMap(c).set(id, r.data);
    }
    for (const r of await getAll("blobs")) remember(r.id, recBlob(r), { type: r.type, size: r.size, createdAt: r.createdAt });
    window.__apBlobUrls = state.blobUrls;
    try {
      channel = typeof BroadcastChannel !== "undefined" ? new BroadcastChannel("autopaspoort") : null;
      if (channel) channel.onmessage = (e) => onMessage(e.data);
    } catch (e) {
      channel = null;
    }
    if (state.colls.size) askPersist();
  }
  var booted = null;
  function install() {
    if (window.claude) return;
    booted = boot().then(
      () => true,
      (e) => {
        window.__apStorageError = e && (e.code || e.name) || "unavailable";
        return false;
      }
    );
    const caps = { db, assets, downloads };
    window.claude = Object.freeze({
      use: (name) => booted.then((ok) => ok || name === "downloads" ? caps[name] || null : null)
    });
  }

  // src/web/boot.js
  install();
  var local = location.hostname === "localhost" || location.hostname === "127.0.0.1";
  if ("serviceWorker" in navigator && (location.protocol === "https:" || local)) {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("sw.js").catch(() => {
      });
    });
  }
  window.addEventListener("beforeinstallprompt", (e) => {
    e.preventDefault();
    window.__apInstall = e;
    window.dispatchEvent(new Event("ap-install"));
  });
  window.addEventListener("appinstalled", () => {
    window.__apInstall = null;
    window.dispatchEvent(new Event("ap-install"));
  });

  // src/extra.js
  var EXTRA = { sheets: {}, GarageTop: null, MeerTop: null };

  // src/util.js
  var VARIANT = true ? "web" : "live";
  var IS_TEST = VARIANT === "test";
  var IS_WEB = VARIANT === "web";
  var IS_NOAI = VARIANT === "noai" || IS_WEB;
  var RDW_DIRECT = IS_WEB;
  var RDW_LOOKUP = "https://ovi.rdw.nl/";
  var CATS = [
    ["motor", "Motor"],
    ["inlaat", "Turbo, in- en uitlaat"],
    ["aandrijving", "Aandrijving en koppeling"],
    ["onderstel", "Onderstel en ophanging"],
    ["remmen", "Remmen"],
    ["wielen", "Banden en wielen"],
    ["carrosserie", "Carrosserie en lak"],
    ["interieur", "Interieur"],
    ["verlichting", "Verlichting"],
    ["elektronica", "Elektronica"],
    ["onderhoud", "Onderhoud en reparatie"],
    ["apk", "APK en keuring"],
    ["vaste", "Verzekering, belasting, stalling"],
    ["brandstof", "Brandstof"],
    ["gereedschap", "Gereedschap"],
    ["reis", "Tol, vignet en parkeren"],
    ["overig", "Overig"]
  ];
  var CAT_LABEL = Object.fromEntries(CATS);
  var catLabel = (id) => CAT_LABEL[id] || "Overig";
  var isCat = (id) => Object.prototype.hasOwnProperty.call(CAT_LABEL, id);
  var GEBRUIK = [
    ["dagelijks", "Dagelijks"],
    ["project", "Project"],
    ["hobby", "Hobby"]
  ];
  var gebruikLabel = (id) => (GEBRUIK.find((g) => g[0] === id) || [null, ""])[1];
  var SPECS = [
    ["motor", "Motor"],
    ["transmissie", "Transmissie"],
    ["vermogen", "Vermogen"],
    ["koppel", "Koppel"],
    ["nul100", "0–100 km/h"],
    ["top", "Topsnelheid"],
    ["gewicht", "Gewicht"]
  ];
  var eurFull = new Intl.NumberFormat("nl-NL", { style: "currency", currency: "EUR" });
  var eurRound = new Intl.NumberFormat("nl-NL", { style: "currency", currency: "EUR", maximumFractionDigits: 0, minimumFractionDigits: 0 });
  var numFmt = new Intl.NumberFormat("nl-NL");
  var isNum = (v) => typeof v === "number" && isFinite(v);
  var eur = (v) => isNum(v) ? eurFull.format(v) : "–";
  var eurR = (v) => isNum(v) ? eurRound.format(Math.round(v)) : "–";
  var num = (v) => isNum(v) ? numFmt.format(v) : "–";
  function parseNum(s) {
    if (s == null || s === "") return null;
    if (typeof s === "number") return isFinite(s) ? s : null;
    let t = String(s).replace(/[^\d,.\-]/g, "");
    if (!t || t === "-") return null;
    const c = t.lastIndexOf(",");
    const d = t.lastIndexOf(".");
    if (c > -1 && d > -1) {
      t = c > d ? t.replace(/\./g, "").replace(",", ".") : t.replace(/,/g, "");
    } else if (c > -1) {
      const commas = (t.match(/,/g) || []).length;
      const after = t.length - c - 1;
      t = commas > 1 || after === 3 && c > 0 && c <= 3 && t.length > 4 ? t.replace(/,/g, "") : t.replace(",", ".");
    } else if (d > -1) {
      const dots = (t.match(/\./g) || []).length;
      const after = t.length - d - 1;
      if (dots > 1 || after === 3 && t.length > 4) t = t.replace(/\./g, "");
    }
    const n = parseFloat(t);
    return isFinite(n) ? n : null;
  }
  var toInput = (v) => isNum(v) ? Number.isInteger(v) ? String(v) : v.toFixed(2).replace(".", ",") : "";
  var nice = (s) => String(s || "").split(/(\s+|-)/).map((w) => /^[A-ZÀ-Ý]{4,}$/.test(w) ? w.charAt(0) + w.slice(1).toLowerCase() : /^\d+[A-Z]+$/.test(w) ? w.toLowerCase() : w).join("");
  function merkModel(r) {
    if (!r) return "";
    const merk = String(r.merk || "").trim();
    let model = String(r.handelsbenaming || "").trim();
    if (merk && model.toUpperCase().startsWith(merk.toUpperCase() + " ")) model = model.slice(merk.length).trim();
    model = model.replace(/^(\d)ER REIHE$/i, "$1-serie");
    return [nice(merk), nice(model)].filter(Boolean).join(" ");
  }
  var niceLower = (s) => {
    const t = String(s || "").toLowerCase();
    return t.charAt(0).toUpperCase() + t.slice(1);
  };
  var pad = (n) => String(n).padStart(2, "0");
  var todayISO = () => {
    const d = /* @__PURE__ */ new Date();
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate());
  };
  var nowISO = () => (/* @__PURE__ */ new Date()).toISOString();
  var isISODate = (s) => typeof s === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s);
  var fmtDate = (iso) => isISODate(String(iso || "").slice(0, 10)) ? iso.slice(8, 10) + "-" + iso.slice(5, 7) + "-" + iso.slice(0, 4) : "–";
  var MAANDEN = ["jan", "feb", "mrt", "apr", "mei", "jun", "jul", "aug", "sep", "okt", "nov", "dec"];
  var fmtMonth = (iso) => iso && iso.length >= 7 ? MAANDEN[+iso.slice(5, 7) - 1] + " " + iso.slice(0, 4) : "";
  var fmtRange = (vanaf, tot) => vanaf && fmtMonth(vanaf) !== fmtMonth(tot) ? (vanaf.slice(0, 4) === (tot || "").slice(0, 4) ? fmtMonth(vanaf).split(" ")[0] : fmtMonth(vanaf)) + " – " + fmtMonth(tot) : fmtMonth(tot);
  function daysUntil(iso) {
    if (!isISODate(iso)) return null;
    const a = /* @__PURE__ */ new Date(iso + "T00:00:00");
    const b = /* @__PURE__ */ new Date(todayISO() + "T00:00:00");
    return Math.round((a - b) / 864e5);
  }
  function relTime(ts) {
    if (!ts) return "";
    const t = new Date(ts);
    if (isNaN(t)) return "";
    const time = pad(t.getHours()) + ":" + pad(t.getMinutes());
    const dayDiff2 = Math.round((/* @__PURE__ */ new Date(todayISO() + "T00:00:00") - /* @__PURE__ */ new Date(t.getFullYear() + "-" + pad(t.getMonth() + 1) + "-" + pad(t.getDate()) + "T00:00:00")) / 864e5);
    if (dayDiff2 === 0) return "vandaag " + time;
    if (dayDiff2 === 1) return "gisteren " + time;
    if (dayDiff2 > 1 && dayDiff2 < 7) return dayDiff2 + " dagen geleden";
    return fmtDate(t.getFullYear() + "-" + pad(t.getMonth() + 1) + "-" + pad(t.getDate()));
  }
  var yearOf = (iso) => iso && iso.length >= 4 ? iso.slice(0, 4) : "";
  var newId = (p) => p + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
  var normPlate = (s) => String(s || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  function fmtPlate(s) {
    const p = normPlate(s);
    if (p.length !== 6) return p;
    const runs = p.match(/[A-Z]+|[0-9]+/g) || [];
    if (runs.length === 3) return runs.join("-");
    if (runs.length === 2) {
      const [a, b] = runs;
      if (a.length === 4) return a.slice(0, 2) + "-" + a.slice(2) + "-" + b;
      if (b.length === 4) return a + "-" + b.slice(0, 2) + "-" + b.slice(2);
      return a + "-" + b;
    }
    if (runs.length === 1) return p.slice(0, 2) + "-" + p.slice(2, 4) + "-" + p.slice(4);
    return p;
  }
  var sum = (arr, f) => arr.reduce((a, x) => a + (Number(f(x)) || 0), 0);
  function groupBy(arr, key) {
    const m = {};
    for (const x of arr || []) {
      const k = typeof key === "function" ? key(x) : x[key];
      (m[k] || (m[k] = [])).push(x);
    }
    return m;
  }
  var byDateDesc = (a, b) => String(b.datum || "").localeCompare(String(a.datum || "")) || String(b.createdAt || "").localeCompare(String(a.createdAt || ""));
  function hostOf(url) {
    try {
      return new URL(url).hostname.replace(/^www\./, "");
    } catch (e) {
      return "";
    }
  }
  function cleanUrl(s) {
    const t = String(s || "").trim();
    if (!t) return "";
    if (/^https?:\/\//i.test(t)) return t;
    if (/^[\w-]+(\.[\w-]+)+(\/|$)/.test(t)) return "https://" + t;
    return t;
  }
  var isUrl = (s) => /^https?:\/\/[^\s]+\.[^\s]+/i.test(String(s || ""));
  var assetUrl = (id) => {
    if (!id) return "";
    if (!IS_WEB) return "/_blob/" + id;
    const local2 = typeof window !== "undefined" && window.__apBlobUrls && window.__apBlobUrls[id];
    return local2 || "_blob/" + id;
  };
  function loadImgEl(blob) {
    return new Promise((res, rej) => {
      const url = URL.createObjectURL(blob);
      const img = new Image();
      img.onload = () => res({ src: img, w: img.naturalWidth, h: img.naturalHeight, done: () => URL.revokeObjectURL(url) });
      img.onerror = () => {
        URL.revokeObjectURL(url);
        rej(new Error("decode"));
      };
      img.src = url;
    });
  }
  async function downscale(blob, maxDim, quality = 0.85) {
    let src;
    let w;
    let h;
    let done = () => {
    };
    try {
      const bmp = await createImageBitmap(blob, { imageOrientation: "from-image" });
      src = bmp;
      w = bmp.width;
      h = bmp.height;
      done = () => bmp.close && bmp.close();
    } catch (e) {
      const r = await loadImgEl(blob);
      src = r.src;
      w = r.w;
      h = r.h;
      done = r.done;
    }
    const s = Math.min(1, maxDim / Math.max(w, h));
    const cw = Math.max(1, Math.round(w * s));
    const ch = Math.max(1, Math.round(h * s));
    const c = document.createElement("canvas");
    c.width = cw;
    c.height = ch;
    const ctx = c.getContext("2d");
    ctx.fillStyle = "#fff";
    ctx.fillRect(0, 0, cw, ch);
    ctx.drawImage(src, 0, 0, cw, ch);
    done();
    const out = await new Promise((r) => c.toBlob(r, "image/jpeg", quality));
    if (!out) throw new Error("encode");
    return out;
  }
  var blobToDataUrl = (blob) => new Promise((res, rej) => {
    const fr = new FileReader();
    fr.onload = () => res(fr.result);
    fr.onerror = () => rej(fr.error);
    fr.readAsDataURL(blob);
  });
  var loaded = {};
  function loadOne(src) {
    if (!loaded[src]) {
      loaded[src] = new Promise((res, rej) => {
        const s = document.createElement("script");
        s.src = src;
        s.async = true;
        s.onload = () => res();
        s.onerror = () => {
          delete loaded[src];
          s.remove();
          rej(new Error("Kon " + src + " niet laden"));
        };
        (document.head || document.documentElement).appendChild(s);
      });
    }
    return loaded[src];
  }
  async function loadLib(name) {
    const srcs = LIBS[name];
    let err;
    for (const s of srcs) {
      try {
        await loadOne(s);
        return;
      } catch (e) {
        err = e;
      }
    }
    throw err;
  }
  var CDN = "https://cdnjs.cloudflare.com/ajax/libs/";
  var JSD = "https://cdn.jsdelivr.net/npm/";
  var own = (file, routes) => IS_WEB ? [file].concat(routes) : routes;
  var LIBS = {
    jspdf: own("jspdf.umd.min.js", [CDN + "jspdf/2.5.1/jspdf.umd.min.js", JSD + "jspdf@2.5.1/dist/jspdf.umd.min.js"]),
    autotable: own("jspdf.plugin.autotable.min.js", [CDN + "jspdf-autotable/3.8.2/jspdf.plugin.autotable.min.js", JSD + "jspdf-autotable@3.8.2/dist/jspdf.plugin.autotable.min.js"]),
    xlsx: own("xlsx.full.min.js", [CDN + "xlsx/0.18.5/xlsx.full.min.js", JSD + "xlsx@0.18.5/dist/xlsx.full.min.js"]),
    pdfjs: own("pdf.min.js", [CDN + "pdf.js/3.11.174/pdf.min.js", JSD + "pdfjs-dist@3.11.174/build/pdf.min.js"]),
    pdfjsWorker: own("pdf.worker.min.js", [CDN + "pdf.js/3.11.174/pdf.worker.min.js", JSD + "pdfjs-dist@3.11.174/build/pdf.worker.min.js"])
  };
  function safeLocal(key, value) {
    try {
      if (value === void 0) return window.localStorage.getItem(key);
      window.localStorage.setItem(key, value);
    } catch (e) {
      return null;
    }
    return null;
  }
  var plural = (n, one, many) => n + " " + (n === 1 ? one : many);
  var clip = (s, n) => {
    const t = String(s || "");
    return t.length > n ? t.slice(0, n - 1) + "…" : t;
  };

  // src/store.jsx
  var HELPER_SERVER = "Claude Code Remote";
  var HELPER_TOOL = "fire_trigger";
  var COLLS = ["cars", "invoices", "steps", "parts", "mods", "apkNotes", "alerts", "requests", "advice", "settings"];
  function useCaps() {
    const [caps, setCaps] = React.useState({ booted: false });
    React.useEffect(() => {
      const c = window.claude;
      if (!c || typeof c.use !== "function") {
        setCaps({ booted: true, none: true });
        return;
      }
      let alive = true;
      const set = (k, v) => alive && setCaps((p) => ({ ...p, [k]: v }));
      c.use("db").catch(() => null).then((db2) => alive && setCaps((p) => ({ ...p, db: db2, booted: true })));
      c.use("assets").catch(() => null).then((v) => set("assets", v));
      c.use("downloads").catch(() => null).then((v) => set("downloads", v));
      if (IS_NOAI) {
        set("helperTool", "unavailable");
        return () => {
          alive = false;
        };
      }
      c.use("sample").catch(() => null).then(async (s) => {
        let limits = null;
        if (s) limits = await s.limits().catch(() => null);
        set("sample", s);
        set("sampleLimits", limits);
      });
      c.use("mcp").catch(() => null).then(async (m) => {
        set("mcp", m);
        if (!m) return set("helperTool", "unavailable");
        try {
          const r = await m.listTools(HELPER_SERVER);
          const srv = (r.servers || []).find((s) => s.server === HELPER_SERVER);
          const ok = srv && (srv.tools || []).some((t) => t.name === HELPER_TOOL);
          set("helperTool", ok ? "ok" : "unavailable");
        } catch (e) {
          set("helperTool", "unavailable");
        }
      });
      return () => {
        alive = false;
      };
    }, []);
    return caps;
  }
  var DataCtx = React.createContext(null);
  var useData = () => React.useContext(DataCtx);
  var DB_COPY = {
    quota_exceeded: IS_WEB ? "De opslag op dit apparaat is vol. Maak ruimte vrij of verwijder oude foto’s en facturen." : "De opslag van de app is vol (maximaal 5.000 items). Verwijder oude onderdelen of facturen.",
    invalid_argument: "Dit kon niet worden opgeslagen: het item is te groot of onjuist.",
    resource_exhausted: "Even te veel wijzigingen tegelijk. Probeer het over een paar seconden opnieuw.",
    revoked: "De app heeft geen toegang meer tot je gegevens. Herlaad de pagina.",
    unavailable: IS_WEB ? "De opslag op dit apparaat reageert niet. Herlaad de app." : "Geen verbinding met de opslag. Probeer het zo opnieuw."
  };
  var dbErrorCopy = (e) => DB_COPY[e && e.code] || "Opslaan lukte niet. Probeer het opnieuw.";
  function DataProvider({ caps, toast, children }) {
    const [raw, setRaw] = React.useState(() => Object.fromEntries(COLLS.map((c) => [c, null])));
    const [dbError, setDbError] = React.useState(null);
    const [fire, setFire] = React.useState({ at: 0, ok: null, code: null, busy: false });
    const db2 = caps.db;
    React.useEffect(() => {
      if (!db2) return void 0;
      const unsubs = COLLS.map(
        (c) => db2.collection(c).onSnapshot(
          (snap) => {
            const rows = snap.docs.map((d) => Object.assign({ id: d.id }, d.data()));
            setRaw((p) => ({ ...p, [c]: rows }));
          },
          (err) => {
            setDbError(err);
            setRaw((p) => p[c] === null ? { ...p, [c]: [] } : p);
          }
        )
      );
      return () => unsubs.forEach((u) => u());
    }, [db2]);
    const settingsDoc = (id) => (raw.settings || []).find((s) => s.id === id) || null;
    const appSettings = settingsDoc("app");
    const helper = settingsDoc("helper");
    const aiEnabled = !IS_NOAI && !(appSettings && appSettings.ai === false);
    const ref = React.useRef({});
    ref.current = { raw, appSettings, caps, aiEnabled };
    const write2 = React.useCallback(
      async (fn, quiet) => {
        try {
          await fn();
          return true;
        } catch (e) {
          if (e && e.code === "unavailable") {
            await new Promise((r) => setTimeout(r, 600 + Math.random() * 600));
            try {
              await fn();
              return true;
            } catch (e2) {
              if (!quiet) toast(dbErrorCopy(e2), "error");
              return false;
            }
          }
          if (!quiet) toast(dbErrorCopy(e), "error");
          return false;
        }
      },
      [toast]
    );
    const fireTimer = React.useRef(null);
    const lastFire = React.useRef(0);
    const fireHelper = React.useCallback(async (manual) => {
      const { caps: cc, appSettings: st, aiEnabled: ae } = ref.current;
      if (!ae) {
        setFire({ at: Date.now(), ok: false, code: "ai_off", busy: false });
        return false;
      }
      const trig = st && st.helperTriggerId;
      if (!cc.mcp || !trig) {
        setFire({ at: Date.now(), ok: false, code: !trig ? "no_trigger" : "no_mcp", busy: false });
        return false;
      }
      lastFire.current = Date.now();
      setFire((f) => ({ ...f, busy: true }));
      const text2 = manual ? "Handmatig gestart vanuit de app: verwerk de open verzoeken en doe daarna de volledige dagelijkse ronde." : "Gestart vanuit de app: verwerk alleen de open verzoeken (collectie requests) en sla de dagelijkse ronde over.";
      try {
        await cc.mcp.callTool(HELPER_SERVER, HELPER_TOOL, { trigger_id: trig, text: text2 });
        setFire({ at: Date.now(), ok: true, code: null, busy: false });
        return true;
      } catch (e) {
        setFire({ at: Date.now(), ok: false, code: e && e.code || "upstream_error", busy: false });
        return false;
      }
    }, []);
    const scheduleFire = React.useCallback(() => {
      if (fireTimer.current) return;
      const since = Date.now() - lastFire.current;
      const wait = since > 6e4 ? 500 : 6e4 - since;
      fireTimer.current = setTimeout(async () => {
        fireTimer.current = null;
        const open = (ref.current.raw.requests || []).filter((r) => r.status === "open");
        if (open.length) await fireHelper(false);
      }, wait);
    }, [fireHelper]);
    const act = React.useMemo(() => {
      const col = (c) => db2.collection(c);
      const stamp = (o) => Object.assign({}, o, { updatedAt: nowISO() });
      const a = {
        put: (c, id, data, quiet) => write2(() => col(c).doc(id).set(stamp(data)), quiet),
        patch: (c, id, fields, quiet) => write2(() => col(c).doc(id).update(stamp(fields)), quiet),
        remove: (c, id, quiet) => write2(() => col(c).doc(id).delete(), quiet),
        async add(c, prefix, data, quiet) {
          const id = newId(prefix);
          const ok = await write2(() => col(c).doc(id).set(stamp(Object.assign({ createdAt: nowISO() }, data))), quiet);
          return ok ? id : null;
        },
        async queue(req, autoFire = true) {
          if (!ref.current.aiEnabled) return false;
          const id = newId("r");
          const ok = await write2(() => col("requests").doc(id).set(Object.assign({ status: "open", createdAt: nowISO(), bron: "app" }, req)), true);
          if (ok && autoFire) setTimeout(scheduleFire, 50);
          return ok;
        },
        async setting(key, value2) {
          const cur = ref.current.appSettings;
          if (cur) return write2(() => col("settings").doc("app").update({ [key]: value2 }));
          return write2(() => col("settings").doc("app").set({ [key]: value2 }));
        },
        async upload(blob, type) {
          const assets2 = ref.current.caps.assets;
          if (!assets2) throw { code: "no_assets" };
          return assets2.upload(blob, type ? { type } : void 0);
        },
        async deleteAsset(id) {
          const assets2 = ref.current.caps.assets;
          if (!assets2 || !id) return;
          try {
            await assets2.delete(id);
          } catch (e) {
          }
        },
        fireHelper
      };
      return a;
    }, [db2, write2, scheduleFire, fireHelper]);
    const value = React.useMemo(() => {
      const ready = COLLS.every((c) => raw[c] !== null);
      const cars = (raw.cars || []).slice().sort((a, b) => (a.order || 0) - (b.order || 0) || String(a.createdAt || "").localeCompare(String(b.createdAt || "")));
      const invoices = (raw.invoices || []).slice().sort(byDateDesc);
      const steps = (raw.steps || []).slice().sort(byDateDesc);
      const parts = (raw.parts || []).slice().sort((a, b) => String(a.createdAt || "").localeCompare(String(b.createdAt || "")));
      const mods = (raw.mods || []).slice().sort(byDateDesc);
      const apkNotes = (raw.apkNotes || []).slice().sort(byDateDesc);
      const alerts = (raw.alerts || []).slice().sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
      const requests = (raw.requests || []).slice().sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
      const advice = Object.fromEntries((raw.advice || []).map((d) => [d.id, d]));
      const inv = groupBy(invoices.filter((i) => !i.voorstel), (i) => i.carId || "_none");
      const prt = groupBy(parts, "carId");
      const stp = groupBy(steps, "carId");
      const mds = groupBy(mods, "carId");
      const apn = groupBy(apkNotes, "carId");
      const totals = {};
      for (const c of cars) totals[c.id] = sum(inv[c.id] || [], (i) => i.totaal);
      const thisYear = todayISO().slice(0, 4);
      return {
        ready,
        dbError,
        cars,
        active: cars.filter((c) => c.status !== "verkocht"),
        previous: cars.filter((c) => c.status === "verkocht"),
        car: (id) => cars.find((c) => c.id === id) || null,
        invoices,
        suggestions: invoices.filter((i) => i.voorstel),
        invoicesOf: (id) => inv[id || "_none"] || [],
        partsOf: (id) => prt[id] || [],
        stepsOf: (id) => stp[id] || [],
        modsOf: (id) => mds[id] || [],
        apkNotesOf: (id) => apn[id] || [],
        adviceOf: (id) => advice[id] || null,
        parts,
        steps,
        mods,
        apkNotes,
        alerts,
        openAlerts: alerts.filter((x) => !x.gelezen),
        requests,
        openRequests: requests.filter((r) => r.status === "open" || r.status === "bezig"),
        settings: Object.assign({ driveSync: false, gmailImport: false, rdwBijwerken: true, prijzenVolgen: true, bijTwijfelVragen: true }, appSettings || {}, aiEnabled ? {} : { driveSync: false, gmailImport: false, rdwBijwerken: RDW_DIRECT ? !(appSettings && appSettings.rdwBijwerken === false) : false, prijzenVolgen: false }),
        aiEnabled,
        helper,
        totals,
        totalAll: sum(invoices.filter((i) => !i.voorstel), (i) => i.totaal),
        thisYear,
        fire,
        act,
        caps
      };
    }, [raw, dbError, appSettings, helper, fire, act, caps, aiEnabled]);
    return /* @__PURE__ */ React.createElement(DataCtx.Provider, { value }, children);
  }
  function planStats(parts, budget) {
    const total = (p) => typeof p.prijs === "number" ? p.prijs * (p.aantal || 1) : 0;
    const open = parts.filter((p) => p.status !== "binnen");
    const binnen = parts.filter((p) => p.status === "binnen");
    const moet = sum(open.filter((p) => p.prioriteit !== "wil"), total);
    const wil = sum(open.filter((p) => p.prioriteit === "wil"), total);
    const binnenSom = sum(binnen, total);
    const onbekend = open.filter((p) => typeof p.prijs !== "number").length;
    const plan = moet + wil + binnenSom;
    return {
      count: parts.length,
      open: open.length,
      binnen: binnen.length,
      nodig: moet + wil,
      moet,
      wil,
      binnenSom,
      onbekend,
      plan,
      budget: typeof budget === "number" ? budget : null,
      ruimte: typeof budget === "number" ? budget - plan : null,
      pct: parts.length ? binnen.length / parts.length : 0,
      deals: parts.filter((p) => p.deal && p.status !== "binnen")
    };
  }
  function rdwState(car, linked = true, aiEnabled = true) {
    const r = car && car.rdw;
    if (car && !r && car.apkTot) return apkOnly(car.apkTot);
    if (!car || !car.kenteken) return { tone: "muted", text: "Geen kenteken" };
    if (!r && !aiEnabled && !RDW_DIRECT) return { tone: "muted", text: "APK-datum nog niet ingevuld" };
    if (car.rdwStatus === "wachten" && !r) return linked || RDW_DIRECT ? { tone: "info", text: "RDW-gegevens worden opgehaald" } : { tone: "muted", text: "RDW-gegevens wachten op de assistent" };
    if (car.rdwStatus === "niet-gevonden") return { tone: "muted", text: "Kenteken niet gevonden bij de RDW" };
    if (car.rdwStatus === "fout" && !r) return { tone: "muted", text: RDW_DIRECT ? "RDW niet bereikbaar, probeer het later" : "RDW-gegevens nog niet opgehaald" };
    if (!r) return { tone: "muted", text: "Nog geen RDW-gegevens" };
    const apk = r.vervaldatumApk;
    const verz = r.wamVerzekerd;
    const parts = [];
    let tone = "ok";
    if (apk) {
      const d = dayDiff(apk);
      if (d < 0) {
        parts.push("APK verlopen");
        tone = "danger";
      } else if (d <= 60) {
        parts.push("APK verloopt " + fmtShort(apk));
        tone = "warn";
      } else parts.push("APK tot " + fmtShort(apk));
    }
    if (verz === false) {
      parts.push("niet verzekerd");
      tone = "danger";
    } else if (verz === true) parts.push("verzekerd");
    if (!parts.length) return { tone: "muted", text: "RDW-gegevens bijgewerkt" };
    return { tone, text: parts.join(" · ") };
  }
  function apkOnly(iso) {
    const d = dayDiff(iso);
    if (d < 0) return { tone: "danger", text: "APK verlopen" };
    if (d <= 60) return { tone: "warn", text: "APK verloopt " + fmtShort(iso) };
    return { tone: "ok", text: "APK tot " + fmtShort(iso) };
  }
  function dayDiff(iso) {
    const a = /* @__PURE__ */ new Date(iso + "T00:00:00");
    const b = /* @__PURE__ */ new Date(todayISO() + "T00:00:00");
    return Math.round((a - b) / 864e5);
  }
  var fmtShort = (iso) => iso.slice(8, 10) + "-" + iso.slice(5, 7) + "-" + iso.slice(0, 4);

  // src/ctx.js
  var UiCtx = React.createContext(null);
  var useUi = () => React.useContext(UiCtx);
  var TABS = [
    ["overzicht", "Overzicht"],
    ["gegevens", "Gegevens"],
    ["kosten", "Kosten"],
    ["bouwlog", "Bouwlog"],
    ["plannen", "Plannen"],
    ["advies", "Advies"]
  ];
  var TAB_IDS = TABS.map((t) => t[0]);
  function parseHash(h) {
    const s = String(h || "").replace(/^#/, "");
    if (!s || s === "garage") return { name: "garage" };
    if (s === "plannen" || s === "kosten" || s === "meer") return { name: s };
    const m = s.match(/^auto-([a-z0-9]+?)(?:-([a-z]+))?$/);
    if (m && (!m[2] || TAB_IDS.includes(m[2]))) return { name: "car", carId: m[1], tab: m[2] || "overzicht" };
    return { name: "garage" };
  }
  var routeHash = (r) => r.name === "car" ? "#auto-" + r.carId + (r.tab && r.tab !== "overzicht" ? "-" + r.tab : "") : "#" + r.name;

  // src/ui.jsx
  var P = {
    plus: ["M12 5v14", "M5 12h14"],
    back: ["M15 5l-7 7 7 7"],
    next: ["M9 5l7 7-7 7"],
    down: ["M6 9l6 6 6-6"],
    close: ["M6 6l12 12", "M18 6 6 18"],
    check: ["m5 12.5 4.5 4.5L19 7.5"],
    home: ["M3 10.5 12 4l9 6.5V20a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1z", "M7.5 21v-7h9v7", "M7.5 17.5h9"],
    list: ["M9 4V3h6v1", "M9 10h6", "M9 14h6", "M9 18h3", { rect: [5, 4, 14, 17, 2] }],
    chart: ["M4 20h16", "M7 16v-5", "M12 16V6", "M17 16V9"],
    dots: [{ dot: [5.5, 12] }, { dot: [12, 12] }, { dot: [18.5, 12] }],
    camera: ["M4 8h3l2-3h6l2 3h3a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V9a1 1 0 0 1 1-1z", { circle: [12, 13.5, 3.5] }],
    image: [{ rect: [3.5, 5, 17, 14, 2] }, "m4 16 4.5-4.5 4 4 2.5-2.5 5 5", { circle: [15.5, 9.5, 1.5] }],
    file: ["M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z", "M14 3v5h5", "M9 13h6", "M9 17h4"],
    alert: ["M12 4 2.5 20h19z", "M12 10v4.5", "M12 17.5v.01"],
    okCircle: [{ circle: [12, 12, 9] }, "m8 12.5 2.7 2.7L16 10"],
    info: [{ circle: [12, 12, 9] }, "M12 11v5", "M12 8v.01"],
    link: ["M10 14a4 4 0 0 0 5.7 0l3-3a4 4 0 0 0-5.7-5.7l-1 1", "M14 10a4 4 0 0 0-5.7 0l-3 3a4 4 0 0 0 5.7 5.7l1-1"],
    out: ["M14 4h6v6", "M20 4l-9 9", "M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5"],
    trash: ["M4 7h16", "M10 11v6", "M14 11v6", "M6 7l1 13h10l1-13", "M9 7V4h6v3"],
    edit: ["M4 20h4L19 9l-4-4L4 16z", "M13.5 6.5l4 4"],
    download: ["M12 4v11", "m7 10 5 5 5-5", "M5 20h14"],
    refresh: ["M20 11a8 8 0 0 0-14.3-4.6L4 8", "M4 4v4h4", "M4 13a8 8 0 0 0 14.3 4.6L20 16", "M20 20v-4h-4"],
    spark: ["M12 3v4", "M12 17v4", "M3 12h4", "M17 12h4", "m6 6 2.5 2.5", "m15.5 15.5 2.5 2.5", "m6 18 2.5-2.5", "m15.5 8.5 2.5-2.5"],
    wrench: ["M14.5 6.5a4 4 0 0 0 5 5L12 19a2.1 2.1 0 0 1-3-3z", "M14.5 6.5 17 4l3 3-2.5 2.5"],
    tag: ["M3 12V4h8l9 9-8 8z", { circle: [7.5, 8.5, 1.3] }],
    flag: ["M5 21V4", "M5 4h11l-2 4 2 4H5"],
    euro: ["M17 6.5A6.5 6.5 0 1 0 17 17.5", "M4 10h9", "M4 14h8"],
    doc: ["M7 3h7l5 5v13H7z", "M14 3v5h5"],
    cloud: ["M7 18a4 4 0 0 1-.5-8A6 6 0 0 1 18 9a4.5 4.5 0 0 1-.5 9z"],
    mail: [{ rect: [3, 5, 18, 14, 2] }, "m4 7 8 6 8-6"],
    gauge: ["M4 15a8 8 0 1 1 16 0", "M12 15l4-5"],
    bolt: ["M13 3 5 13h6l-1 8 8-10h-6z"],
    clock: [{ circle: [12, 12, 9] }, "M12 7v5l3 2"],
    minus: ["M5 12h14"],
    search: [{ circle: [11, 11, 6.5] }, "m16 16 4 4"],
    share: ["M12 3v12", "m8 7 4-4 4 4", "M7 10H6a1 1 0 0 0-1 1v9a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-9a1 1 0 0 0-1-1h-1"],
    upload: ["M12 20V9", "m7 14 5-5 5 5", "M5 4h14"],
    phone: [{ rect: [7, 2.5, 10, 19, 2.5] }, "M11 18.5h2"]
  };
  function Icon({ name, size = 20, stroke = 2, className, style }) {
    const parts = P[name] || [];
    return /* @__PURE__ */ React.createElement("svg", { "aria-hidden": "true", focusable: "false", width: size, height: size, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: stroke, strokeLinecap: "round", strokeLinejoin: "round", className, style }, parts.map((p, i) => {
      if (typeof p === "string") return /* @__PURE__ */ React.createElement("path", { key: i, d: p });
      if (p.rect) return /* @__PURE__ */ React.createElement("rect", { key: i, x: p.rect[0], y: p.rect[1], width: p.rect[2], height: p.rect[3], rx: p.rect[4] });
      if (p.circle) return /* @__PURE__ */ React.createElement("circle", { key: i, cx: p.circle[0], cy: p.circle[1], r: p.circle[2] });
      if (p.dot) return /* @__PURE__ */ React.createElement("circle", { key: i, cx: p.dot[0], cy: p.dot[1], r: "1.4", fill: "currentColor", stroke: "none" });
      return null;
    }));
  }
  function CarShape({ width = 132, stroke = 1.6, detail = true }) {
    return /* @__PURE__ */ React.createElement("svg", { "aria-hidden": "true", focusable: "false", width, height: width * 48 / 120, viewBox: "0 0 120 48", fill: "none", stroke: "currentColor", strokeWidth: stroke, strokeLinecap: "round", strokeLinejoin: "round" }, /* @__PURE__ */ React.createElement("path", { d: "M6 34V28q0-3 3-3.5L18 23l15-9.5q2-1.5 5-1.5h23q3 0 5 1.5L79 22l25 2.5q8 1 9.5 4.5l.5 5h-13a9 9 0 0 0-18 0H39a9 9 0 0 0-18 0z" }), detail && /* @__PURE__ */ React.createElement("path", { d: "M27 22.5 37 15.5h23.5L70 22z" }), detail && /* @__PURE__ */ React.createElement("path", { d: "M49 15.5v7" }), detail && /* @__PURE__ */ React.createElement("circle", { cx: "30", cy: "34", r: "6.5" }), detail && /* @__PURE__ */ React.createElement("circle", { cx: "92", cy: "34", r: "6.5" }));
  }
  function Plate({ kenteken, size = "md" }) {
    if (!kenteken) return null;
    return /* @__PURE__ */ React.createElement("span", { className: "plate plate-" + size }, /* @__PURE__ */ React.createElement("span", { className: "plate-nl", "aria-hidden": "true" }, size === "lg" || size === "md" ? "NL" : ""), /* @__PURE__ */ React.createElement("span", { className: "plate-txt" }, fmtPlate(kenteken)));
  }
  function CarThumb({ car, size = 48 }) {
    const [bad2, setBad] = React.useState(false);
    const r = Math.round(size / 4);
    if (car && car.foto && !bad2) {
      return /* @__PURE__ */ React.createElement("img", { className: "thumb", src: assetUrl(car.foto), alt: "", width: size, height: size, style: { width: size, height: size, borderRadius: r }, onError: () => setBad(true) });
    }
    return /* @__PURE__ */ React.createElement("span", { className: "thumb thumb-empty", "aria-hidden": "true", style: { width: size, height: size, borderRadius: r } }, /* @__PURE__ */ React.createElement(CarShape, { width: Math.round(size * 0.75), stroke: 4, detail: false }));
  }
  function Photo({ id, alt, className, onError }) {
    const [bad2, setBad] = React.useState(false);
    if (!id || bad2) return null;
    return /* @__PURE__ */ React.createElement(
      "img",
      {
        className,
        src: assetUrl(id),
        alt: alt || "",
        onError: () => {
          setBad(true);
          onError && onError();
        }
      }
    );
  }
  function Btn({ kind = "secondary", icon, children, className = "", block, small, busy, ...rest }) {
    return /* @__PURE__ */ React.createElement("button", { type: "button", className: "btn btn-" + kind + (block ? " btn-block" : "") + (small ? " btn-small" : "") + (busy ? " is-busy" : "") + " " + className, disabled: busy || rest.disabled, ...rest }, busy ? /* @__PURE__ */ React.createElement(Spinner, null) : icon ? /* @__PURE__ */ React.createElement(Icon, { name: icon, size: small ? 16 : 18, stroke: 2.2 }) : null, children && /* @__PURE__ */ React.createElement("span", null, children));
  }
  function IconBtn({ icon, label, onClick, className = "", size = 22 }) {
    return /* @__PURE__ */ React.createElement("button", { type: "button", className: "icon-btn " + className, "aria-label": label, title: label, onClick }, /* @__PURE__ */ React.createElement(Icon, { name: icon, size }));
  }
  var Spinner = ({ size = 16 }) => /* @__PURE__ */ React.createElement("span", { className: "spinner", style: { width: size, height: size }, "aria-hidden": "true" });
  function Sheet({ title, onClose, left, right, children, footer, size }) {
    const ref = React.useRef(null);
    React.useEffect(() => {
      const prev = document.activeElement;
      const el = ref.current;
      if (el) {
        const first = el.querySelector("[data-autofocus]") || el.querySelector(".sheet-head button, .sheet-body input, .sheet-body textarea, .sheet-body button");
        if (first && window.matchMedia("(min-width: 960px)").matches) first.focus({ preventScroll: true });
      }
      const onKey = (e) => {
        if (e.key === "Escape") onClose && onClose();
      };
      window.addEventListener("keydown", onKey);
      return () => {
        window.removeEventListener("keydown", onKey);
        if (prev && prev.focus) prev.focus({ preventScroll: true });
      };
    }, []);
    return /* @__PURE__ */ React.createElement("div", { className: "sheet-wrap", role: "presentation", onMouseDown: (e) => e.target === e.currentTarget && onClose && onClose() }, /* @__PURE__ */ React.createElement("div", { className: "sheet" + (size === "wide" ? " sheet-wide" : ""), role: "dialog", "aria-modal": "true", "aria-label": title, ref }, /* @__PURE__ */ React.createElement("div", { className: "sheet-head" }, /* @__PURE__ */ React.createElement("div", { className: "sheet-head-side" }, left !== void 0 ? left : /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: onClose }, "Annuleer")), /* @__PURE__ */ React.createElement("h2", { className: "sheet-title" }, title), /* @__PURE__ */ React.createElement("div", { className: "sheet-head-side sheet-head-right" }, right)), /* @__PURE__ */ React.createElement("div", { className: "sheet-body" }, children), footer && /* @__PURE__ */ React.createElement("div", { className: "sheet-foot" }, footer)));
  }
  function Field({ label, hint, children, id, inline }) {
    return /* @__PURE__ */ React.createElement("div", { className: "field" + (inline ? " field-inline" : "") }, label && /* @__PURE__ */ React.createElement("label", { className: "field-label", htmlFor: id }, label), children, hint && /* @__PURE__ */ React.createElement("p", { className: "field-hint" }, hint));
  }
  function Seg({ options, value, onChange, label, small }) {
    return /* @__PURE__ */ React.createElement("div", { role: "group", "aria-label": label, className: "seg" + (small ? " seg-small" : ""), style: { gridTemplateColumns: "repeat(" + options.length + ", minmax(0, 1fr))" } }, options.map(([v, l]) => /* @__PURE__ */ React.createElement("button", { key: String(v), type: "button", "aria-pressed": value === v, className: "seg-btn" + (value === v ? " is-on" : ""), onClick: () => onChange(v) }, l)));
  }
  function Switch({ checked, onChange, label, sub, id, disabled }) {
    return /* @__PURE__ */ React.createElement("label", { className: "switch-row" + (disabled ? " is-disabled" : ""), htmlFor: id }, /* @__PURE__ */ React.createElement("span", { className: "switch-text" }, /* @__PURE__ */ React.createElement("span", { className: "switch-label" }, label), sub && /* @__PURE__ */ React.createElement("span", { className: "switch-sub" }, sub)), /* @__PURE__ */ React.createElement("input", { id, type: "checkbox", role: "switch", className: "switch-input", checked: !!checked, disabled, onChange: (e) => onChange(e.target.checked) }), /* @__PURE__ */ React.createElement("span", { className: "switch", "aria-hidden": "true" }));
  }
  function Empty({ title, text: text2, action, art = "car" }) {
    return /* @__PURE__ */ React.createElement("div", { className: "empty" }, /* @__PURE__ */ React.createElement("div", { className: "empty-art", "aria-hidden": "true" }, art === "car" ? /* @__PURE__ */ React.createElement(CarShape, { width: 120 }) : /* @__PURE__ */ React.createElement(Icon, { name: art, size: 40, stroke: 1.6 })), /* @__PURE__ */ React.createElement("h3", { className: "empty-title" }, title), text2 && /* @__PURE__ */ React.createElement("p", { className: "empty-text" }, text2), action);
  }
  function Stat({ label, value, sub, onClick, tone }) {
    const Tag = onClick ? "button" : "div";
    return /* @__PURE__ */ React.createElement(Tag, { type: onClick ? "button" : void 0, className: "stat" + (onClick ? " stat-btn" : ""), onClick }, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, label), /* @__PURE__ */ React.createElement("span", { className: "stat-value" + (tone ? " tone-" + tone : "") }, value), sub && /* @__PURE__ */ React.createElement("span", { className: "stat-sub" }, sub));
  }
  function StatusLine({ tone, text: text2 }) {
    const icon = tone === "danger" || tone === "warn" ? "alert" : tone === "ok" ? "okCircle" : tone === "info" ? "clock" : "info";
    return /* @__PURE__ */ React.createElement("span", { className: "status-line tone-" + tone }, /* @__PURE__ */ React.createElement(Icon, { name: icon, size: 15, stroke: 2.2 }), text2);
  }
  var ToastCtx = React.createContext(() => {
  });
  function ToastHost({ children }) {
    const [items, setItems] = React.useState([]);
    const push = React.useCallback((text2, tone = "ok") => {
      const id = Math.random().toString(36).slice(2);
      setItems((x) => x.concat({ id, text: text2, tone }).slice(-3));
      setTimeout(() => setItems((x) => x.filter((t) => t.id !== id)), tone === "error" ? 6500 : 3200);
    }, []);
    return /* @__PURE__ */ React.createElement(ToastCtx.Provider, { value: push }, children, /* @__PURE__ */ React.createElement("div", { className: "toasts", role: "status", "aria-live": "polite" }, items.map((t) => /* @__PURE__ */ React.createElement("div", { key: t.id, className: "toast toast-" + t.tone }, t.text))));
  }
  var useToast = () => React.useContext(ToastCtx);
  function Section({ title, right, children, className = "" }) {
    return /* @__PURE__ */ React.createElement("section", { className: "section " + className }, (title || right) && /* @__PURE__ */ React.createElement("div", { className: "section-head" }, title && /* @__PURE__ */ React.createElement("h3", { className: "section-title" }, title), right), children);
  }
  function Bar({ pct, tone }) {
    const p = Math.max(0, Math.min(1, pct || 0));
    return /* @__PURE__ */ React.createElement("span", { className: "bar", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement("span", { className: "bar-fill" + (tone ? " tone-bg-" + tone : ""), style: { width: (p * 100).toFixed(1) + "%" } }));
  }
  function useMedia(q) {
    const [m, setM] = React.useState(() => window.matchMedia(q).matches);
    React.useEffect(() => {
      const mq = window.matchMedia(q);
      const f = () => setM(mq.matches);
      mq.addEventListener ? mq.addEventListener("change", f) : mq.addListener(f);
      f();
      return () => mq.removeEventListener ? mq.removeEventListener("change", f) : mq.removeListener(f);
    }, [q]);
    return m;
  }
  function FilePick({ accept, capture, onFile, children, className = "", multiple, id }) {
    return /* @__PURE__ */ React.createElement("label", { className, htmlFor: id }, /* @__PURE__ */ React.createElement(
      "input",
      {
        id,
        type: "file",
        accept,
        capture,
        multiple,
        className: "visually-hidden",
        onChange: (e) => {
          const files = Array.from(e.target.files || []);
          e.target.value = "";
          if (files.length) onFile(multiple ? files : files[0]);
        }
      }
    ), children);
  }

  // src/exporters.js
  async function pdfjs() {
    if (!window.pdfjsLib) {
      await loadLib("pdfjsWorker");
      await loadLib("pdfjs");
    }
    const lib = window.pdfjsLib;
    if (!lib) throw new Error("pdf.js ontbreekt");
    return lib;
  }
  async function pdfToImages(blob, { maxPages = 3, width = 1600 } = {}) {
    const lib = await pdfjs();
    const data = new Uint8Array(await blob.arrayBuffer());
    const doc = await lib.getDocument({ data, isEvalSupported: false }).promise;
    const n = Math.min(doc.numPages, maxPages);
    const images = [];
    let text2 = "";
    for (let i = 1; i <= n; i++) {
      const page = await doc.getPage(i);
      const base = page.getViewport({ scale: 1 });
      const scale = Math.min(3, width / base.width);
      const vp = page.getViewport({ scale });
      const canvas = document.createElement("canvas");
      canvas.width = Math.round(vp.width);
      canvas.height = Math.round(vp.height);
      const ctx = canvas.getContext("2d");
      ctx.fillStyle = "#fff";
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      await page.render({ canvasContext: ctx, viewport: vp }).promise;
      images.push(await new Promise((r) => canvas.toBlob(r, "image/jpeg", 0.85)));
      try {
        const tc = await page.getTextContent();
        text2 += tc.items.map((it) => it.str + (it.hasEOL ? "\n" : " ")).join("") + "\n";
      } catch (e) {
      }
    }
    return { images: images.filter(Boolean), text: text2.trim(), pages: doc.numPages };
  }
  async function renderPdfPages(bytes, width) {
    const lib = await pdfjs();
    const doc = await lib.getDocument({ data: new Uint8Array(bytes), isEvalSupported: false }).promise;
    const urls = [];
    for (let i = 1; i <= doc.numPages; i++) {
      const page = await doc.getPage(i);
      const base = page.getViewport({ scale: 1 });
      const vp = page.getViewport({ scale: width * (window.devicePixelRatio || 1) / base.width });
      const canvas = document.createElement("canvas");
      canvas.width = Math.round(vp.width);
      canvas.height = Math.round(vp.height);
      const ctx = canvas.getContext("2d");
      await page.render({ canvasContext: ctx, viewport: vp }).promise;
      urls.push(canvas.toDataURL("image/jpeg", 0.9));
    }
    return urls;
  }
  async function buildWorkbook(d) {
    await loadLib("xlsx");
    const X = window.XLSX;
    const wb = X.utils.book_new();
    const money = "€ #,##0.00";
    const dateOf = (iso) => iso && /^\d{4}-\d{2}-\d{2}$/.test(iso) ? /* @__PURE__ */ new Date(iso + "T12:00:00") : iso || "";
    const sheetName = (s, used2) => {
      let n = String(s || "Auto").replace(/[\\/?*[\]:]/g, " ").slice(0, 28).trim() || "Auto";
      let k = n;
      let i = 2;
      while (used2.has(k.toLowerCase())) k = n.slice(0, 25) + " " + i++;
      used2.add(k.toLowerCase());
      return k;
    };
    const used = /* @__PURE__ */ new Set();
    const fmtCol = (ws, col, rows, fmt) => {
      for (let r = 1; r <= rows; r++) {
        const ref = X.utils.encode_cell({ r, c: col });
        if (ws[ref] && typeof ws[ref].v === "number") ws[ref].z = fmt;
      }
    };
    const overview = [["Auto", "Kenteken", "Status", "Facturen", "Totaal", "Dit jaar", "Nog nodig (plannen)"]];
    for (const c of d.cars) {
      const inv = d.invoicesOf(c.id);
      const ps = planStats(d.partsOf(c.id), c.budget);
      overview.push([
        c.naam || "Auto",
        c.kenteken ? fmtPlate(c.kenteken) : "",
        c.status === "verkocht" ? "Eerder gehad" : gebruikLabel(c.gebruik) || "In bezit",
        inv.length,
        Math.round(sum(inv, (i) => i.totaal) * 100) / 100,
        Math.round(sum(inv.filter((i) => (i.datum || "").startsWith(d.thisYear)), (i) => i.totaal) * 100) / 100,
        Math.round(ps.nodig * 100) / 100
      ]);
    }
    const none = d.invoicesOf(null);
    if (none.length) overview.push(["Niet aan een auto gekoppeld", "", "", none.length, Math.round(sum(none, (i) => i.totaal) * 100) / 100, "", ""]);
    overview.push(["Totaal", "", "", d.invoices.filter((i) => !i.voorstel).length, Math.round(d.totalAll * 100) / 100, "", ""]);
    const ws0 = X.utils.aoa_to_sheet(overview);
    ws0["!cols"] = [{ wch: 30 }, { wch: 11 }, { wch: 14 }, { wch: 9 }, { wch: 14 }, { wch: 12 }, { wch: 18 }];
    [4, 5, 6].forEach((c) => fmtCol(ws0, c, overview.length, money));
    X.utils.book_append_sheet(wb, ws0, sheetName("Overzicht", used));
    const invRows = (list) => {
      const rows = [["Datum", "Leverancier", "Omschrijving", "Categorie", "Bedrag", "Factuurnummer", "Bron", "Toegewezen"]];
      for (const i of list) {
        rows.push([
          dateOf(i.datum),
          i.leverancier || "",
          i.omschrijving || "",
          catLabel(i.categorie),
          typeof i.totaal === "number" ? i.totaal : "",
          i.factuurnummer || "",
          i.bron === "scan" ? "Scan" : i.bron === "gmail" ? "Mail" : i.bron === "import" ? "Import" : "Handmatig",
          i.ai && i.ai.gekozen === "assistent" ? "Assistent (" + i.ai.zekerheid + ")" : "Zelf"
        ]);
      }
      rows.push(["", "", "", "Totaal", Math.round(sum(list, (i) => i.totaal) * 100) / 100, "", "", ""]);
      const ws = X.utils.aoa_to_sheet(rows, { cellDates: true });
      ws["!cols"] = [{ wch: 11 }, { wch: 26 }, { wch: 36 }, { wch: 24 }, { wch: 12 }, { wch: 16 }, { wch: 10 }, { wch: 22 }];
      fmtCol(ws, 4, rows.length, money);
      for (let r = 1; r < rows.length; r++) {
        const ref = X.utils.encode_cell({ r, c: 0 });
        if (ws[ref] && ws[ref].t === "d") ws[ref].z = "dd-mm-yyyy";
      }
      return ws;
    };
    for (const c of d.cars) X.utils.book_append_sheet(wb, invRows(d.invoicesOf(c.id)), sheetName(c.naam, used));
    if (none.length) X.utils.book_append_sheet(wb, invRows(none), sheetName("Niet gekoppeld", used));
    const plan = [["Auto", "Categorie", "Onderdeel", "Nodig", "Status", "Prijs", "Aantal", "Winkel", "Link"]];
    for (const c of d.cars) {
      for (const p of d.partsOf(c.id)) {
        plan.push([c.naam || "", catLabel(p.categorie), p.naam || "", p.prioriteit === "wil" ? "Wil" : "Moet", p.status === "binnen" ? "Gekocht" : p.status === "besteld" ? "Gekocht, onderweg" : "Nog kopen", typeof p.prijs === "number" ? p.prijs : "", p.aantal || 1, p.winkel || "", p.link || ""]);
      }
    }
    const wsP = X.utils.aoa_to_sheet(plan);
    wsP["!cols"] = [{ wch: 22 }, { wch: 22 }, { wch: 34 }, { wch: 7 }, { wch: 10 }, { wch: 12 }, { wch: 7 }, { wch: 18 }, { wch: 40 }];
    fmtCol(wsP, 5, plan.length, money);
    X.utils.book_append_sheet(wb, wsP, sheetName("Plannen", used));
    return new Uint8Array(X.write(wb, { type: "array", bookType: "xlsx" }));
  }
  var INK = [21, 23, 26];
  var INK2 = [74, 80, 88];
  var LABEL = [95, 101, 109];
  var RULE = [201, 205, 210];
  var RULE2 = [225, 228, 232];
  var ACCENT = [180, 81, 12];
  var SALE_SECTIONS = [
    ["foto", "Foto", "Grote foto bovenaan"],
    ["tekst", "Verkooptekst", "Korte tekst over de auto"],
    ["gegevens", "Gegevens en prestaties", "RDW en fabriek tegenover nu"],
    ["aanpassingen", "Aanpassingen", "Wat er aan de auto veranderd is"],
    ["kosten", "Onderhoud en kosten", "Totaal per categorie"],
    ["facturen", "Facturenlijst", "Alle facturen op datum"],
    ["apk", "APK-historie", "Keuringen en gebreken"],
    ["bouw", "Bouwproces", "Mijlpalen en stappen"]
  ];
  var defaultSections = () => ({ foto: true, tekst: true, gegevens: true, aanpassingen: true, kosten: true, facturen: false, apk: true, bouw: true });
  var pdfSafe = (s) => String(s == null ? "" : s).replace(/[‘’]/g, "'").replace(/[“”]/g, '"').replace(/[–—]/g, "-").replace(/…/g, "...").replace(/ | /g, " ").replace(/[^\x09\x0A\x0D\x20-\x7E -ÿ€]/g, "");
  async function buildSalesPdf(d, car, opts) {
    await loadLib("jspdf");
    await loadLib("autotable");
    const { jsPDF } = window.jspdf;
    const doc = new jsPDF({ unit: "mm", format: "a4", compress: true });
    if (typeof doc.autoTable !== "function" && window.jspdf_autotable && window.jspdf_autotable.applyPlugin) window.jspdf_autotable.applyPlugin(jsPDF);
    const S = Object.assign(defaultSections(), opts.secties || {});
    const W = 210;
    const M = 16;
    const CW = W - 2 * M;
    let y = M;
    const r = car.rdw || {};
    const inv = d.invoicesOf(car.id);
    const steps = d.stepsOf(car.id);
    const mods = d.modsOf(car.id);
    const plate = car.kenteken ? fmtPlate(car.kenteken) : "";
    const ensure = (h) => {
      if (y + h > 297 - 18) {
        doc.addPage();
        y = M;
      }
    };
    const heading = (t) => {
      ensure(16);
      y += 4;
      doc.setFont("helvetica", "bold");
      doc.setFontSize(9);
      doc.setTextColor(...ACCENT);
      doc.text(pdfSafe(t.toUpperCase()), M, y, { charSpace: 0.4 });
      y += 2;
      doc.setDrawColor(...RULE);
      doc.setLineWidth(0.3);
      doc.line(M, y, W - M, y);
      y += 5;
    };
    const para = (t, size = 10.5, color = INK) => {
      doc.setFont("helvetica", "normal");
      doc.setFontSize(size);
      doc.setTextColor(...color);
      const lines = doc.splitTextToSize(pdfSafe(t), CW);
      for (const ln of lines) {
        ensure(5.2);
        doc.text(ln, M, y);
        y += size * 0.46;
      }
      y += 1.5;
    };
    const table = (head, body, colStyles) => {
      doc.autoTable({
        startY: y,
        head: head ? [head.map(pdfSafe)] : void 0,
        body: body.map((row) => row.map(pdfSafe)),
        margin: { left: M, right: M, bottom: 18 },
        theme: "plain",
        styles: { font: "helvetica", fontSize: 9.5, textColor: INK, cellPadding: { top: 1.6, bottom: 1.6, left: 1.2, right: 1.2 }, lineColor: RULE2, lineWidth: { bottom: 0.2 } },
        headStyles: { fontStyle: "bold", textColor: LABEL, fontSize: 8.5, lineWidth: { bottom: 0.3 }, lineColor: RULE },
        columnStyles: colStyles || {}
      });
      y = doc.lastAutoTable.finalY + 4;
    };
    doc.setFont("helvetica", "bold");
    doc.setFontSize(24);
    doc.setTextColor(...INK);
    doc.text(pdfSafe(car.naam || "Auto"), M, y + 8);
    const sub = [car.bouwjaar || (r.datumEersteToelating || "").slice(0, 4), r.kleur ? niceLower(r.kleur) : null, car.km ? num(car.km) + " km" : null].filter(Boolean).join(" · ");
    doc.setFont("helvetica", "normal");
    doc.setFontSize(11);
    doc.setTextColor(...INK2);
    doc.text(pdfSafe(sub), M, y + 15);
    if (plate) {
      const pw = 34;
      const px = W - M - pw;
      doc.setFillColor(245, 196, 0);
      doc.roundedRect(px, y + 1, pw, 9, 1.2, 1.2, "F");
      doc.setFillColor(31, 78, 180);
      doc.rect(px, y + 1, 4.5, 9, "F");
      doc.setFont("helvetica", "bold");
      doc.setFontSize(12);
      doc.setTextColor(17, 17, 17);
      doc.text(plate, px + 4.5 + (pw - 4.5) / 2, y + 7.4, { align: "center" });
    }
    y += 20;
    if (opts.vraagprijs || opts.contact) {
      doc.setFont("helvetica", "bold");
      doc.setFontSize(12);
      doc.setTextColor(...ACCENT);
      const left = opts.vraagprijs ? "Vraagprijs " + eurR(opts.vraagprijs) : "";
      doc.text(pdfSafe(left), M, y + 2);
      if (opts.contact) {
        doc.setFont("helvetica", "normal");
        doc.setFontSize(10);
        doc.setTextColor(...INK2);
        doc.text(pdfSafe(opts.contact), W - M, y + 2, { align: "right" });
      }
      y += 8;
    }
    if (S.foto && car.foto) {
      try {
        const res = await fetch(assetUrl(car.foto));
        if (res.ok) {
          const blob = await downscale(await res.blob(), 1800, 0.86);
          const url = await blobToDataUrl(blob);
          const dims = await new Promise((ok) => {
            const im = new Image();
            im.onload = () => ok([im.naturalWidth, im.naturalHeight]);
            im.onerror = () => ok([16, 10]);
            im.src = url;
          });
          let h = CW * dims[1] / dims[0];
          let w = CW;
          if (h > 95) {
            h = 95;
            w = h * dims[0] / dims[1];
          }
          doc.addImage(url, "JPEG", M + (CW - w) / 2, y, w, h, void 0, "FAST");
          y += h + 6;
        }
      } catch (e) {
      }
    }
    if (S.tekst && opts.tekst) {
      para(opts.tekst, 11);
      y += 1;
    }
    if (S.gegevens) {
      heading("Gegevens");
      const rows = [];
      const add = (k, v) => v && rows.push([k, v]);
      add("Merk en model", merkModel(r) || car.naam);
      add("Eerste toelating", r.datumEersteToelating ? fmtDate(r.datumEersteToelating) : car.bouwjaar ? String(car.bouwjaar) : "");
      add("Kleur en carrosserie", [r.kleur ? niceLower(r.kleur) : "", r.inrichting ? String(r.inrichting).toLowerCase() : ""].filter(Boolean).join(" · "));
      add("Motor", [r.cilinderinhoud ? num(r.cilinderinhoud) + " cc" : "", r.cilinders ? r.cilinders + " cilinders" : "", r.brandstof ? String(r.brandstof).toLowerCase() : ""].filter(Boolean).join(" · "));
      add("Kilometerstand", car.km ? num(car.km) + " km" + (car.kmDatum ? " (" + fmtMonth(car.kmDatum) + ")" : "") + (r.tellerstandOordeel ? " · tellerstand " + String(r.tellerstandOordeel).toLowerCase() : "") : "");
      add("APK", r.vervaldatumApk ? (r.vervaldatumApk < todayISO() ? "verlopen op " : "geldig tot ") + fmtDate(r.vervaldatumApk) : "");
      add("Gewicht", r.massaRijklaar ? num(r.massaRijklaar) + " kg rijklaar" : "");
      if (rows.length) table(null, rows, { 0: { textColor: LABEL, cellWidth: 48 } });
      const specs = car.specs || {};
      const anyNu = SPECS.some(([k]) => specs[k] && specs[k].nu);
      const specRows = SPECS.map(([k, l]) => {
        const s = specs[k] || {};
        if (!s.fabriek && !s.nu) return null;
        return anyNu ? [l, s.fabriek || "–", s.nu ? s.nu + (s.notitie ? " (" + s.notitie + ")" : "") : "origineel"] : [l, s.fabriek];
      }).filter(Boolean);
      if (specRows.length) {
        ensure(20);
        if (anyNu) table(["", "Fabriek", "Nu"], specRows, { 0: { textColor: LABEL, cellWidth: 48 }, 2: { fontStyle: "bold" } });
        else table(["", "Fabrieksopgave"], specRows, { 0: { textColor: LABEL, cellWidth: 48 } });
      }
    }
    if (S.aanpassingen && mods.length) {
      heading("Aanpassingen");
      table(
        null,
        mods.map((m) => [m.titel + (m.notitie ? " - " + m.notitie : ""), m.datum ? fmtMonth(m.datum) : ""]),
        { 1: { halign: "right", textColor: LABEL, cellWidth: 30 } }
      );
    }
    if (S.kosten && inv.length) {
      heading("Onderhoud en kosten");
      const total = sum(inv, (i) => i.totaal);
      const yrs = inv.map((i) => (i.datum || "").slice(0, 4)).filter(Boolean).sort();
      para("In totaal " + eur(total) + " aan " + inv.length + (inv.length === 1 ? " factuur" : " facturen") + (yrs.length ? " (" + yrs[0] + (yrs[yrs.length - 1] !== yrs[0] ? "–" + yrs[yrs.length - 1] : "") + ")" : "") + ", allemaal bewaard.", 10.5, INK2);
      const byCat = {};
      for (const i of inv) byCat[i.categorie || "overig"] = (byCat[i.categorie || "overig"] || 0) + (i.totaal || 0);
      const rows = Object.entries(byCat).sort((a, b) => b[1] - a[1]).map(([k, v]) => [catLabel(k), eur(v)]);
      table(["Categorie", "Bedrag"], rows, { 1: { halign: "right", cellWidth: 36 } });
    }
    if (S.facturen && inv.length) {
      heading("Facturen");
      table(
        ["Datum", "Omschrijving", "Leverancier", "Bedrag"],
        inv.map((i) => [fmtDate(i.datum), i.omschrijving || "", i.leverancier || "", eur(i.totaal)]),
        { 0: { cellWidth: 22 }, 3: { halign: "right", cellWidth: 28 } }
      );
    }
    const keuringen = mergeApk(car, d.apkNotesOf(car.id));
    if (S.apk && keuringen.length) {
      heading("APK-historie");
      table(
        ["Datum", "Uitslag", "Gebreken en bevindingen"],
        keuringen.map((k) => [fmtDate(k.datum), k.uitslag, k.regels.join("\n") || "-"]),
        { 0: { cellWidth: 22 }, 1: { cellWidth: 34 } }
      );
    }
    if (S.bouw && (steps.length || car.samenvatting)) {
      heading("Bouwproces");
      if (car.samenvatting && car.samenvatting.tekst) para(car.samenvatting.tekst, 10.5, INK2);
      const list = steps.filter((s) => s.mijlpaal).length >= 3 ? steps.filter((s) => s.mijlpaal) : steps;
      if (list.length) {
        table(
          null,
          list.map((s) => [s.bezig ? "nu bezig" : fmtRange(s.vanaf, s.datum), s.titel + (s.tekst ? "\n" + s.tekst : "")]),
          { 0: { cellWidth: 36, textColor: LABEL } }
        );
      }
    }
    const pages = doc.getNumberOfPages();
    for (let p = 1; p <= pages; p++) {
      doc.setPage(p);
      doc.setFont("helvetica", "normal");
      doc.setFontSize(8);
      doc.setTextColor(...LABEL);
      doc.text(pdfSafe((car.naam || "") + (plate ? " · " + plate : "") + " · verkoopsamenvatting " + fmtDate(todayISO())), M, 297 - 9);
      doc.text("Pagina " + p + " van " + pages, W - M, 297 - 9, { align: "right" });
    }
    return { bytes: doc.output("arraybuffer"), pages };
  }
  var isAdvies = (g) => /^A[A-Z]/i.test(String(g && g.code || ""));
  function mergeApk(car, notes) {
    const map = {};
    for (const k of car.apk || []) {
      if (!k || !k.datum) continue;
      map[k.datum] = { datum: k.datum, bron: "rdw", soort: k.soort || "", vervaldatum: k.vervaldatum || null, gebreken: k.gebreken || [], eigen: [], keuring: null };
    }
    for (const n of notes || []) {
      if (!n.datum) continue;
      const e = map[n.datum] || (map[n.datum] = { datum: n.datum, bron: "eigen", soort: "", vervaldatum: null, gebreken: [], eigen: [], keuring: null });
      if (n.soort === "keuring") e.keuring = n;
      else e.eigen.push(n);
    }
    return Object.values(map).sort((a, b) => b.datum.localeCompare(a.datum)).map((e) => {
      const defects = e.gebreken.filter((g) => !isAdvies(g));
      const advies = e.gebreken.filter(isAdvies);
      const nG = defects.reduce((a, g) => a + (Number(g.aantal) || 1), 0);
      const res = e.keuring && e.keuring.resultaat;
      const uitslag = res === "afgekeurd" ? "Afgekeurd" : res === "herkeuring" ? "Herkeuring" : res === "goedgekeurd" ? "Goedgekeurd" : nG ? nG + (nG === 1 ? " gebrek" : " gebreken") : advies.length ? "Alleen advies" : e.bron === "rdw" ? "Geen gebreken gemeld" : "Eigen notitie";
      const regels = defects.map((g) => (g.code ? g.code + " " : "") + (g.omschrijving || "gebrek") + (Number(g.aantal) > 1 ? " (" + g.aantal + "×)" : "")).concat(advies.map((g) => "Advies: " + (g.omschrijving || g.code))).concat(e.eigen.map((n) => "Eigen: " + n.tekst), e.keuring && e.keuring.tekst ? ["Eigen: " + e.keuring.tekst] : []);
      return Object.assign(e, { uitslag, aantalGebreken: nG, regels });
    });
  }

  // src/web/backup.js
  var MANIFEST = "autopaspoort.json";
  var LEESMIJ = "Back-up van Autopaspoort\r\n\r\nZet deze back-up terug in de app: Meer > Back-up terugzetten. Kies dan dit zip-bestand.\r\nautopaspoort.json bevat je auto’s, facturen, plannen en bouwlog. In de map bestanden staan je foto’s en pdf’s.\r\n";
  var CRC = (() => {
    const t = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 3988292384 ^ c >>> 1 : c >>> 1;
      t[n] = c >>> 0;
    }
    return t;
  })();
  function crc32(bytes) {
    let c = 4294967295;
    for (let i = 0; i < bytes.length; i++) c = CRC[(c ^ bytes[i]) & 255] ^ c >>> 8;
    return (c ^ 4294967295) >>> 0;
  }
  function dosStamp(d) {
    return {
      time: d.getHours() << 11 | d.getMinutes() << 5 | d.getSeconds() >> 1,
      date: Math.max(1980, d.getFullYear()) - 1980 << 9 | d.getMonth() + 1 << 5 | d.getDate()
    };
  }
  async function makeZip(entries, onProgress) {
    const enc = new TextEncoder();
    const { time, date } = dosStamp(/* @__PURE__ */ new Date());
    const parts = [];
    const central = [];
    let offset = 0;
    let i = 0;
    for (const e of entries) {
      const name = enc.encode(e.name);
      const blob = e.blob instanceof Blob ? e.blob : new Blob([enc.encode(e.text || "")]);
      const crc = crc32(new Uint8Array(await blob.arrayBuffer()));
      const size = blob.size;
      const lh = new DataView(new ArrayBuffer(30));
      lh.setUint32(0, 67324752, true);
      lh.setUint16(4, 20, true);
      lh.setUint16(6, 2048, true);
      lh.setUint16(8, 0, true);
      lh.setUint16(10, time, true);
      lh.setUint16(12, date, true);
      lh.setUint32(14, crc, true);
      lh.setUint32(18, size, true);
      lh.setUint32(22, size, true);
      lh.setUint16(26, name.length, true);
      lh.setUint16(28, 0, true);
      parts.push(lh.buffer, name, blob);
      const ch = new DataView(new ArrayBuffer(46));
      ch.setUint32(0, 33639248, true);
      ch.setUint16(4, 20, true);
      ch.setUint16(6, 20, true);
      ch.setUint16(8, 2048, true);
      ch.setUint16(10, 0, true);
      ch.setUint16(12, time, true);
      ch.setUint16(14, date, true);
      ch.setUint32(16, crc, true);
      ch.setUint32(20, size, true);
      ch.setUint32(24, size, true);
      ch.setUint16(28, name.length, true);
      ch.setUint32(42, offset, true);
      central.push(ch.buffer, name);
      offset += 30 + name.length + size;
      if (onProgress) onProgress(++i, entries.length);
    }
    const cdSize = central.reduce((a, p) => a + p.byteLength, 0);
    const end = new DataView(new ArrayBuffer(22));
    end.setUint32(0, 101010256, true);
    end.setUint16(8, entries.length, true);
    end.setUint16(10, entries.length, true);
    end.setUint32(12, cdSize, true);
    end.setUint32(16, offset, true);
    return new Blob(parts.concat(central, [end.buffer]), { type: "application/zip" });
  }
  function bad(code) {
    const e = new Error(code);
    e.code = code;
    return e;
  }
  async function readZip(file) {
    const tailLen = Math.min(file.size, 65557);
    const tail = new DataView(await file.slice(file.size - tailLen).arrayBuffer());
    let eocd = -1;
    for (let i = tail.byteLength - 22; i >= 0; i--) {
      if (tail.getUint32(i, true) === 101010256) {
        eocd = i;
        break;
      }
    }
    if (eocd < 0) throw bad("geen_backup");
    const count = tail.getUint16(eocd + 10, true);
    const cdSize = tail.getUint32(eocd + 12, true);
    const cdOff = tail.getUint32(eocd + 16, true);
    if (count === 65535 || cdOff === 4294967295) throw bad("te_groot");
    const cd = new DataView(await file.slice(cdOff, cdOff + cdSize).arrayBuffer());
    const dec = new TextDecoder();
    const map = /* @__PURE__ */ new Map();
    let p = 0;
    for (let n = 0; n < count; n++) {
      if (p + 46 > cd.byteLength || cd.getUint32(p, true) !== 33639248) throw bad("kapot");
      const method = cd.getUint16(p + 10, true);
      const csize = cd.getUint32(p + 20, true);
      const nlen = cd.getUint16(p + 28, true);
      const xlen = cd.getUint16(p + 30, true);
      const clen = cd.getUint16(p + 32, true);
      const lho = cd.getUint32(p + 42, true);
      const name = dec.decode(new Uint8Array(cd.buffer, cd.byteOffset + p + 46, nlen));
      map.set(name, { method, csize, lho });
      p += 46 + nlen + xlen + clen;
    }
    return {
      names: [...map.keys()],
      async blob(name, type) {
        const e = map.get(name);
        if (!e) return null;
        const lh = new DataView(await file.slice(e.lho, e.lho + 30).arrayBuffer());
        if (lh.getUint32(0, true) !== 67324752) throw bad("kapot");
        const start = e.lho + 30 + lh.getUint16(26, true) + lh.getUint16(28, true);
        if (e.method === 0) return file.slice(start, start + e.csize, type || "");
        if (e.method === 8 && typeof DecompressionStream !== "undefined") {
          const stream = file.slice(start, start + e.csize).stream().pipeThrough(new DecompressionStream("deflate-raw"));
          return new Blob([await new Response(stream).arrayBuffer()], { type: type || "" });
        }
        throw bad("onbekend_formaat");
      }
    };
  }
  var EXT = { "image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif", "application/pdf": ".pdf" };
  async function makeBackup(onProgress) {
    const { docs, blobs } = await exportAll();
    const manifest = { app: "autopaspoort", formaat: 1, gemaakt: (/* @__PURE__ */ new Date()).toISOString(), docs: {}, bestanden: [] };
    for (const d of docs) manifest.docs[d.path] = d.data;
    const files = [];
    for (const b of blobs) {
      const pad3 = "bestanden/" + b.id + (EXT[b.type] || "");
      manifest.bestanden.push({ id: b.id, type: b.type, pad: pad3, grootte: b.size });
      files.push({ name: pad3, blob: b.blob });
    }
    const entries = [{ name: MANIFEST, text: JSON.stringify(manifest) }, { name: "LEESMIJ.txt", text: LEESMIJ }].concat(files);
    const zip = await makeZip(entries, onProgress);
    return { zip, cars: Object.keys(manifest.docs).filter((k) => k.startsWith("cars/")).length, files: files.length };
  }
  var COLLS2 = ["cars", "invoices", "steps", "parts", "mods", "apkNotes", "alerts", "advice", "settings"];
  function summarise(docs, files, gemaakt, bron) {
    const count = (c) => docs.filter((d) => d.path.startsWith(c + "/")).length;
    return { cars: count("cars"), invoices: count("invoices"), parts: count("parts"), steps: count("steps"), files, gemaakt, bron };
  }
  async function readBackup(file) {
    const head = new Uint8Array(await file.slice(0, 4).arrayBuffer());
    if (head[0] === 80 && head[1] === 75) {
      const z = await readZip(file);
      const mname = z.names.find((n) => n === MANIFEST) || z.names.find((n) => n.endsWith("/" + MANIFEST) && !n.startsWith("__MACOSX"));
      if (!mname) throw bad("geen_backup");
      const prefix = mname.slice(0, mname.length - MANIFEST.length);
      let m;
      try {
        m = JSON.parse(await (await z.blob(mname, "application/json")).text());
      } catch (e) {
        throw bad("kapot");
      }
      if (!m || m.app !== "autopaspoort" || !m.docs) throw bad("geen_backup");
      const docs = Object.keys(m.docs).map((path) => ({ path, data: m.docs[path] }));
      const blobs = [];
      for (const f of m.bestanden || []) {
        const blob = await z.blob(prefix + f.pad, f.type);
        if (blob) blobs.push({ id: f.id, type: f.type, blob });
      }
      return { docs, blobs, summary: summarise(docs, blobs.length, m.gemaakt || null, "zip") };
    }
    let j;
    try {
      j = JSON.parse(await file.text());
    } catch (e) {
      throw bad("geen_backup");
    }
    if (j && j.app === "autopaspoort" && j.docs) {
      const docs = Object.keys(j.docs).map((path) => ({ path, data: j.docs[path] }));
      return { docs, blobs: [], summary: summarise(docs, 0, j.gemaakt || null, "json") };
    }
    if (j && Array.isArray(j.cars)) {
      const docs = [];
      for (const c of COLLS2) {
        for (const row of Array.isArray(j[c]) ? j[c] : []) {
          if (!row || !row.id) continue;
          const data = Object.assign({}, row);
          delete data.id;
          if (c === "cars") delete data.foto;
          if (c === "invoices") delete data.file;
          docs.push({ path: c + "/" + row.id, data });
        }
      }
      return { docs, blobs: [], summary: summarise(docs, 0, j.gemaakt || null, "claude") };
    }
    throw bad("geen_backup");
  }
  async function restoreBackup(parsed) {
    await importAll(parsed);
  }
  var BACKUP_COPY = {
    geen_backup: "Dit is geen back-up van Autopaspoort. Kies het zip-bestand dat de app heeft gemaakt.",
    kapot: "Deze back-up is beschadigd en kan niet worden teruggezet.",
    te_groot: "Deze back-up is te groot om hier terug te zetten.",
    onbekend_formaat: "Dit zip-bestand is ingepakt op een manier die de app niet kan lezen. Gebruik het originele bestand.",
    quota_exceeded: "Er is niet genoeg ruimte op dit apparaat om de back-up terug te zetten."
  };

  // src/web/parts.jsx
  var IS_IOS2 = typeof navigator !== "undefined" && (/iPad|iPhone|iPod/.test(navigator.userAgent) || navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1);
  var standalone = () => {
    try {
      return window.navigator.standalone === true || window.matchMedia("(display-mode: standalone)").matches;
    } catch (e) {
      return false;
    }
  };
  var DAY = 864e5;
  var mb = (n) => n >= 1048576 ? (n / 1048576).toFixed(1).replace(".", ",") + " MB" : Math.max(n ? 1 : 0, Math.round(n / 1024)) + " kB";
  function useInstall() {
    const [, tick] = React.useState(0);
    React.useEffect(() => {
      const on = () => tick((n) => n + 1);
      window.addEventListener("ap-install", on);
      return () => window.removeEventListener("ap-install", on);
    }, []);
    const offer = window.__apInstall || null;
    return {
      installed: standalone(),
      ios: IS_IOS2,
      canPrompt: !!offer,
      async prompt() {
        if (!offer) return false;
        offer.prompt();
        const choice = await offer.userChoice.catch(() => null);
        window.__apInstall = null;
        window.dispatchEvent(new Event("ap-install"));
        return !!(choice && choice.outcome === "accepted");
      }
    };
  }
  function useBackup() {
    const d = useData();
    const ui = useUi();
    const [busy, setBusy] = React.useState(null);
    async function run() {
      if (busy) return;
      setBusy("Back-up maken…");
      let res;
      try {
        res = await makeBackup((i, n) => n > 12 && setBusy("Bezig… " + Math.round(i / n * 100) + "%"));
      } catch (e) {
        setBusy(null);
        return ui.toast("De back-up kon niet worden gemaakt.", "error");
      }
      try {
        await d.caps.downloads.save({ filename: "Autopaspoort back-up " + todayISO() + ".zip", data: res.zip });
        await d.act.setting("laatsteBackup", nowISO());
        ui.toast("Back-up gemaakt: " + plural(res.cars, "auto", "auto’s") + " en " + plural(res.files, "bestand", "bestanden"));
      } catch (e) {
        if (!e || e.code !== "declined") ui.toast("Opslaan lukte niet.", "error");
      } finally {
        setBusy(null);
      }
    }
    return { busy, run };
  }
  function WebNotice() {
    const d = useData();
    const ui = useUi();
    const inst = useInstall();
    React.useEffect(() => {
      let wat = null;
      try {
        wat = window.sessionStorage.getItem("ap-restored");
        window.sessionStorage.removeItem("ap-restored");
      } catch (e) {
        wat = null;
      }
      if (wat) ui.toast("Back-up teruggezet: " + wat);
    }, []);
    const bk = useBackup();
    const [hidden, setHidden] = React.useState(() => ({ install: safeLocal("ap-install-hint") === "1", later: +(safeLocal("ap-backup-later") || 0) }));
    const last = d.settings.laatsteBackup ? new Date(d.settings.laatsteBackup).getTime() : 0;
    const first = d.cars.reduce((a, c) => Math.min(a, c.createdAt ? new Date(c.createdAt).getTime() : Date.now()), Date.now());
    const needBackup = d.cars.length > 0 && Date.now() - hidden.later > 14 * DAY && (last ? Date.now() - last > 30 * DAY : Date.now() - first > 3 * DAY);
    const showInstall = inst.ios && !inst.installed && !hidden.install;
    return /* @__PURE__ */ React.createElement(React.Fragment, null, showInstall && /* @__PURE__ */ React.createElement("div", { className: "banner banner-info web-banner" }, /* @__PURE__ */ React.createElement(Icon, { name: "phone", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Zet Autopaspoort op je beginscherm: tik in Safari op ", /* @__PURE__ */ React.createElement(Icon, { name: "share", size: 15, className: "inline-icon" }), " Deel en kies ", /* @__PURE__ */ React.createElement("strong", null, "Zet op beginscherm"), ". Dan opent hij als een app en blijven je gegevens goed bewaard."), /* @__PURE__ */ React.createElement(
      "button",
      {
        type: "button",
        className: "link-btn",
        onClick: () => {
          safeLocal("ap-install-hint", "1");
          setHidden((h) => ({ ...h, install: true }));
        }
      },
      "Oké"
    )), needBackup && /* @__PURE__ */ React.createElement("div", { className: "banner banner-warn web-banner" }, /* @__PURE__ */ React.createElement(Icon, { name: "download", size: 18 }), /* @__PURE__ */ React.createElement("span", null, last ? "Je laatste back-up is van " + fmtDate(d.settings.laatsteBackup.slice(0, 10)) + "." : "Je hebt nog geen back-up.", " Je gegevens staan alleen op dit apparaat; met een back-up raak je niets kwijt."), /* @__PURE__ */ React.createElement("span", { className: "web-banner-btns" }, /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "secondary", busy: !!bk.busy, onClick: bk.run }, "Back-up maken"), /* @__PURE__ */ React.createElement(
      "button",
      {
        type: "button",
        className: "link-btn",
        onClick: () => {
          const t = String(Date.now());
          safeLocal("ap-backup-later", t);
          setHidden((h) => ({ ...h, later: +t }));
        }
      },
      "Later"
    ))));
  }
  function WebMeer() {
    const d = useData();
    const ui = useUi();
    const inst = useInstall();
    const bk = useBackup();
    const [info, setInfo] = React.useState(null);
    const [reading, setReading] = React.useState(false);
    React.useEffect(() => {
      let alive = true;
      storageInfo().then((i) => alive && setInfo(i), () => {
      });
      return () => {
        alive = false;
      };
    }, [d.invoices.length, d.cars.length]);
    async function pickBackup(file) {
      setReading(true);
      let parsed;
      try {
        parsed = await readBackup(file);
      } catch (e) {
        setReading(false);
        return ui.toast(BACKUP_COPY[e && e.code] || "Deze back-up kon niet worden gelezen.", "error");
      }
      setReading(false);
      const s = parsed.summary;
      const wat = [plural(s.cars, "auto", "auto’s"), plural(s.invoices, "factuur", "facturen"), s.parts ? plural(s.parts, "onderdeel", "onderdelen") : null, s.files ? plural(s.files, "foto of pdf", "foto’s en pdf’s") : null].filter(Boolean).join(", ");
      ui.openSheet("confirm", {
        title: "Back-up terugzetten",
        text: "Back-up" + (s.gemaakt ? " van " + fmtDate(String(s.gemaakt).slice(0, 10)) : "") + " met " + wat + ". Alles wat nu in de app staat, wordt vervangen door de back-up." + (s.bron === "claude" ? " Dit is een back-up uit de claude.ai-app: foto’s en pdf’s zitten daar niet in." : ""),
        confirmLabel: "Terugzetten",
        danger: true,
        onConfirm: async () => {
          try {
            await restoreBackup(parsed);
          } catch (e) {
            ui.toast(BACKUP_COPY[e && e.code] || "Terugzetten lukte niet. Er is niets veranderd.", "error");
            return;
          }
          try {
            window.sessionStorage.setItem("ap-restored", wat);
          } catch (e) {
          }
          window.location.hash = "garage";
          window.location.reload();
        }
      });
    }
    const last = d.settings.laatsteBackup;
    return /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement(Section, { title: "Op dit apparaat" }, /* @__PURE__ */ React.createElement("div", { className: "card" }, /* @__PURE__ */ React.createElement("p", { className: "body-text" }, "Je auto’s, facturen en foto’s staan alleen op dit apparaat, in deze browser. Er gaat niets naar een server en niemand anders kan erbij."), /* @__PURE__ */ React.createElement("dl", { className: "kv" }, /* @__PURE__ */ React.createElement("div", { className: "kv-row" }, /* @__PURE__ */ React.createElement("dt", null, "Opgeslagen"), /* @__PURE__ */ React.createElement("dd", null, info ? plural(d.cars.length, "auto", "auto’s") + " · " + plural(info.files, "bestand", "bestanden") + " · " + mb(info.bytes) : "–")), /* @__PURE__ */ React.createElement("div", { className: "kv-row" }, /* @__PURE__ */ React.createElement("dt", null, "Laatste back-up"), /* @__PURE__ */ React.createElement("dd", null, last ? relTime(last) : "nog niet gemaakt"))), /* @__PURE__ */ React.createElement("div", { className: "row-gap" }, /* @__PURE__ */ React.createElement(Btn, { kind: "primary", icon: "download", busy: !!bk.busy, onClick: bk.run, id: "web-backup" }, bk.busy || "Back-up maken"), /* @__PURE__ */ React.createElement(FilePick, { accept: ".zip,.json,application/zip,application/json", onFile: pickBackup, className: "btn btn-secondary", id: "web-restore" }, reading ? /* @__PURE__ */ React.createElement(Spinner, null) : /* @__PURE__ */ React.createElement(Icon, { name: "upload", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Back-up terugzetten"))), /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Een back-up is één zip-bestand met alles erin, ook je foto’s en pdf’s. Bewaar hem in iCloud Drive, Google Drive of op je pc. Zo verhuis je je gegevens ook naar een ander apparaat."), inst.ios && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Let op: haal je de app van je beginscherm, dan wist je iPhone ook de gegevens. Maak eerst een back-up."))), /* @__PURE__ */ React.createElement(Section, { title: "RDW" }, /* @__PURE__ */ React.createElement("div", { className: "list-card switch-list" }, /* @__PURE__ */ React.createElement(Switch, { id: "set-rdw", checked: d.settings.rdwBijwerken, onChange: (v) => d.act.setting("rdwBijwerken", v), label: "RDW elke week bijwerken", sub: "APK, verzekering en keuringen, zodra je de app opent" }))), !inst.installed && /* @__PURE__ */ React.createElement(Section, { title: "Als app installeren" }, /* @__PURE__ */ React.createElement("div", { className: "card" }, inst.ios ? /* @__PURE__ */ React.createElement("ol", { className: "steps-list" }, /* @__PURE__ */ React.createElement("li", null, "Open deze pagina in Safari."), /* @__PURE__ */ React.createElement("li", null, "Tik op ", /* @__PURE__ */ React.createElement(Icon, { name: "share", size: 15, className: "inline-icon" }), " Deel, onderaan of naast de adresbalk."), /* @__PURE__ */ React.createElement("li", null, "Kies ", /* @__PURE__ */ React.createElement("strong", null, "Zet op beginscherm"), " en tik op Voeg toe.")) : inst.canPrompt ? /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement("p", { className: "body-text" }, "Installeer Autopaspoort als app: hij krijgt een eigen venster en een icoon in je startmenu of dock."), /* @__PURE__ */ React.createElement(Btn, { kind: "secondary", icon: "download", onClick: () => inst.prompt().then((ok) => ok && ui.toast("Geïnstalleerd")) }, "Installeren")) : /* @__PURE__ */ React.createElement("p", { className: "body-text" }, "In Chrome of Edge: klik rechts in de adresbalk op het installeer-icoon, of open het menu en kies Installeren. Op een iPhone: open de pagina in Safari en kies Deel, Zet op beginscherm."))));
  }
  function FileViewer({ id, type, naam, onClose }) {
    const d = useData();
    const ui = useUi();
    const [pages, setPages] = React.useState(null);
    const [err, setErr] = React.useState("");
    const [saving, setSaving] = React.useState(false);
    const url = assetUrl(id);
    const isPdf = type === "pdf";
    React.useEffect(() => {
      if (!isPdf) return void 0;
      let alive = true;
      (async () => {
        try {
          const bytes = await (await fetch(url)).arrayBuffer();
          const width = Math.min(window.innerWidth - 32, 760);
          const out = await renderPdfPages(bytes, width);
          if (alive) setPages(out);
        } catch (e) {
          if (alive) setErr("Deze pdf kon niet worden getoond. Bewaar hem en open hem in een andere app.");
        }
      })();
      return () => {
        alive = false;
      };
    }, [id]);
    async function save() {
      setSaving(true);
      try {
        const blob = await (await fetch(url)).blob();
        const name = naam || (isPdf ? "Factuur.pdf" : "Factuur.jpg");
        await d.caps.downloads.save({ filename: /\.[a-z0-9]{2,4}$/i.test(name) ? name : name + (isPdf ? ".pdf" : ".jpg"), data: blob });
      } catch (e) {
        if (!e || e.code !== "declined") ui.toast("Dit bestand kon niet worden bewaard.", "error");
      } finally {
        setSaving(false);
      }
    }
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: isPdf ? "Pdf" : "Foto",
        onClose,
        left: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: onClose }, "Sluit"),
        right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: save, disabled: saving }, saving ? "Bezig…" : "Bewaar"),
        size: "wide"
      },
      naam && /* @__PURE__ */ React.createElement("p", { className: "small muted sheet-context" }, naam),
      /* @__PURE__ */ React.createElement("div", { className: "viewer" }, !isPdf && /* @__PURE__ */ React.createElement("img", { className: "viewer-page", src: url, alt: "" }), isPdf && !pages && !err && /* @__PURE__ */ React.createElement("div", { className: "viewer-wait" }, /* @__PURE__ */ React.createElement(Spinner, { size: 22 }), /* @__PURE__ */ React.createElement("span", { className: "muted" }, "Pdf openen…")), isPdf && pages && pages.map((src, i) => /* @__PURE__ */ React.createElement("img", { key: i, className: "viewer-page", src, alt: "Pagina " + (i + 1) })), err && /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, err)),
      /* @__PURE__ */ React.createElement("div", { className: "row-gap" }, /* @__PURE__ */ React.createElement(Btn, { kind: "secondary", icon: IS_IOS2 ? "share" : "download", busy: saving, onClick: save }, IS_IOS2 ? "Bewaar of deel" : "Downloaden"), !IS_IOS2 && /* @__PURE__ */ React.createElement("a", { className: "btn btn-ghost", href: url, target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement(Icon, { name: "out", size: 16 }), /* @__PURE__ */ React.createElement("span", null, "In nieuw tabblad")))
    );
  }

  // src/web/register.jsx
  EXTRA.sheets.viewer = FileViewer;
  EXTRA.GarageTop = WebNotice;
  EXTRA.MeerTop = WebMeer;

  // src/screens/garage.jsx
  function Garage() {
    const d = useData();
    const ui = useUi();
    const totalCars = d.cars.reduce((a, c) => a + (d.totals[c.id] || 0), 0);
    return /* @__PURE__ */ React.createElement("div", { className: "page" }, /* @__PURE__ */ React.createElement("header", { className: "page-head" }, /* @__PURE__ */ React.createElement("div", { className: "page-head-row" }, /* @__PURE__ */ React.createElement("h1", { className: "h1" }, "Garage"), ui.wide ? /* @__PURE__ */ React.createElement(Btn, { kind: "secondary", icon: "plus", onClick: () => ui.openSheet("car", {}) }, "Auto toevoegen") : /* @__PURE__ */ React.createElement(IconBtn, { icon: "plus", label: "Auto toevoegen", className: "round", onClick: () => ui.openSheet("car", {}) })), /* @__PURE__ */ React.createElement("p", { className: "sub" }, IS_TEST && /* @__PURE__ */ React.createElement("span", { className: "test-tag" }, "Testversie"), IS_NOAI && !IS_WEB && /* @__PURE__ */ React.createElement("span", { className: "test-tag" }, "Zonder AI"), IS_TEST && !d.aiEnabled && /* @__PURE__ */ React.createElement("span", { className: "test-tag is-off" }, "AI uit"), d.cars.length ? eurR(totalCars) + " uitgegeven aan " + plural(d.cars.length, "auto", "auto’s") : "Alles over je auto’s op één plek")), EXTRA.GarageTop && /* @__PURE__ */ React.createElement(EXTRA.GarageTop, null), /* @__PURE__ */ React.createElement(HelperBanner, null), /* @__PURE__ */ React.createElement(DealBanner, null), /* @__PURE__ */ React.createElement(SuggestionBanner, null), d.active.length === 0 ? /* @__PURE__ */ React.createElement(
      Empty,
      {
        title: d.previous.length ? "Geen auto’s in bezit" : "Je garage is nog leeg",
        text: d.aiEnabled ? "Voeg een auto toe met het kenteken. De assistent haalt de gegevens en APK-historie bij de RDW op. Daarna kun je facturen scannen, plannen maken en je bouwproces bijhouden." : RDW_DIRECT ? "Voeg een auto toe met het kenteken. De app haalt merk, model, APK en keuringen bij de RDW op. Daarna kun je facturen toevoegen, plannen maken en je bouwproces bijhouden." : "Voeg een auto toe met kenteken, merk en model. Daarna kun je facturen toevoegen, plannen maken en je bouwproces bijhouden.",
        action: /* @__PURE__ */ React.createElement(Btn, { kind: "primary", icon: "plus", onClick: () => ui.openSheet("car", {}) }, "Auto toevoegen")
      }
    ) : /* @__PURE__ */ React.createElement("div", { className: "car-grid" }, d.active.map((c) => /* @__PURE__ */ React.createElement(CarCard, { key: c.id, car: c }))), d.previous.length > 0 && /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement("h2", { className: "eyebrow-h" }, "Eerder gehad"), /* @__PURE__ */ React.createElement("div", { className: "list-card" }, d.previous.map((c) => /* @__PURE__ */ React.createElement("button", { key: c.id, type: "button", className: "list-row", onClick: () => ui.navigate({ name: "car", carId: c.id }) }, /* @__PURE__ */ React.createElement(CarThumb, { car: c, size: 40 }), /* @__PURE__ */ React.createElement("span", { className: "list-row-main" }, /* @__PURE__ */ React.createElement("span", { className: "list-row-title" }, c.naam || "Auto"), /* @__PURE__ */ React.createElement("span", { className: "list-row-sub" }, previousSub(c))), /* @__PURE__ */ React.createElement("span", { className: "list-row-amount" }, eurR(d.totals[c.id] || 0)), /* @__PURE__ */ React.createElement(Icon, { name: "next", size: 18, className: "chev" }))))));
  }
  function previousSub(c) {
    const a = c.aankoopdatum ? c.aankoopdatum.slice(0, 4) : "";
    const b = c.verkoopdatum ? c.verkoopdatum.slice(0, 4) : "";
    const span = a && b ? a === b ? a : a + "–" + b : a || b;
    return [span, c.ondertitel].filter(Boolean).join(" · ") || "Niet meer in bezit";
  }
  function CarCard({ car }) {
    const d = useData();
    const ui = useUi();
    const st = rdwState(car, !!d.settings.helperTriggerId, d.aiEnabled);
    const total = d.totals[car.id] || 0;
    const ps = planStats(d.partsOf(car.id), car.budget);
    const meta = [gebruikLabel(car.gebruik), total ? eurR(total) : null, car.km ? num(car.km) + " km" : null, ps.open ? ps.open + " nog kopen" : null].filter(Boolean).join(" · ");
    return /* @__PURE__ */ React.createElement(
      "a",
      {
        className: "car-card",
        href: "#auto-" + car.id,
        onClick: (e) => {
          e.preventDefault();
          ui.navigate({ name: "car", carId: car.id });
        }
      },
      /* @__PURE__ */ React.createElement("div", { className: "car-card-photo" }, car.foto ? /* @__PURE__ */ React.createElement(Photo, { id: car.foto, className: "cover" }) : /* @__PURE__ */ React.createElement("div", { className: "ph" }, /* @__PURE__ */ React.createElement(CarShape, { width: 132 }))),
      /* @__PURE__ */ React.createElement("div", { className: "car-card-body" }, /* @__PURE__ */ React.createElement("div", { className: "row-between" }, /* @__PURE__ */ React.createElement("span", { className: "car-name" }, car.naam || "Nieuwe auto"), /* @__PURE__ */ React.createElement(Plate, { kenteken: car.kenteken })), /* @__PURE__ */ React.createElement(StatusLine, { tone: st.tone, text: st.text }), meta && /* @__PURE__ */ React.createElement("span", { className: "meta" }, meta))
    );
  }
  function HelperBanner() {
    const d = useData();
    if (!d.aiEnabled) return null;
    const h = d.helper;
    const busy = h && h.status === "bezig" && h.startedAt && Date.now() - new Date(h.startedAt).getTime() < 20 * 6e4;
    const waiting = d.openRequests.filter((r) => r.type === "rdw" || r.type === "link").length;
    const linked = !!d.settings.helperTriggerId;
    if (!busy && !waiting) return null;
    return /* @__PURE__ */ React.createElement("div", { className: "banner banner-info" }, busy || linked ? /* @__PURE__ */ React.createElement("span", { className: "spinner", "aria-hidden": "true" }) : /* @__PURE__ */ React.createElement(Icon, { name: "info", size: 18 }), /* @__PURE__ */ React.createElement("span", null, busy ? "De assistent is bezig. Nieuwe gegevens verschijnen vanzelf." : linked ? plural(waiting, "verzoek wacht", "verzoeken wachten") + " op de assistent. Dat duurt meestal een paar minuten." : plural(waiting, "verzoek wacht", "verzoeken wachten") + " op de assistent, maar die is nog niet gekoppeld aan deze app."));
  }
  function DealBanner({ carId }) {
    const d = useData();
    const ui = useUi();
    const deals = d.openAlerts.filter((a2) => a2.soort === "korting" && (!carId || a2.carId === carId));
    if (!deals.length) return null;
    const a = deals[0];
    const car = d.car(a.carId);
    return /* @__PURE__ */ React.createElement("div", { className: "deal" }, /* @__PURE__ */ React.createElement("span", { className: "deal-icon", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement(Icon, { name: "tag", size: 18 })), /* @__PURE__ */ React.createElement("div", { className: "deal-main" }, /* @__PURE__ */ React.createElement("span", { className: "deal-title" }, a.titel || "Aanbieding"), /* @__PURE__ */ React.createElement("span", { className: "deal-sub" }, [car && !carId ? car.naam : null, a.winkel, typeof a.nieuw === "number" ? eur(a.nieuw) + (typeof a.oud === "number" ? " (was " + eur(a.oud) + ")" : "") : null].filter(Boolean).join(" · "), deals.length > 1 ? " · en nog " + (deals.length - 1) : "")), /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", onClick: () => ui.navigate({ name: "plannen" }) }, "Bekijk"));
  }
  function SuggestionBanner() {
    const d = useData();
    const ui = useUi();
    const n = d.suggestions.length;
    if (!n) return null;
    return /* @__PURE__ */ React.createElement("button", { type: "button", className: "banner banner-btn", onClick: () => ui.navigate({ name: "kosten" }) }, /* @__PURE__ */ React.createElement(Icon, { name: "mail", size: 18 }), /* @__PURE__ */ React.createElement("span", null, plural(n, "factuur", "facturen"), " uit je mail om te controleren"), /* @__PURE__ */ React.createElement(Icon, { name: "next", size: 18, className: "chev" }));
  }

  // src/rdw.js
  var API = "https://opendata.rdw.nl/resource/";
  var WEEK = 7 * 864e5;
  function rdwError(code, message) {
    const e = new Error(message);
    e.code = code;
    return e;
  }
  async function query(dataset, params) {
    const url = API + dataset + ".json?" + new URLSearchParams(params).toString();
    const ctl = typeof AbortController !== "undefined" ? new AbortController() : null;
    const timer = ctl ? setTimeout(() => ctl.abort(), 15e3) : null;
    try {
      const res = await fetch(url, ctl ? { signal: ctl.signal } : void 0);
      if (!res.ok) throw rdwError("rdw_http", "RDW antwoordde met " + res.status);
      const rows = await res.json();
      return Array.isArray(rows) ? rows : [];
    } catch (e) {
      if (e && e.code) throw e;
      throw rdwError("rdw_offline", "De RDW is niet bereikbaar");
    } finally {
      if (timer) clearTimeout(timer);
    }
  }
  var ymd = (s) => {
    const t = String(s || "");
    if (/^\d{8}$/.test(t)) return t.slice(0, 4) + "-" + t.slice(4, 6) + "-" + t.slice(6, 8);
    if (/^\d{4}-\d{2}-\d{2}/.test(t)) return t.slice(0, 10);
    return null;
  };
  var num2 = (v) => {
    const x = parseFloat(v);
    return isFinite(x) ? x : null;
  };
  var jaNee = (v) => v === "Ja" ? true : v === "Nee" ? false : null;
  var text = (v) => v && v !== "Niet geregistreerd" && v !== "N.v.t." ? String(v) : null;
  async function lookupRdw(kenteken) {
    const k = normPlate(kenteken);
    const base = (await query("m9d7-ebf2", { kenteken: k }))[0];
    if (!base) return null;
    const [fuel, meld, gebr] = await Promise.all([
      query("8ys7-d773", { kenteken: k }).catch(() => []),
      query("sgfe-77wx", { kenteken: k, $limit: "500" }).catch(() => null),
      query("a34c-vvps", { kenteken: k, $limit: "2000" }).catch(() => null)
    ]);
    const codes = [...new Set((gebr || []).map((g) => g.gebrek_identificatie).filter(Boolean))];
    const descr = {};
    if (codes.length) {
      const list = codes.map((c) => "'" + String(c).replace(/'/g, "''") + "'").join(",");
      const rows = await query("hx2c-gt7k", { $where: "gebrek_identificatie in(" + list + ")", $limit: "2000" }).catch(() => []);
      for (const r of rows) if (r.gebrek_identificatie && r.gebrek_omschrijving && !descr[r.gebrek_identificatie]) descr[r.gebrek_identificatie] = r.gebrek_omschrijving;
    }
    const fuels = (fuel || []).slice().sort((a, b) => (num2(a.brandstof_volgnummer) || 0) - (num2(b.brandstof_volgnummer) || 0));
    const first = (f) => {
      for (const row of fuels) {
        const v = f(row);
        if (v !== null && v !== void 0 && v !== "") return v;
      }
      return null;
    };
    const rdw = {
      merk: text(base.merk),
      handelsbenaming: text(base.handelsbenaming),
      voertuigsoort: text(base.voertuigsoort),
      inrichting: text(base.inrichting),
      kleur: text(base.eerste_kleur),
      cilinders: num2(base.aantal_cilinders),
      cilinderinhoud: num2(base.cilinderinhoud),
      massaLedig: num2(base.massa_ledig_voertuig),
      massaRijklaar: num2(base.massa_rijklaar),
      datumEersteToelating: ymd(base.datum_eerste_toelating_dt || base.datum_eerste_toelating),
      datumTenaamstelling: ymd(base.datum_tenaamstelling_dt || base.datum_tenaamstelling),
      vervaldatumApk: ymd(base.vervaldatum_apk_dt || base.vervaldatum_apk),
      wamVerzekerd: jaNee(base.wam_verzekerd),
      terugroepactie: text(base.openstaande_terugroepactie_indicator),
      tellerstandOordeel: text(base.tellerstandoordeel),
      tellerstandJaar: num2(base.jaar_laatste_registratie_tellerstand),
      brandstof: fuels.map((f) => f.brandstof_omschrijving).filter(Boolean).join(" / ") || null,
      vermogenKw: first((f) => num2(f.nettomaximumvermogen) || num2(f.netto_max_vermogen_elektrisch)),
      verbruik: first((f) => num2(f.brandstofverbruik_gecombineerd)),
      co2: first((f) => num2(f.co2_uitstoot_gecombineerd)),
      emissie: first((f) => text(f.uitlaatemissieniveau)),
      fetchedAt: nowISO()
    };
    let apk;
    if (meld !== null && gebr !== null) {
      const byDate = {};
      const entry = (datum) => byDate[datum] || (byDate[datum] = { datum, soort: "", vervaldatum: null, gebreken: [] });
      for (const m of meld) {
        const datum = ymd(m.meld_datum_door_keuringsinstantie_dt || m.meld_datum_door_keuringsinstantie);
        if (!datum) continue;
        const e = entry(datum);
        const tot = ymd(m.vervaldatum_keuring_dt || m.vervaldatum_keuring);
        if (tot && (!e.vervaldatum || tot > e.vervaldatum)) e.vervaldatum = tot;
        if (!e.soort && m.soort_melding_ki_omschrijving) e.soort = m.soort_melding_ki_omschrijving;
      }
      for (const g of gebr) {
        const datum = ymd(g.meld_datum_door_keuringsinstantie_dt || g.meld_datum_door_keuringsinstantie);
        if (!datum) continue;
        entry(datum).gebreken.push({ code: g.gebrek_identificatie || null, omschrijving: descr[g.gebrek_identificatie] || null, aantal: num2(g.aantal_gebreken_geconstateerd) || 1 });
      }
      apk = Object.values(byDate).sort((a, b) => b.datum.localeCompare(a.datum));
    }
    return { rdw, apk };
  }
  async function syncRdw(act, car, { quiet = false } = {}) {
    const k = normPlate(car && car.kenteken);
    if (!k) return { ok: false, code: "geen_kenteken" };
    let res;
    try {
      res = await lookupRdw(k);
    } catch (e) {
      await act.patch("cars", car.id, { rdwStatus: car.rdw ? "klaar" : "fout", rdwFout: e && e.code || "rdw_offline", rdwCheckedAt: nowISO() }, true);
      return { ok: false, code: e && e.code || "rdw_offline" };
    }
    if (!res) {
      await act.patch("cars", car.id, { rdwStatus: "niet-gevonden", rdwFout: null, rdwCheckedAt: nowISO() }, true);
      return { ok: false, code: "niet_gevonden" };
    }
    const patch = { rdw: res.rdw, rdwStatus: "klaar", rdwFout: null, rdwCheckedAt: nowISO() };
    if (res.apk) patch.apk = res.apk;
    if (!car.naam || !String(car.naam).trim()) patch.naam = merkModel(res.rdw) || fmtPlate(k);
    if (!car.bouwjaar && res.rdw.datumEersteToelating) patch.bouwjaar = +res.rdw.datumEersteToelating.slice(0, 4);
    const r = res.rdw;
    const fromRdw = {
      vermogen: r.vermogenKw ? Math.round(r.vermogenKw * 1.35962) + " pk (" + num(r.vermogenKw) + " kW)" : null,
      gewicht: r.massaRijklaar ? num(r.massaRijklaar) + " kg rijklaar" : null,
      motor: r.cilinderinhoud ? [num(r.cilinderinhoud) + " cc", r.cilinders ? r.cilinders + " cilinders" : null, r.brandstof ? r.brandstof.toLowerCase() : null].filter(Boolean).join(", ") : null
    };
    const specs = {};
    for (const k2 of Object.keys(fromRdw)) {
      const cur = car.specs && car.specs[k2];
      if (fromRdw[k2] && !(cur && cur.fabriek)) specs[k2] = { fabriek: fromRdw[k2], bron: "rdw" };
    }
    if (!car.rdw && Object.keys(specs).length) patch.specs = specs;
    const ok = await act.patch("cars", car.id, patch, quiet);
    return { ok, code: ok ? null : "opslaan", naam: patch.naam || car.naam };
  }
  var RDW_COPY = {
    rdw_offline: "De RDW is nu niet bereikbaar. Controleer je internet en probeer het opnieuw.",
    rdw_http: "De RDW gaf een foutmelding. Probeer het later opnieuw.",
    niet_gevonden: "De RDW kent dit kenteken niet. Controleer het kenteken.",
    opslaan: "De RDW-gegevens konden niet worden opgeslagen."
  };
  function due(car) {
    if (!car.kenteken || car.status === "verkocht") return false;
    const checked = car.rdwCheckedAt ? new Date(car.rdwCheckedAt).getTime() : 0;
    if (car.rdwStatus === "wachten") return true;
    if (car.rdwStatus === "niet-gevonden") return Date.now() - checked > 4 * WEEK;
    if (!car.rdw) return Date.now() - checked > 36e5;
    const at = car.rdw.fetchedAt ? new Date(car.rdw.fetchedAt).getTime() : 0;
    return Date.now() - at > WEEK && Date.now() - checked > 6 * 36e5;
  }
  function useRdwAutoUpdate(d) {
    const last = React.useRef(0);
    const ref = React.useRef(d);
    ref.current = d;
    React.useEffect(() => {
      if (!RDW_DIRECT) return void 0;
      let alive = true;
      const run = async () => {
        const cur = ref.current;
        if (!cur.ready || !cur.settings.rdwBijwerken || typeof navigator !== "undefined" && navigator.onLine === false) return;
        if (Date.now() - last.current < 6 * 36e5) return;
        last.current = Date.now();
        for (const c of cur.cars.filter(due)) {
          if (!alive) return;
          const r = await syncRdw(cur.act, c, { quiet: true });
          if (r.code === "rdw_offline") return;
          await new Promise((ok) => setTimeout(ok, 300));
        }
      };
      const onVisible = () => document.visibilityState === "visible" && run();
      const t = setTimeout(run, 1500);
      document.addEventListener("visibilitychange", onVisible);
      return () => {
        alive = false;
        clearTimeout(t);
        document.removeEventListener("visibilitychange", onVisible);
      };
    }, [d.ready]);
  }

  // src/rules.js
  var isAdviesCode = (g) => /^A[A-Z]/i.test(String(g && g.code || ""));
  var joinNl = (list) => list.length <= 1 ? list.join("") : list.slice(0, -1).join(", ") + " en " + list[list.length - 1];
  var cap = (s) => s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
  var round2 = (n) => Math.round(n * 100) / 100;
  function toAmount(raw) {
    let s = String(raw || "").replace(/[\s ]/g, "");
    const c = s.lastIndexOf(",");
    const p = s.lastIndexOf(".");
    if (c > -1 && p > -1) s = c > p ? s.replace(/\./g, "").replace(",", ".") : s.replace(/,/g, "");
    else if (c > -1) s = /,\d{2}$/.test(s) ? s.replace(/\.(?=\d{3})/g, "").replace(",", ".") : s.replace(/,/g, "");
    else if (p > -1) s = /\.\d{2}$/.test(s) && (s.match(/\./g) || []).length === 1 ? s : s.replace(/\./g, "");
    const n = parseFloat(s);
    return isFinite(n) ? n : null;
  }
  var AMOUNT = /(?:€|eur\b|euro\b|\$|usd\b|£|gbp\b)?\s?(-?\d{1,3}(?:[.,\s ]\d{3})+(?:[.,]\d{2})?|-?\d+[.,]\d{2})(?![\d%])/gi;
  var TOTAL_KEY = /(te\s+betalen|totaalbedrag|eindtotaal|grand\s+total|order\s+total|amount\s+due|total\s+due|total\s+paid|totaal(?:\s+(?:incl\.?|inclusief)(?:\s+btw)?)?|total(?:\s+incl\.?)?)/gi;
  var STRONG_KEY = /te betalen|grand|due|eindtotaal|totaalbedrag|incl|paid|order total/;
  function amountsAfterKeys(text2) {
    const out = [];
    TOTAL_KEY.lastIndex = 0;
    let m;
    while (m = TOTAL_KEY.exec(text2)) {
      const before = text2.slice(Math.max(0, m.index - 3), m.index).toLowerCase();
      if (/sub$/.test(before)) continue;
      const after = text2.slice(m.index + m[0].length, m.index + m[0].length + 70);
      AMOUNT.lastIndex = 0;
      const a = AMOUNT.exec(after);
      if (!a) continue;
      const v = toAmount(a[1]);
      if (v !== null && v > 0 && v < 25e4) out.push({ v, strong: STRONG_KEY.test(m[0].toLowerCase()) });
    }
    return out;
  }
  function totalFrom(text2) {
    const keyed = amountsAfterKeys(text2);
    if (keyed.length) {
      const strong = keyed.filter((k) => k.strong);
      return Math.max(...(strong.length ? strong : keyed).map((k) => k.v));
    }
    const all = [];
    const withSign = /(?:€|\$|£|eur\b|usd\b)\s?(\d{1,3}(?:[.,\s]\d{3})*(?:[.,]\d{2})|\d+[.,]\d{2})/gi;
    let m;
    while (m = withSign.exec(text2)) {
      const v = toAmount(m[1]);
      if (v !== null && v > 0 && v < 25e4) all.push(v);
    }
    return all.length ? Math.max(...all) : null;
  }
  var MAAND = { jan: 1, januari: 1, january: 1, feb: 2, februari: 2, february: 2, mrt: 3, maart: 3, mar: 3, march: 3, apr: 4, april: 4, mei: 5, may: 5, jun: 6, juni: 6, june: 6, jul: 7, juli: 7, july: 7, aug: 8, augustus: 8, august: 8, sep: 9, sept: 9, september: 9, okt: 10, oktober: 10, oct: 10, october: 10, nov: 11, november: 11, dec: 12, december: 12 };
  var pad2 = (n) => String(n).padStart(2, "0");
  function validDate(y, mo, da) {
    if (!(y >= 2e3 && mo >= 1 && mo <= 12 && da >= 1 && da <= 31)) return null;
    const iso = y + "-" + pad2(mo) + "-" + pad2(da);
    const dt = /* @__PURE__ */ new Date(iso + "T00:00:00");
    if (isNaN(dt) || dt.getDate() !== da) return null;
    if (daysUntil(iso) > 31) return null;
    return iso;
  }
  function datesIn(text2) {
    const out = [];
    const push = (iso, index) => iso && out.push({ iso, index });
    let m;
    const dmy = /\b(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})\b/g;
    while (m = dmy.exec(text2)) {
      let da = +m[1];
      let mo = +m[2];
      if (mo > 12 && da <= 12) [da, mo] = [mo, da];
      push(validDate(+m[3], mo, da), m.index);
    }
    const ymd2 = /\b(\d{4})-(\d{2})-(\d{2})\b/g;
    while (m = ymd2.exec(text2)) push(validDate(+m[1], +m[2], +m[3]), m.index);
    const dMy = /\b(\d{1,2})\s+([a-z]{3,9})\.?,?\s+(\d{4})\b/gi;
    while (m = dMy.exec(text2)) {
      const mo = MAAND[m[2].toLowerCase()];
      if (mo) push(validDate(+m[3], mo, +m[1]), m.index);
    }
    const Mdy = /\b([a-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{4})\b/gi;
    while (m = Mdy.exec(text2)) {
      const mo = MAAND[m[1].toLowerCase()];
      if (mo) push(validDate(+m[3], mo, +m[2]), m.index);
    }
    return out.sort((a, b) => a.index - b.index);
  }
  function dateFrom(text2) {
    const dates = datesIn(text2);
    if (!dates.length) return null;
    const key = /(factuurdatum|invoice\s+date|datum|date|besteldatum|orderdatum|order\s+date|paid\s+on|betaald\s+op)/gi;
    let m;
    let best = null;
    while (m = key.exec(text2)) {
      const next = dates.find((x) => x.index >= m.index && x.index - m.index < 60);
      if (next) {
        best = next.iso;
        break;
      }
    }
    return best || dates[0].iso;
  }
  var IGNORE_DOMAIN = /^(gmail|hotmail|outlook|live|icloud|yahoo|paypal|google|apple|microsoft|ziggo|kpn|xs4all|kvk|belastingdienst|postnl|dhl|ups|fedex|gls|dpd|mollie|klarna|ideal|stripe)$/i;
  function supplierFrom(text2) {
    const dom = /(?:https?:\/\/)?(?:www\.)?(?:[a-z0-9._%+-]+@)?([a-z0-9-]{2,})\.(?:co\.uk|nl|com|ie|de|be|eu|uk|fr|it|es|net|org|shop|store|io)\b/gi;
    let m;
    while (m = dom.exec(text2)) {
      const name = m[1].toLowerCase();
      if (!IGNORE_DOMAIN.test(name)) return name.split("-").map(cap).join(" ");
    }
    const lines = text2.split("\n").map((l) => l.trim()).filter(Boolean);
    for (const l of lines.slice(0, 6)) {
      if (l.length < 3 || l.length > 40 || !/[a-z]{3}/i.test(l)) continue;
      if (/factuur|invoice|order|pagina|page|datum|date|bon|receipt|rekening/i.test(l)) continue;
      return l;
    }
    return "";
  }
  var CAT_WORDS = [
    ["apk", /\bapk\b|keuring/i],
    ["remmen", /rem(schijf|schijven|blok|blokken|klauw|klauwen|leiding|vloeistof)|brake/i],
    ["aandrijving", /koppeling|clutch|vliegwiel|flywheel|versnellingsbak|gearbox|aandrijfas|cardan|differentieel|shifter/i],
    ["inlaat", /turbo|intercooler|uitlaat|exhaust|inlaat|intake|spruitstuk|manifold|wastegate|downpipe|gasklep|throttle/i],
    ["onderstel", /schokdemper|demper|coilover|draagarm|stabilisator|fuseekogel|ophanging|uitlijnen|veren\b/i],
    ["wielen", /\bband(en)?\b|velg|wielen|tyre|tire|\brims?\b/i],
    ["onderhoud", /olie|oliefilter|luchtfilter|interieurfilter|bougie|onderhoud|beurt|koelvloeistof|distributie|multiriem/i],
    ["elektronica", /\becu\b|sensor|kabelboom|harness|\bdash\b|display|\baccu\b|battery|dynamo|startmotor|relais/i],
    ["carrosserie", /\blak\b|autolak|primer|blanke|spuitbus|plamuur|bumper|spatbord|motorkap|carrosserie/i],
    ["verlichting", /koplamp|xenon|\bled\b|verlichting|achterlicht|\blamp/i],
    ["interieur", /stoel|stuurwiel|interieur|bekleding/i],
    ["brandstof", /tankstation|brandstof|benzine|diesel|euro\s?95|liter\s+@/i],
    ["vaste", /verzekering|wegenbelasting|motorrijtuigenbelasting|parkeer/i],
    ["gereedschap", /gereedschap|\btools?\b|krik|momentsleutel/i]
  ];
  function guessCategory(text2) {
    for (const [id, re] of CAT_WORDS) if (re.test(text2)) return id;
    return null;
  }
  function parseInvoiceText(text2, cars) {
    const t = String(text2 || "");
    if (t.replace(/\s/g, "").length < 20) return null;
    const nr = /(factuurnummer|factuurnr\.?|factuur\s*nr\.?|factuur\s*#|invoice\s*(?:number|no\.?|nr\.?|#)|order\s*(?:number|no\.?|nr\.?|#)|ordernummer|bestelnummer)\s*[:#.]?\s*([A-Z0-9][A-Z0-9\-/.]{2,24})/i.exec(t);
    const usd = /\$|\bUSD\b/.test(t) && !/€|\bEUR\b/.test(t);
    const gbp = /£|\bGBP\b/.test(t) && !/€|\bEUR\b/.test(t);
    const flat = normPlate(t);
    const car = (cars || []).find((c) => c.kenteken && c.kenteken.length >= 6 && flat.includes(c.kenteken));
    return {
      totaal: totalFrom(t),
      datum: dateFrom(t),
      leverancier: supplierFrom(t),
      factuurnummer: nr ? nr[2].replace(/[.]$/, "") : null,
      valuta: usd ? "USD" : gbp ? "GBP" : "EUR",
      categorie: guessCategory(t),
      kenteken: car ? car.kenteken : null,
      carId: car ? car.id : null
    };
  }
  function localSummary(car, steps, mods) {
    const oud = steps.slice().sort((a, b) => String(a.datum || "").localeCompare(String(b.datum || "")));
    const zinnen = [];
    if (car.aankoopdatum) zinnen.push("Gekocht in " + fmtMonth(car.aankoopdatum) + (car.ondertitel ? " (" + car.ondertitel + ")" : "") + ".");
    if (oud.length) {
      zinnen.push(cap(plural(oud.length, "bouwstap", "bouwstappen")) + " sinds " + fmtMonth(oud[0].datum) + ".");
      const mijl = oud.filter((s) => s.mijlpaal).slice(-3).map((s) => s.titel);
      if (mijl.length) zinnen.push("Mijlpalen: " + joinNl(mijl) + ".");
      const bezig = oud.find((s) => s.bezig);
      const last = oud[oud.length - 1];
      zinnen.push(bezig ? "Nu bezig met: " + bezig.titel + "." : "Laatste stap: " + last.titel + " (" + fmtMonth(last.datum) + ").");
    }
    if (mods.length) zinnen.push(cap(plural(mods.length, "aanpassing", "aanpassingen")) + ", waaronder " + joinNl(mods.slice(0, 3).map((m) => m.titel)) + ".");
    return zinnen.join(" ");
  }
  var monthsSince = (iso) => iso ? -daysUntil(iso) / 30.44 : null;
  function localAdvice(d, car) {
    const r = car.rdw || {};
    const actief = car.status !== "verkocht";
    const kaarten = [];
    const apk = r.vervaldatumApk || car.apkTot || null;
    if (actief && apk) {
      const dd = daysUntil(apk);
      if (dd < 0) kaarten.push({ soort: "let-op", titel: "APK verlopen", tekst: "De APK is verlopen sinds " + fmtDate(apk) + ". De weg op mag pas na een nieuwe keuring." });
      else if (dd <= 60) kaarten.push({ soort: "let-op", titel: "APK verloopt binnenkort", tekst: "Geldig tot " + fmtDate(apk) + ". Plan op tijd een keuring." });
    } else if (actief && car.kenteken) {
      kaarten.push({ soort: "tip", titel: "APK-datum invullen", tekst: "Vul bij Auto bewerken in tot wanneer de APK geldig is. Dan waarschuwt de app op tijd." });
    }
    if (actief && r.wamVerzekerd === false) kaarten.push({ soort: "let-op", titel: "Niet verzekerd volgens de RDW", tekst: "Staat hij niet geschorst? Dan kan de RDW een boete sturen, ook als hij stilstaat." });
    if (actief && r.terugroepactie === "Ja") kaarten.push({ soort: "let-op", titel: "Openstaande terugroepactie", tekst: "Vraag bij de dealer na wat er moet gebeuren." });
    const keuringen = car.apk || [];
    if (keuringen.length) {
      const adv = (keuringen[0].gebreken || []).filter(isAdviesCode);
      if (adv.length) kaarten.push({ soort: "onderhoud", titel: "Advies van de laatste APK", tekst: adv.slice(0, 2).map((g) => g.omschrijving).join("; ") + "." });
      const tel = {};
      for (const k of keuringen) for (const g of k.gebreken || []) if (!isAdviesCode(g) && g.omschrijving) tel[g.omschrijving] = (tel[g.omschrijving] || 0) + 1;
      const terug = Object.keys(tel).filter((o) => tel[o] >= 2);
      if (terug.length) kaarten.push({ soort: "onderhoud", titel: "Komt vaker terug bij de APK", tekst: terug.slice(0, 2).join("; ") + "." });
    }
    const inv = d.invoicesOf(car.id);
    if (actief && car.gebruik !== "project") {
      const beurt = inv.find((i) => i.categorie === "onderhoud");
      const m = beurt ? monthsSince(beurt.datum) : null;
      if (!beurt) kaarten.push({ soort: "onderhoud", titel: "Nog geen onderhoud vastgelegd", tekst: "Voeg de laatste beurt toe, dan zie je hier wanneer de volgende nodig is." });
      else if (m >= 12) kaarten.push({ soort: "onderhoud", titel: "Laatste beurt ruim een jaar geleden", tekst: "Laatste onderhoud: " + (beurt.omschrijving || beurt.leverancier || "beurt") + " op " + fmtDate(beurt.datum) + ". Tijd voor een nieuwe beurt?" });
    }
    const parts = d.partsOf(car.id);
    const ps = planStats(parts, car.budget);
    if (ps.budget !== null && ps.ruimte < 0) kaarten.push({ soort: "tip", titel: "Plan boven budget", tekst: "Het plan is " + eurR(-ps.ruimte) + " duurder dan je budget van " + eurR(ps.budget) + "." });
    if (ps.onbekend) kaarten.push({ soort: "tip", titel: cap(plural(ps.onbekend, "onderdeel", "onderdelen")) + " zonder prijs", tekst: "Vul een prijs in, dan klopt het totaal van je plan." });
    const zonder = parts.filter((p) => p.status === "binnen" && !(p.facturen || []).length);
    if (zonder.length) kaarten.push({ soort: "tip", titel: cap(plural(zonder.length, "gekocht onderdeel", "gekochte onderdelen")) + " zonder factuur", tekst: "Koppel een factuur, dan zie je precies wat je hebt uitgegeven." });
    if (apk && actief && !kaarten.some((k) => k.soort === "let-op")) kaarten.push({ soort: "goed", titel: "Geen dringende punten", tekst: "APK en verzekering zijn in orde volgens de gegevens in de app." });
    const totaal = sum(inv, (i) => i.totaal);
    const stand = [
      (car.naam || "Deze auto") + (car.bouwjaar ? " uit " + car.bouwjaar : "") + (car.km ? ", " + num(car.km) + " km" : "") + ".",
      inv.length ? cap(plural(inv.length, "factuur", "facturen")) + ", samen " + eurR(totaal) + "." : "Nog geen facturen.",
      ps.count ? "Plan: " + ps.open + " van " + plural(ps.count, "onderdeel", "onderdelen") + " nog te kopen" + (ps.nodig ? " (" + eurR(ps.nodig) + ")" : "") + "." : ""
    ].filter(Boolean).join(" ");
    return { stand, kaarten: kaarten.slice(0, 6) };
  }
  function localSalesText(d, car) {
    const r = car.rdw || {};
    const zinnen = [];
    zinnen.push("Te koop: " + (car.naam || merkModel(r) || "deze auto") + (car.bouwjaar ? " uit " + car.bouwjaar : "") + (car.ondertitel ? ", " + car.ondertitel : "") + ".");
    const feiten = [];
    if (car.km) feiten.push(num(car.km) + " km");
    const apk = r.vervaldatumApk || car.apkTot;
    if (apk && daysUntil(apk) >= 0) feiten.push("APK tot " + fmtDate(apk));
    if (r.brandstof) feiten.push(String(r.brandstof).toLowerCase());
    if (feiten.length) zinnen.push(cap(feiten.join(", ")) + ".");
    const mods = d.modsOf(car.id);
    if (mods.length) zinnen.push("Aanpassingen: " + joinNl(mods.slice(0, 5).map((m) => m.titel)) + ".");
    const recent = d.invoicesOf(car.id).filter((i) => ["onderhoud", "remmen", "apk", "wielen"].includes(i.categorie)).slice(0, 3);
    if (recent.length) zinnen.push("Recent onderhoud: " + joinNl(recent.map((i) => (i.omschrijving || i.leverancier || "onderhoud") + " (" + fmtMonth(i.datum) + ")")) + ".");
    const totaal = sum(d.invoicesOf(car.id), (i) => i.totaal);
    if (totaal > 0) zinnen.push("Facturen van samen " + eurR(totaal) + " aan onderhoud en onderdelen kunnen worden ingezien.");
    return zinnen.join(" ");
  }
  var STOP = /* @__PURE__ */ new Set(["voor", "met", "zonder", "incl", "excl", "inclusief", "exclusief", "kopen", "stuk", "deel", "totaal", "samen", "bij", "nieuw", "nieuwe", "new", "and", "the", "for", "with", "verzending", "shipping", "order", "factuur", "invoice", "onderdeel", "onderdelen"]);
  var words = (s) => String(s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "").split(/[^a-z0-9]+/).filter((w) => w.length >= 4 && !STOP.has(w));
  var shiftDays = (iso, n) => {
    const dt = /* @__PURE__ */ new Date(iso + "T00:00:00");
    dt.setDate(dt.getDate() + n);
    return dt.toISOString().slice(0, 10);
  };
  function suggestInvoices(d, part, limit = 3) {
    const elders = /* @__PURE__ */ new Set();
    for (const p of d.parts) if (p.id !== part.id) for (const f of p.facturen || []) elders.add(f);
    const host = part.link ? hostOf(part.link).replace(/^www\./, "").split(".")[0] : "";
    const shop = /* @__PURE__ */ new Set([...words(part.winkel), ...words(host)]);
    const naam = /* @__PURE__ */ new Set([...words(part.naam), ...words(part.notitie)]);
    const prijs = typeof part.prijs === "number" ? part.prijs * (part.aantal || 1) : null;
    const sinds = part.createdAt ? shiftDays(String(part.createdAt).slice(0, 10), -21) : null;
    const res = [];
    for (const inv of d.invoices) {
      if (inv.carId && inv.carId !== part.carId) continue;
      const lev = new Set(words(inv.leverancier));
      const hooi = new Set(words([inv.leverancier, inv.omschrijving, inv.notitie, (inv.regels || []).map((x) => x.omschrijving).join(" ")].join(" ")));
      let score = 0;
      const why = [];
      if ([...shop].some((w) => lev.has(w) || hooi.has(w))) {
        score += 5;
        why.push("zelfde winkel");
      }
      const n = [...naam].filter((w) => hooi.has(w)).length;
      if (n) {
        score += Math.min(6, n * 2);
        why.push("naam komt overeen");
      }
      if (prijs && typeof inv.totaal === "number" && inv.totaal > 0) {
        const diff = Math.abs(inv.totaal - prijs) / Math.max(prijs, 1);
        if (diff <= 0.02) {
          score += 4;
          why.push("bedrag klopt");
        } else if (diff <= 0.1) {
          score += 3;
          why.push("bedrag bijna gelijk");
        } else if (diff <= 0.25) score += 1;
      }
      if (inv.voorstel) {
        score += 1;
        why.push("uit je mail");
      }
      if (inv.carId === part.carId) score += 1;
      if (sinds && inv.datum && inv.datum >= sinds) score += 1;
      if (elders.has(inv.id)) score -= 3;
      if (score >= 4) res.push({ inv, score, why });
    }
    return res.sort((a, b) => b.score - a.score || String(b.inv.datum || "").localeCompare(String(a.inv.datum || ""))).slice(0, limit);
  }
  async function linkInvoiceToPart(d, partId, invoiceId, inv, onderweg) {
    const part = d.parts.find((p) => p.id === partId);
    if (!part) return false;
    const facturen = Array.from(/* @__PURE__ */ new Set([...part.facturen || [], invoiceId]));
    const status = !part.status || part.status === "nog" ? onderweg ? "besteld" : "binnen" : part.status;
    const fields = { facturen, status, gekochtOp: part.gekochtOp || inv && inv.datum || todayISO(), volgen: false, deal: null };
    if (typeof part.prijs !== "number" && inv && typeof inv.totaal === "number") {
      fields.prijs = round2(inv.totaal / (part.aantal || 1));
      fields.prijsVanLink = false;
    }
    return d.act.patch("parts", partId, fields, true);
  }

  // src/ai.js
  var HIDE = /* @__PURE__ */ new Set(["not_granted", "sampling_disabled", "not_declared", "capability_disabled", "capability_removed"]);
  var aiHidden = (e) => !!(e && HIDE.has(e.code));
  function aiErrorCopy(e) {
    const c = e && e.code;
    if (HIDE.has(c)) return "De assistent is in deze weergave niet beschikbaar.";
    if (c === "rate_limited") return "Even te veel vragen tegelijk. Probeer het over een minuut opnieuw.";
    if (c === "image_rejected") return "Deze afbeelding kon niet worden gelezen. Kies een scherpere foto of een ander bestand.";
    if (c === "images_unavailable") return "Foto’s lezen kan in deze weergave niet. Vul de factuur zelf in.";
    if (c === "session_expired") return "Je bent uitgelogd bij Claude. Log opnieuw in en probeer het nog eens.";
    if (c === "refused") return "De assistent kon dit niet verwerken. Pas de tekst aan of vul het zelf in.";
    if (c === "prompt_too_large") return "Te veel gegevens in één keer. Probeer het met minder.";
    if (c === "cancelled") return "";
    return "Dat lukte niet. Probeer het opnieuw.";
  }
  var CAT_LIST = CATS.map(([id, l]) => id + " (" + l + ")").join(", ");
  function carContext(d, c) {
    const parts = d.partsOf(c.id).slice(-15).map((p) => p.naam);
    const recent = d.invoicesOf(c.id).slice(0, 8).map((i) => clip((i.omschrijving || "") + " · " + (i.leverancier || ""), 70));
    const mods = d.modsOf(c.id).slice(0, 10).map((m) => m.titel);
    const r = c.rdw || {};
    return {
      id: c.id,
      naam: c.naam || "",
      bouwjaar: c.bouwjaar || (r.datumEersteToelating || "").slice(0, 4) || null,
      kenteken: c.kenteken ? fmtPlate(c.kenteken) : null,
      brandstof: r.brandstof || null,
      motor: c.specs && c.specs.motor && (c.specs.motor.nu || c.specs.motor.fabriek) || (r.cilinderinhoud ? r.cilinderinhoud + " cc" : null),
      gebruik: gebruikLabel(c.gebruik) || null,
      status: c.status === "verkocht" ? "niet meer in bezit" + (c.verkoopdatum ? " sinds " + c.verkoopdatum : "") : "in bezit" + (c.aankoopdatum ? " sinds " + c.aankoopdatum : ""),
      plannen: parts,
      aanpassingen: mods,
      recenteFacturen: recent
    };
  }
  function cleanInvoice(j, carIds) {
    const o = j && typeof j === "object" ? j : {};
    const regels = Array.isArray(o.regels) ? o.regels.slice(0, 12).map((r) => ({ omschrijving: clip(String(r && r.omschrijving || ""), 120), bedrag: parseNum(r && r.bedrag) })).filter((r) => r.omschrijving) : [];
    const auto = o.auto && typeof o.auto === "object" ? o.auto : {};
    const zek = ["zeker", "waarschijnlijk", "gok"].includes(auto.zekerheid) ? auto.zekerheid : "gok";
    return {
      leesbaar: o.leesbaar !== false,
      leverancier: clip(String(o.leverancier || "").trim(), 80),
      datum: isISODate(o.datum) ? o.datum : null,
      factuurnummer: o.factuurnummer ? clip(String(o.factuurnummer), 40) : null,
      totaal: parseNum(o.totaal),
      btw: parseNum(o.btw),
      valuta: o.valuta ? String(o.valuta).slice(0, 3).toUpperCase() : "EUR",
      kenteken: o.kenteken ? String(o.kenteken).slice(0, 12) : null,
      km: parseNum(o.kmStand != null ? o.kmStand : o.km),
      omschrijving: clip(String(o.omschrijving || "").trim(), 80),
      categorie: isCat(o.categorie) ? o.categorie : "overig",
      regels,
      auto: {
        id: auto.id && carIds.includes(auto.id) ? auto.id : null,
        zekerheid: zek,
        reden: clip(String(auto.reden || ""), 120)
      }
    };
  }
  async function readInvoice(sample, { images, pdfText, d, signal }) {
    const cars = d.cars.map((c) => carContext(d, c));
    const prompt = [
      "Je bent de factuurlezer van een app waarin iemand alle kosten van zijn auto’s bijhoudt (Nederland).",
      "Bij dit verzoek horen " + images.length + " afbeelding(en): een foto of de pagina’s van één factuur of bon.",
      pdfText ? 'Tekst uit het pdf-bestand (kan helpen bij het lezen):\n"""\n' + clip(pdfText, 6e3) + '\n"""' : "",
      "Antwoord met alleen JSON in precies deze vorm:",
      '{"leesbaar": true, "leverancier": "naam van winkel of garage", "datum": "2026-03-14", "factuurnummer": "F-1234" of null, "totaal": 123.45, "btw": 21.43 of null, "valuta": "EUR", "kenteken": "AB-123-C" of null, "kmStand": 123456 of null, "omschrijving": "max 6 woorden: wat er gekocht of gedaan is", "categorie": "onderhoud", "regels": [{"omschrijving": "...", "bedrag": 12.5}], "auto": {"id": "id uit de lijst of null", "zekerheid": "zeker", "reden": "max 12 woorden"}}',
      "Regels:",
      "- totaal is het eindbedrag inclusief btw dat betaald is, als getal met een punt als decimaalteken. Andere valuta: zet valuta en reken niet om.",
      "- datum is de factuurdatum als JJJJ-MM-DD.",
      "- categorie is één id uit: " + CAT_LIST + ".",
      "- regels: maximaal 12, de belangrijkste, bedragen inclusief btw als dat te zien is.",
      '- auto: kies de auto waar deze factuur het waarschijnlijkst bij hoort. Staat het kenteken van een auto op de factuur: zekerheid "zeker". Past het onderdeel of werk duidelijk bij één auto (merk, model, motor, plannen, eerdere facturen): "waarschijnlijk". Anders "gok". Hoort het bij geen van deze auto’s (andere auto, geen autokosten): id null.',
      '- Is de afbeelding geen factuur of onleesbaar: "leesbaar": false.',
      "De auto’s:",
      JSON.stringify(cars)
    ].filter(Boolean).join("\n");
    const j = await sample.json(prompt, { images, signal, modelTier: "default" });
    return cleanInvoice(j, d.cars.map((c) => c.id));
  }
  async function condenseStep(sample, { car, text: text2, recent, signal }) {
    const prompt = [
      "Maak van deze notitie één bouwstap voor het bouwlog van een " + (car.naam || "auto") + ".",
      'Antwoord met alleen JSON: {"titel": "max 6 woorden", "tekst": "één zin van max 20 woorden", "samenvoegen": "id of null"}',
      "Feitelijk en kort, Nederlands, geen verhaal, geen overdrijving, geen datum in de tekst.",
      'Gaat de notitie duidelijk over hetzelfde werk als een van deze recente stappen, geef dan die id bij "samenvoegen" en schrijf titel en tekst voor de samengevoegde stap. Anders null.',
      "Recente stappen: " + JSON.stringify(recent.map((s) => ({ id: s.id, datum: s.datum, titel: s.titel, tekst: s.tekst }))),
      "Notitie:",
      '"""' + clip(text2, 3e3) + '"""'
    ].join("\n");
    const j = await sample.json(prompt, { signal, modelTier: "quick", cache: false });
    const ids = recent.map((s) => s.id);
    return {
      titel: clip(String(j && j.titel || "").trim(), 60),
      tekst: clip(String(j && j.tekst || "").trim(), 220),
      samenvoegen: j && ids.includes(j.samenvoegen) ? j.samenvoegen : null
    };
  }
  async function buildSummary(sample, { car, steps, mods, signal }) {
    const oud = steps.slice().reverse();
    const prompt = [
      "Schrijf de samenvatting van het bouwproces van deze auto voor zijn digitale paspoort.",
      "Maximaal 70 woorden, 3 tot 5 korte zinnen, Nederlands, feitelijk. Volgorde: begin (aankoop), belangrijkste stappen, waar hij nu staat, doel als dat bekend is. Geen opsomming, geen kopjes, geen inleiding.",
      "Auto: " + JSON.stringify({ naam: car.naam, bouwjaar: car.bouwjaar || null, aankoopdatum: car.aankoopdatum || null, gebruik: gebruikLabel(car.gebruik) || null }),
      "Bouwstappen van oud naar nieuw: " + JSON.stringify(oud.map((s) => ({ datum: s.datum, titel: s.titel, tekst: s.tekst, mijlpaal: !!s.mijlpaal, bezig: !!s.bezig }))),
      "Aanpassingen: " + JSON.stringify(mods.map((m) => m.titel))
    ].join("\n");
    const r = await sample(prompt, { signal, modelTier: "default", cache: false });
    return r.text.trim();
  }
  function adviceData(d, car) {
    const inv = d.invoicesOf(car.id);
    const byCat = {};
    for (const i of inv) byCat[catLabel(i.categorie)] = (byCat[catLabel(i.categorie)] || 0) + (i.totaal || 0);
    const r = car.rdw || {};
    return {
      vandaag: todayISO(),
      auto: {
        naam: car.naam,
        bouwjaar: car.bouwjaar || null,
        gebruik: gebruikLabel(car.gebruik) || null,
        km: car.km || null,
        aankoopdatum: car.aankoopdatum || null,
        specs: car.specs || {}
      },
      rdw: car.rdw ? {
        apkTot: r.vervaldatumApk || null,
        verzekerd: r.wamVerzekerd,
        brandstof: r.brandstof || null,
        vermogenKw: r.vermogenKw || null,
        tellerstand: r.tellerstandOordeel || null,
        terugroepactie: r.terugroepactie || null,
        laatsteKeuringen: (car.apk || []).slice(0, 3)
      } : "onbekend",
      kosten: { totaal: Math.round(sum(inv, (i) => i.totaal)), facturen: inv.length, perCategorie: byCat, recent: inv.slice(0, 10).map((i) => ({ datum: i.datum, wat: i.omschrijving, bedrag: i.totaal })) },
      plannen: d.partsOf(car.id).map((p) => ({ naam: p.naam, categorie: catLabel(p.categorie), nodig: p.prioriteit, status: p.status, prijs: p.prijs ?? null })),
      bouwlog: d.stepsOf(car.id).slice(0, 15).map((s) => ({ datum: s.datum, titel: s.titel, tekst: s.tekst, bezig: !!s.bezig })),
      aanpassingen: d.modsOf(car.id).map((m) => m.titel),
      eigenApkBevindingen: d.apkNotesOf(car.id).slice(0, 10).map((n) => ({ datum: n.datum, tekst: n.tekst }))
    };
  }
  var SOORTEN = ["let-op", "tip", "afgeraden", "goed", "onderhoud"];
  async function makeAdvice(sample, { d, car, signal }) {
    const prompt = [
      "Je bent een ervaren, eerlijke monteur en auto-hobbyist. Beoordeel de stand van zaken van deze auto en geef praktisch advies aan de eigenaar.",
      'Antwoord met alleen JSON: {"stand": "2 of 3 zinnen", "kaarten": [{"soort": "let-op", "titel": "max 7 woorden", "tekst": "max 30 woorden", "onderdeel": {"naam": "...", "categorie": "id"} of null}]}',
      "soort is een van: let-op (echte risico’s: APK verlopen, niet verzekerd, veiligheid), tip, afgeraden (iets in de plannen of aankopen dat je zou afraden), goed (wat goed gaat), onderhoud.",
      'Maximaal 5 kaarten, belangrijkste eerst. Geef alleen "onderdeel" als de tip een concreet onderdeel is dat op de wensenlijst kan; categorie is een id uit: ' + CATS.map((c) => c[0]).join(", ") + ".",
      "Wees concreet en gebruik de gegevens. Verzin geen feiten die er niet staan. Nederlands.",
      "Gegevens: " + JSON.stringify(adviceData(d, car))
    ].join("\n");
    const j = await sample.json(prompt, { signal, modelTier: "default", cache: false });
    const kaarten = Array.isArray(j && j.kaarten) ? j.kaarten.slice(0, 6).map((k) => ({
      soort: SOORTEN.includes(k && k.soort) ? k.soort : "tip",
      titel: clip(String(k && k.titel || ""), 80),
      tekst: clip(String(k && k.tekst || ""), 280),
      onderdeel: k && k.onderdeel && k.onderdeel.naam ? { naam: clip(String(k.onderdeel.naam), 80), categorie: isCat(k.onderdeel.categorie) ? k.onderdeel.categorie : "overig" } : null
    })).filter((k) => k.titel) : [];
    return { stand: clip(String(j && j.stand || ""), 500), kaarten };
  }
  async function salesText(sample, { d, car, signal }) {
    const prompt = [
      "Schrijf een verkooptekst voor deze auto, voor bovenaan een A4-verkoopsamenvatting.",
      "80 tot 120 woorden, Nederlands, eerlijk en feitelijk, zonder overdreven woorden of uitroeptekens. Noem de sterke punten, wat er recent is gedaan en wat een koper moet weten. Geen kopje, geen opsomming.",
      "Gegevens: " + JSON.stringify(adviceData(d, car))
    ].join("\n");
    const r = await sample(prompt, { signal, modelTier: "default", cache: false });
    return r.text.trim();
  }

  // src/screens/car.jsx
  function CarPage({ carId, tab }) {
    const d = useData();
    const ui = useUi();
    const car = d.car(carId);
    if (!car) {
      return /* @__PURE__ */ React.createElement("div", { className: "page" }, /* @__PURE__ */ React.createElement(
        Empty,
        {
          title: "Deze auto staat niet (meer) in je garage",
          action: /* @__PURE__ */ React.createElement(Btn, { kind: "primary", onClick: () => ui.navigate({ name: "garage" }) }, "Naar de garage")
        }
      ));
    }
    const subline = [car.bouwjaar, car.ondertitel || gebruikLabel(car.gebruik), car.status === "verkocht" ? "eerder gehad" : null].filter(Boolean).join(" · ");
    const go = (t) => ui.navigate({ name: "car", carId, tab: t });
    const menu = () => ui.openSheet("menu", {
      title: car.naam || "Auto",
      items: [
        ["edit", "Auto bewerken", () => ui.openSheet("car", { id: car.id })],
        ["doc", "Verkoopsamenvatting (A4)", () => ui.openSheet("verkoop", { carId: car.id })],
        ["camera", "Factuur scannen", () => ui.openSheet("scan", { carId: car.id })],
        ["wrench", "Bouwstap toevoegen", () => ui.openSheet("step", { carId: car.id })]
      ]
    });
    let body = null;
    if (tab === "gegevens") body = /* @__PURE__ */ React.createElement(GegevensTab, { car });
    else if (tab === "kosten") body = /* @__PURE__ */ React.createElement(KostenTab, { car });
    else if (tab === "bouwlog") body = /* @__PURE__ */ React.createElement(BouwlogTab, { car });
    else if (tab === "plannen") body = /* @__PURE__ */ React.createElement(PlannenTab, { car });
    else if (tab === "advies") body = /* @__PURE__ */ React.createElement(AdviesTab, { car });
    else body = /* @__PURE__ */ React.createElement(OverzichtTab, { car, go });
    return /* @__PURE__ */ React.createElement("div", { className: "car-page" }, /* @__PURE__ */ React.createElement("div", { className: "car-head" }, !ui.wide && /* @__PURE__ */ React.createElement("div", { className: "topbar" }, /* @__PURE__ */ React.createElement("button", { type: "button", className: "back-btn", onClick: () => ui.navigate({ name: "garage" }) }, /* @__PURE__ */ React.createElement(Icon, { name: "back", size: 24, stroke: 2.2 }), "Garage"), /* @__PURE__ */ React.createElement(IconBtn, { icon: "dots", label: "Meer opties", onClick: menu })), /* @__PURE__ */ React.createElement("div", { className: "car-id" }, /* @__PURE__ */ React.createElement(CarThumb, { car, size: ui.wide ? 56 : 48 }), /* @__PURE__ */ React.createElement("div", { className: "car-id-text" }, /* @__PURE__ */ React.createElement("h1", { className: "car-title" }, car.naam || "Nieuwe auto"), subline && /* @__PURE__ */ React.createElement("span", { className: "sub" }, subline)), /* @__PURE__ */ React.createElement(Plate, { kenteken: car.kenteken, size: ui.wide ? "md" : "sm" }), ui.wide && /* @__PURE__ */ React.createElement("div", { className: "car-id-actions" }, /* @__PURE__ */ React.createElement(Btn, { icon: "doc", onClick: () => ui.openSheet("verkoop", { carId: car.id }) }, "Verkoopsamenvatting"), /* @__PURE__ */ React.createElement(IconBtn, { icon: "edit", label: "Auto bewerken", onClick: () => ui.openSheet("car", { id: car.id }) }))), /* @__PURE__ */ React.createElement("nav", { className: "tabs", "aria-label": "Paspoort" }, TABS.map(([id, label]) => /* @__PURE__ */ React.createElement(
      "a",
      {
        key: id,
        href: "#auto-" + carId + (id === "overzicht" ? "" : "-" + id),
        className: "tab" + (tab === id ? " is-on" : ""),
        "aria-current": tab === id ? "page" : void 0,
        onClick: (e) => {
          e.preventDefault();
          go(id);
        }
      },
      label
    )))), /* @__PURE__ */ React.createElement("div", { className: "page page-car" }, body));
  }
  function OverzichtTab({ car, go }) {
    const d = useData();
    const ui = useUi();
    const inv = d.invoicesOf(car.id);
    const steps = d.stepsOf(car.id);
    const ps = planStats(d.partsOf(car.id), car.budget);
    const total = sum(inv, (i) => i.totaal);
    const now = steps.find((s) => s.bezig) || steps[0];
    const adv = d.adviceOf(car.id);
    const r = car.rdw || {};
    const [photoBusy, setPhotoBusy] = React.useState(false);
    async function setPhoto(file) {
      setPhotoBusy(true);
      try {
        const blob = await downscale(file, 1800, 0.84);
        const res = await d.act.upload(blob, "image/jpeg");
        const old = car.foto;
        const ok = await d.act.patch("cars", car.id, { foto: res.id });
        if (ok && old) d.act.deleteAsset(old);
      } catch (e) {
        ui.toast(e && e.code === "too_large" ? "Deze foto is te groot." : "De foto kon niet worden opgeslagen.", "error");
      } finally {
        setPhotoBusy(false);
      }
    }
    const apkDate = r.vervaldatumApk || !car.rdw && car.apkTot || null;
    const apkDays = apkDate ? daysUntil(apkDate) : null;
    let alert = null;
    if (car.status !== "verkocht" && (car.rdw || car.apkTot)) {
      const expired = apkDays !== null && apkDays < 0;
      const unins = r.wamVerzekerd === false;
      if (expired || unins) {
        alert = {
          tone: "danger",
          title: expired && unins ? "APK verlopen en niet verzekerd" : expired ? "APK verlopen" : "Niet verzekerd",
          text: "Staat hij niet geschorst? Dan kan de RDW een boete sturen, ook als hij alleen stilstaat."
        };
      } else if (apkDays !== null && apkDays <= 60) {
        alert = { tone: "warn", title: "APK verloopt over " + plural(apkDays, "dag", "dagen"), text: "Plan een keuring vóór " + fmtDate(apkDate) + "." };
      }
      if (!alert && r.terugroepactie === "Ja") alert = { tone: "warn", title: "Openstaande terugroepactie", text: "De RDW meldt een terugroepactie voor deze auto. Vraag het na bij de dealer." };
    }
    return /* @__PURE__ */ React.createElement("div", { className: "ov-grid" }, /* @__PURE__ */ React.createElement("div", { className: "ov-main" }, /* @__PURE__ */ React.createElement("div", { className: "hero" }, car.foto ? /* @__PURE__ */ React.createElement(Photo, { id: car.foto, className: "cover" }) : /* @__PURE__ */ React.createElement("div", { className: "ph" }, /* @__PURE__ */ React.createElement(CarShape, { width: 150 }), /* @__PURE__ */ React.createElement("span", null, "Nog geen foto")), d.caps.assets && /* @__PURE__ */ React.createElement(FilePick, { accept: "image/*", onFile: setPhoto, className: "hero-btn", id: "foto-" + car.id }, photoBusy ? /* @__PURE__ */ React.createElement(Spinner, null) : /* @__PURE__ */ React.createElement(Icon, { name: "camera", size: 18 }), /* @__PURE__ */ React.createElement("span", null, car.foto ? "Foto wijzigen" : "Foto toevoegen"))), car.rdwStatus === "wachten" && RDW_DIRECT && /* @__PURE__ */ React.createElement("div", { className: "banner banner-info" }, /* @__PURE__ */ React.createElement("span", { className: "spinner", "aria-hidden": "true" }), /* @__PURE__ */ React.createElement("span", null, "De RDW-gegevens worden opgehaald…")), car.rdwStatus === "wachten" && !RDW_DIRECT && /* @__PURE__ */ React.createElement("div", { className: "banner banner-info" }, d.settings.helperTriggerId ? /* @__PURE__ */ React.createElement("span", { className: "spinner", "aria-hidden": "true" }) : /* @__PURE__ */ React.createElement(Icon, { name: "info", size: 18 }), /* @__PURE__ */ React.createElement("span", null, d.settings.helperTriggerId ? "De assistent haalt de RDW-gegevens op. Dat duurt meestal een paar minuten; je kunt gewoon verder." : "De RDW-gegevens worden opgehaald zodra de assistent aan deze app is gekoppeld. Je kunt gewoon verder.")), alert && /* @__PURE__ */ React.createElement("div", { className: "alert-card tone-card-" + alert.tone }, /* @__PURE__ */ React.createElement(Icon, { name: "alert", size: 20 }), /* @__PURE__ */ React.createElement("div", null, /* @__PURE__ */ React.createElement("strong", null, alert.title), /* @__PURE__ */ React.createElement("p", null, alert.text))), /* @__PURE__ */ React.createElement("div", { className: "quick" }, /* @__PURE__ */ React.createElement("button", { type: "button", onClick: () => ui.openSheet("scan", { carId: car.id }) }, /* @__PURE__ */ React.createElement(Icon, { name: "camera", size: 22 }), /* @__PURE__ */ React.createElement("span", null, "Factuur")), /* @__PURE__ */ React.createElement("button", { type: "button", onClick: () => ui.openSheet("step", { carId: car.id }) }, /* @__PURE__ */ React.createElement(Icon, { name: "wrench", size: 22 }), /* @__PURE__ */ React.createElement("span", null, "Bouwstap")), /* @__PURE__ */ React.createElement("button", { type: "button", onClick: () => ui.openSheet("part", { carId: car.id }) }, /* @__PURE__ */ React.createElement(Icon, { name: "list", size: 22 }), /* @__PURE__ */ React.createElement("span", null, "Onderdeel"))), /* @__PURE__ */ React.createElement("div", { className: "stats" }, /* @__PURE__ */ React.createElement(Stat, { label: "Uitgegeven", value: eurR(total), sub: plural(inv.length, "factuur", "facturen"), onClick: () => go("kosten") }), /* @__PURE__ */ React.createElement(Stat, { label: "Km-stand", value: car.km ? num(car.km) : "–", sub: car.kmDatum ? "per " + fmtDate(car.kmDatum) : "nog niet ingevuld", onClick: () => ui.openSheet("car", { id: car.id, focus: "km" }) }), /* @__PURE__ */ React.createElement(Stat, { label: "Nog nodig", value: !ps.count || !ps.nodig && ps.onbekend ? "–" : eurR(ps.nodig) + (ps.onbekend ? "+" : ""), sub: ps.count ? ps.open + " nog kopen" + (ps.onbekend && !ps.nodig ? ", zonder prijs" : "") : "geen plannen", onClick: () => go("plannen") }), /* @__PURE__ */ React.createElement(
      Stat,
      {
        label: "APK",
        value: r.vervaldatumApk ? fmtDate(r.vervaldatumApk) : "–",
        tone: apkDays !== null && apkDays < 0 ? "danger" : apkDays !== null && apkDays <= 60 ? "warn" : null,
        sub: r.vervaldatumApk ? apkDays < 0 ? "verlopen" : "geldig" : car.kenteken ? "via de RDW" : "geen kenteken",
        onClick: () => go("gegevens")
      }
    )), /* @__PURE__ */ React.createElement("section", { className: "card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, "Bouwproces"), steps.length > 0 && /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => go("bouwlog") }, "Bouwlog")), car.samenvatting && car.samenvatting.tekst ? /* @__PURE__ */ React.createElement("p", { className: "body-text" }, clip(car.samenvatting.tekst, 240)) : null, now ? /* @__PURE__ */ React.createElement("button", { type: "button", className: "now-row", onClick: () => ui.openSheet("step", { id: now.id }) }, /* @__PURE__ */ React.createElement("span", { className: "now-dot" + (now.bezig ? " is-now" : ""), "aria-hidden": "true" }), /* @__PURE__ */ React.createElement("span", { className: "now-text" }, /* @__PURE__ */ React.createElement("span", { className: "now-label" }, now.bezig ? "Nu bezig" : "Laatste stap · " + fmtRange(now.vanaf, now.datum)), /* @__PURE__ */ React.createElement("span", { className: "now-title" }, now.titel))) : /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, d.aiEnabled ? "Houd je bouwproces bij in korte stappen. De assistent houdt er een samenvatting van bij." : "Houd je bouwproces bij in korte stappen, met een samenvatting erbij."), /* @__PURE__ */ React.createElement(Btn, { kind: "ghost", icon: "plus", onClick: () => ui.openSheet("step", { carId: car.id }) }, "Bouwstap toevoegen")), /* @__PURE__ */ React.createElement("section", { className: "card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, "Laatste facturen"), inv.length > 0 && /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => go("kosten") }, "Alle ", inv.length)), inv.length ? /* @__PURE__ */ React.createElement("div", { className: "rows" }, inv.filter((i) => i.totaal).slice(0, 3).map((i) => /* @__PURE__ */ React.createElement(InvoiceRow, { key: i.id, inv: i }))) : /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, "Nog geen facturen. Scan een bon of voeg kosten zelf toe."))), /* @__PURE__ */ React.createElement("div", { className: "ov-side" }, /* @__PURE__ */ React.createElement(DealBanner, { carId: car.id }), /* @__PURE__ */ React.createElement("section", { className: "card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, car.planNaam || "Plannen"), /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => go("plannen") }, ps.count ? "Lijst" : "Begin een lijst")), ps.count ? /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement("div", { className: "plan-mini" }, /* @__PURE__ */ React.createElement("span", { className: "plan-mini-value" }, !ps.nodig && ps.onbekend ? ps.open + " nog kopen" : eurR(ps.nodig)), /* @__PURE__ */ React.createElement("span", { className: "muted" }, !ps.nodig && ps.onbekend ? "nog zonder prijs · " + ps.binnen + " gekocht" : "nog nodig · " + ps.open + " nog kopen · " + ps.binnen + " gekocht")), /* @__PURE__ */ React.createElement(Bar, { pct: ps.pct }), ps.budget !== null && /* @__PURE__ */ React.createElement("p", { className: "small " + (ps.ruimte < 0 ? "tone-danger" : "muted") }, ps.ruimte < 0 ? eurR(-ps.ruimte) + " boven budget" : "Budget " + eurR(ps.budget) + " · " + eurR(ps.ruimte) + " ruimte")) : /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, "Zet onderdelen die je wilt of moet hebben op één lijst per categorie, met prijs en link.")), /* @__PURE__ */ React.createElement("section", { className: "card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, "Advies"), /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => go("advies") }, adv ? "Alle " + (adv.kaarten || []).length : "Bekijk")), adv && adv.kaarten && adv.kaarten.length ? /* @__PURE__ */ React.createElement(AdviceCard, { k: adv.kaarten[0], car, compact: true }) : /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, d.aiEnabled ? "Laat de assistent naar deze auto kijken: wat goed gaat, waar je op moet letten en wat je beter niet kunt doen." : "Vaste controles voor deze auto: APK, verzekering, onderhoud en je plannen."))));
  }
  function GegevensTab({ car }) {
    const [view, setView] = React.useState("voertuig");
    return /* @__PURE__ */ React.createElement("div", { className: "stack" }, /* @__PURE__ */ React.createElement(
      Seg,
      {
        label: "Gegevens",
        options: [
          ["voertuig", "Voertuig"],
          ["apk", "APK-historie"]
        ],
        value: view,
        onChange: setView
      }
    ), view === "voertuig" ? /* @__PURE__ */ React.createElement(Voertuig, { car }) : /* @__PURE__ */ React.createElement(ApkHistorie, { car }));
  }
  var pk = (kw) => Math.round(kw * 1.35962);
  function Voertuig({ car }) {
    const d = useData();
    const ui = useUi();
    const r = car.rdw || null;
    const mods = d.modsOf(car.id);
    const waiting = car.rdwStatus === "wachten";
    const rdwAuto = d.aiEnabled || RDW_DIRECT;
    async function refresh() {
      if (!car.kenteken) return ui.openSheet("car", { id: car.id, focus: "kenteken" });
      await d.act.patch("cars", car.id, { rdwStatus: "wachten" });
      if (RDW_DIRECT) {
        const res = await syncRdw(d.act, car, { quiet: true });
        return ui.toast(res.ok ? "RDW-gegevens bijgewerkt" : RDW_COPY[res.code] || "Ophalen lukte niet.", res.ok ? void 0 : "error");
      }
      await d.act.queue({ type: "rdw", carId: car.id, kenteken: car.kenteken });
      ui.toast(d.settings.helperTriggerId ? "De assistent haalt de RDW-gegevens op" : "Klaargezet voor de assistent");
    }
    const rows = [];
    if (r) {
      const add = (k, v, tone) => v && rows.push([k, v, tone]);
      add("Merk en model", merkModel(r));
      add("Kleur · inrichting", [r.kleur ? niceLower(r.kleur) : "", r.inrichting ? String(r.inrichting).toLowerCase() : ""].filter(Boolean).join(" · "));
      add("Motor", [r.cilinderinhoud ? num(r.cilinderinhoud) + " cc" : "", r.cilinders ? r.cilinders + " cil." : "", r.brandstof ? String(r.brandstof).toLowerCase() : ""].filter(Boolean).join(" · "));
      add("Vermogen", r.vermogenKw ? num(r.vermogenKw) + " kW · " + pk(r.vermogenKw) + " pk" : "");
      add("Gewicht rijklaar", r.massaRijklaar ? num(r.massaRijklaar) + " kg" : "");
      add("Emissie · verbruik", [r.emissie, r.verbruik ? String(r.verbruik).replace(".", ",") + " l/100 km" : ""].filter(Boolean).join(" · "));
      add("Eerste toelating", r.datumEersteToelating ? fmtDate(r.datumEersteToelating) : "");
      add("Op jouw naam", r.datumTenaamstelling ? "sinds " + fmtDate(r.datumTenaamstelling) : "");
      if (r.vervaldatumApk) {
        const dd = daysUntil(r.vervaldatumApk);
        add("APK", (dd < 0 ? "Verlopen " : "Tot ") + fmtDate(r.vervaldatumApk), dd < 0 ? "danger" : dd <= 60 ? "warn" : null);
      }
      if (r.wamVerzekerd === true || r.wamVerzekerd === false) add("WAM-verzekering", r.wamVerzekerd ? "Verzekerd" : "Niet verzekerd", r.wamVerzekerd ? null : "danger");
      add("Tellerstand", r.tellerstandOordeel ? r.tellerstandOordeel + (r.tellerstandJaar ? " (" + r.tellerstandJaar + ")" : "") : "");
      if (r.terugroepactie === "Ja") add("Terugroepactie", "Openstaand", "warn");
    }
    if (!r && car.apkTot) {
      const dd = daysUntil(car.apkTot);
      rows.push(["APK (zelf ingevuld)", (dd < 0 ? "Verlopen " : "Tot ") + fmtDate(car.apkTot), dd < 0 ? "danger" : dd <= 60 ? "warn" : null]);
    }
    return /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement("section", { className: "card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, "Kentekenregister (RDW)"), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, waiting && RDW_DIRECT ? "wordt opgehaald…" : waiting && d.aiEnabled ? d.settings.helperTriggerId ? "wordt opgehaald…" : "wacht op de assistent" : r && r.fetchedAt ? "bijgewerkt " + relTime(r.fetchedAt) : "")), rows.length ? /* @__PURE__ */ React.createElement("dl", { className: "kv" }, rows.map(([k, v, tone]) => /* @__PURE__ */ React.createElement("div", { key: k, className: "kv-row" }, /* @__PURE__ */ React.createElement("dt", null, k), /* @__PURE__ */ React.createElement("dd", { className: tone ? "tone-" + tone : "" }, v)))) : /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, !rdwAuto ? "Zonder AI haalt de app geen gegevens bij de RDW op. Zoek het kenteken op bij de RDW en vul de APK-datum in bij Auto bewerken; dan waarschuwt de app op tijd." : !car.kenteken ? RDW_DIRECT ? "Vul een kenteken in, dan haalt de app de gegevens bij de RDW op. Zonder kenteken kun je de APK-datum zelf invullen." : "Vul een kenteken in, dan haalt de assistent de gegevens bij de RDW op." : waiting ? RDW_DIRECT ? "De gegevens worden opgehaald…" : "De assistent haalt de gegevens op. Dat duurt meestal een paar minuten; je kunt gewoon verder." : RDW_DIRECT && car.rdwStatus === "fout" ? "De RDW was niet bereikbaar. Probeer het zo opnieuw." : car.rdwStatus === "niet-gevonden" ? car.status === "verkocht" ? "De RDW heeft geen gegevens meer over dit kenteken. Een auto die gesloopt of geëxporteerd is, staat niet meer in het register." : "De RDW kent dit kenteken niet. Controleer het kenteken." : "Nog geen gegevens van de RDW."), !rdwAuto || RDW_DIRECT && !r && (!car.kenteken || car.rdwStatus === "niet-gevonden") ? /* @__PURE__ */ React.createElement("div", { className: "card-foot row-gap" }, /* @__PURE__ */ React.createElement("a", { className: "btn btn-ghost btn-small", href: RDW_LOOKUP, target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement(Icon, { name: "search", size: 16 }), /* @__PURE__ */ React.createElement("span", null, "Bekijk bij de RDW")), /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: "edit", onClick: () => ui.openSheet("car", { id: car.id, focus: "apk" }) }, "APK-datum invullen")) : !(car.status === "verkocht" && car.rdwStatus === "niet-gevonden") && /* @__PURE__ */ React.createElement("div", { className: "card-foot" }, /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: "refresh", onClick: refresh, disabled: waiting }, car.kenteken ? "Opnieuw ophalen" : "Kenteken invullen"))), /* @__PURE__ */ React.createElement("section", { className: "card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, "Prestaties: fabriek en nu"), /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: "edit", onClick: () => ui.openSheet("specs", { carId: car.id }) }, "Bewerken")), /* @__PURE__ */ React.createElement("div", { className: "spec-table", role: "table", "aria-label": "Prestaties" }, /* @__PURE__ */ React.createElement("div", { className: "spec-row spec-head", role: "row" }, /* @__PURE__ */ React.createElement("span", { role: "columnheader" }), /* @__PURE__ */ React.createElement("span", { role: "columnheader" }, "Fabriek"), /* @__PURE__ */ React.createElement("span", { role: "columnheader" }, "Nu")), SPECS.map(([k, l]) => {
      const s = (car.specs || {})[k] || {};
      return /* @__PURE__ */ React.createElement("div", { className: "spec-row", role: "row", key: k }, /* @__PURE__ */ React.createElement("span", { role: "cell", className: "spec-label" }, l), /* @__PURE__ */ React.createElement("span", { role: "cell", className: "spec-f" }, s.fabriek || "–"), /* @__PURE__ */ React.createElement("span", { role: "cell", className: "spec-n" }, s.nu ? /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement("strong", null, s.nu), /* @__PURE__ */ React.createElement("span", { className: "spec-own" }, "eigen", s.notitie ? " · " + s.notitie : "")) : /* @__PURE__ */ React.createElement("span", { className: "muted" }, "–")));
    })), /* @__PURE__ */ React.createElement("p", { className: "hint" }, "Fabriek is de fabrieksopgave. Koppel en 0–100 staan niet in het RDW-register; wat jij aanpast staat onder Nu.")), /* @__PURE__ */ React.createElement("section", { className: "card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, "Aanpassingen en bijzonderheden"), /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: "plus", onClick: () => ui.openSheet("mod", { carId: car.id }) }, "Toevoegen")), mods.length ? /* @__PURE__ */ React.createElement("div", { className: "rows" }, mods.map((m) => /* @__PURE__ */ React.createElement("button", { type: "button", key: m.id, className: "list-row", onClick: () => ui.openSheet("mod", { id: m.id }) }, /* @__PURE__ */ React.createElement("span", { className: "list-row-main" }, /* @__PURE__ */ React.createElement("span", { className: "list-row-title" }, m.titel), /* @__PURE__ */ React.createElement("span", { className: "list-row-sub" }, [m.datum ? fmtMonth(m.datum) : null, m.notitie].filter(Boolean).join(" · "))), /* @__PURE__ */ React.createElement(Icon, { name: "next", size: 18, className: "chev" })))) : /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, "Bijvoorbeeld een ander onderstel, grotere remmen of een motorswap. Ze komen ook op de verkoopsamenvatting.")));
  }
  function ApkHistorie({ car }) {
    const d = useData();
    const ui = useUi();
    const list = mergeApk(car, d.apkNotesOf(car.id));
    const r = car.rdw || {};
    const dd = r.vervaldatumApk ? daysUntil(r.vervaldatumApk) : null;
    return /* @__PURE__ */ React.createElement(React.Fragment, null, r.vervaldatumApk && /* @__PURE__ */ React.createElement("div", { className: "alert-card tone-card-" + (dd < 0 ? "danger" : dd <= 60 ? "warn" : "ok") }, /* @__PURE__ */ React.createElement(Icon, { name: dd < 0 || dd <= 60 ? "alert" : "okCircle", size: 20 }), /* @__PURE__ */ React.createElement("div", null, /* @__PURE__ */ React.createElement("strong", null, dd < 0 ? "APK verlopen" : "APK geldig"), /* @__PURE__ */ React.createElement("p", null, dd < 0 ? "Vervaldatum " + fmtDate(r.vervaldatumApk) + " · " + plural(-dd, "dag", "dagen") + " geleden" : "Tot " + fmtDate(r.vervaldatumApk) + " · nog " + plural(dd, "dag", "dagen")))), /* @__PURE__ */ React.createElement("div", { className: "row-between" }, /* @__PURE__ */ React.createElement("h3", { className: "section-title" }, "Keuringen"), /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: "plus", onClick: () => ui.openSheet("apk", { carId: car.id, soort: "keuring" }) }, "Keuring toevoegen")), list.length === 0 ? /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, car.rdwStatus === "niet-gevonden" ? "De RDW heeft van deze auto geen keuringen meer." : car.kenteken ? "Nog geen keuringen bekend. Zodra de RDW-gegevens binnen zijn, staan ze hier." : "Vul een kenteken in om de keuringen van de RDW te zien.", " Je kunt ook zelf een keuring toevoegen.") : list.map((k) => /* @__PURE__ */ React.createElement("section", { className: "card apk-card", key: k.datum }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, fmtDate(k.datum)), /* @__PURE__ */ React.createElement("span", { className: "pill " + (k.aantalGebreken || k.uitslag === "Afgekeurd" ? "pill-warn" : k.uitslag === "Alleen advies" ? "pill-info" : "pill-ok") }, k.uitslag)), k.soort && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, k.soort, k.vervaldatum ? " · geldig tot " + fmtDate(k.vervaldatum) : ""), k.gebreken.length > 0 && /* @__PURE__ */ React.createElement("ul", { className: "defects" }, k.gebreken.map((g, i) => /* @__PURE__ */ React.createElement("li", { key: i }, g.code && /* @__PURE__ */ React.createElement("span", { className: "defect-code" + (isAdvies(g) ? " is-advies" : "") }, isAdvies(g) ? "advies" : g.code), /* @__PURE__ */ React.createElement("span", null, g.omschrijving || "Gebrek", Number(g.aantal) > 1 ? " (" + g.aantal + "×)" : "")))), (k.eigen.length > 0 || k.keuring && k.keuring.tekst) && /* @__PURE__ */ React.createElement("div", { className: "own-notes" }, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, "Eigen bevindingen"), k.keuring && k.keuring.tekst && /* @__PURE__ */ React.createElement("button", { type: "button", className: "own-note", onClick: () => ui.openSheet("apk", { id: k.keuring.id }) }, k.keuring.tekst), k.eigen.map((n) => /* @__PURE__ */ React.createElement("button", { type: "button", className: "own-note", key: n.id, onClick: () => ui.openSheet("apk", { id: n.id }) }, n.tekst))), /* @__PURE__ */ React.createElement("div", { className: "card-foot" }, /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: "plus", onClick: () => ui.openSheet("apk", { carId: car.id, soort: "bevinding", datum: k.datum }) }, "Bevinding toevoegen"), k.keuring && !k.keuring.tekst && /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: "edit", onClick: () => ui.openSheet("apk", { id: k.keuring.id }) }, "Bewerken")))));
  }
  function InvoiceRow({ inv, showCar }) {
    const d = useData();
    const ui = useUi();
    const car = showCar && inv.carId ? d.car(inv.carId) : null;
    const zek = inv.ai && inv.ai.gekozen === "assistent" ? inv.ai.zekerheid : null;
    return /* @__PURE__ */ React.createElement("button", { type: "button", className: "list-row", onClick: () => ui.openSheet("invoice", { id: inv.id }) }, inv.file && inv.file.type === "image" ? /* @__PURE__ */ React.createElement(Photo, { id: inv.file.id, className: "row-thumb" }) : inv.file && inv.file.thumb ? /* @__PURE__ */ React.createElement(Photo, { id: inv.file.thumb, className: "row-thumb" }) : /* @__PURE__ */ React.createElement("span", { className: "row-thumb row-thumb-empty", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement(Icon, { name: inv.file || inv.driveFileId ? "doc" : "euro", size: 18 })), /* @__PURE__ */ React.createElement("span", { className: "list-row-main" }, /* @__PURE__ */ React.createElement("span", { className: "list-row-title" }, inv.omschrijving || inv.leverancier || "Factuur"), /* @__PURE__ */ React.createElement("span", { className: "list-row-sub" }, zek && zek !== "zeker" && /* @__PURE__ */ React.createElement("span", { className: "zek zek-" + zek }, zek), [fmtDate(inv.datum), inv.leverancier && inv.omschrijving ? inv.leverancier : null, car ? car.naam : null].filter(Boolean).join(" · "))), /* @__PURE__ */ React.createElement("span", { className: "list-row-amount" }, eur(inv.totaal)));
  }
  function CategoryBars({ invoices, limit = 6 }) {
    const [all, setAll] = React.useState(false);
    const byCat = {};
    for (const i of invoices) byCat[i.categorie || "overig"] = (byCat[i.categorie || "overig"] || 0) + (i.totaal || 0);
    const rows = Object.entries(byCat).sort((a, b) => b[1] - a[1]);
    if (!rows.length) return null;
    const max = rows[0][1] || 1;
    const shown = all ? rows : rows.slice(0, limit);
    const rest = rows.slice(limit);
    return /* @__PURE__ */ React.createElement("div", { className: "cat-bars" }, shown.map(([k, v]) => /* @__PURE__ */ React.createElement("div", { className: "cat-row", key: k }, /* @__PURE__ */ React.createElement("span", { className: "cat-name" }, catLabel(k)), /* @__PURE__ */ React.createElement("span", { className: "cat-amount" }, eurR(v)), /* @__PURE__ */ React.createElement(Bar, { pct: v / max }))), !all && rest.length > 0 && /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => setAll(true) }, plural(rest.length, "andere categorie", "andere categorieën"), " · ", eurR(sum(rest, (x) => x[1]))));
  }
  function KostenTab({ car }) {
    const d = useData();
    const ui = useUi();
    const inv = d.invoicesOf(car.id);
    const [year, setYear] = React.useState("alle");
    const total = sum(inv, (i) => i.totaal);
    const years = Array.from(new Set(inv.map((i) => yearOf(i.datum)).filter(Boolean))).sort().reverse();
    const list = year === "alle" ? inv : inv.filter((i) => yearOf(i.datum) === year);
    const zekOf = (i) => i.ai && i.ai.gekozen === "assistent" ? i.ai.zekerheid : "zeker";
    const showZek = inv.some((i) => zekOf(i) !== "zeker");
    const zekSum = (z) => sum(inv.filter((i) => zekOf(i) === z), (i) => i.totaal);
    return /* @__PURE__ */ React.createElement("div", { className: "stack" }, /* @__PURE__ */ React.createElement("section", { className: "card total-card" }, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, "Totaal uitgegeven"), /* @__PURE__ */ React.createElement("span", { className: "total-value" }, eur(total)), /* @__PURE__ */ React.createElement("span", { className: "muted" }, plural(inv.length, "factuur", "facturen"), years.length ? " · " + (years.length > 1 ? years[years.length - 1] + "–" + years[0] : years[0]) : ""), years.length > 0 && /* @__PURE__ */ React.createElement("div", { className: "year-grid" }, years.slice(0, 3).map((y) => /* @__PURE__ */ React.createElement("div", { key: y, className: "year-cell" }, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, y === d.thisYear ? y + " tot nu" : y), /* @__PURE__ */ React.createElement("span", { className: "year-value" }, eurR(sum(inv.filter((i) => yearOf(i.datum) === y), (i) => i.totaal)))))), showZek && /* @__PURE__ */ React.createElement("div", { className: "zek-grid", "aria-label": "Toegewezen door de assistent" }, [
      ["zeker", "Zeker"],
      ["waarschijnlijk", "Waarschijnlijk"],
      ["gok", "Gok"]
    ].map(([z, l]) => /* @__PURE__ */ React.createElement("div", { key: z, className: "zek-cell" }, /* @__PURE__ */ React.createElement("span", { className: "zek-dot zek-bg-" + z, "aria-hidden": "true" }), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, l), /* @__PURE__ */ React.createElement("span", { className: "zek-value" }, eurR(zekSum(z))))))), inv.length > 0 && /* @__PURE__ */ React.createElement(Section, { title: "Per categorie", right: /* @__PURE__ */ React.createElement("span", { className: "small muted" }, plural(new Set(inv.map((i) => i.categorie)).size, "categorie", "categorieën")) }, /* @__PURE__ */ React.createElement("section", { className: "card" }, /* @__PURE__ */ React.createElement(CategoryBars, { invoices: inv }))), /* @__PURE__ */ React.createElement(
      Section,
      {
        title: "Facturen",
        right: /* @__PURE__ */ React.createElement("span", { className: "row-gap" }, /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: "plus", onClick: () => ui.openSheet("invoice", { carId: car.id }) }, "Kosten"), /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "primary", icon: "camera", onClick: () => ui.openSheet("scan", { carId: car.id }) }, "Scan"))
      },
      years.length > 1 && /* @__PURE__ */ React.createElement("div", { className: "chips", role: "group", "aria-label": "Jaar" }, ["alle"].concat(years).map((y) => /* @__PURE__ */ React.createElement("button", { key: y, type: "button", className: "chip" + (year === y ? " is-on" : ""), "aria-pressed": year === y, onClick: () => setYear(y) }, y === "alle" ? "Alle" : y))),
      list.length ? /* @__PURE__ */ React.createElement("div", { className: "list-card" }, list.map((i) => /* @__PURE__ */ React.createElement(InvoiceRow, { key: i.id, inv: i }))) : /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, "Nog geen facturen voor deze auto.")
    ), car.driveFolderId && /* @__PURE__ */ React.createElement("a", { className: "link-row", href: "https://drive.google.com/drive/folders/" + car.driveFolderId, target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement(Icon, { name: "cloud", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Map in Google Drive"), /* @__PURE__ */ React.createElement(Icon, { name: "out", size: 16 })));
  }
  async function refreshSummary(getD, ui, carId) {
    if (ui.ai.disabled) {
      const d = getD();
      const car = d.car(carId);
      if (!car) return;
      const steps = d.stepsOf(carId);
      const tekst = steps.length ? localSummary(car, steps, d.modsOf(carId)) : "";
      await d.act.patch("cars", carId, { samenvatting: tekst ? { tekst, at: nowISO(), stappen: steps.length, bron: "sjabloon" } : null }, true);
      return;
    }
    const sample = ui.ai.sample;
    if (!sample || ui.ai.off) return;
    const key = "sum-" + carId;
    if (ui.isBusy(key)) return;
    ui.setBusy(key, true);
    try {
      const d = getD();
      const car = d.car(carId);
      if (!car) return;
      const steps = d.stepsOf(carId);
      if (!steps.length) {
        await d.act.patch("cars", carId, { samenvatting: null }, true);
        return;
      }
      const tekst = await buildSummary(sample, { car, steps, mods: d.modsOf(carId) });
      await getD().act.patch("cars", carId, { samenvatting: { tekst, at: nowISO(), stappen: steps.length } }, true);
    } catch (e) {
      if (aiHidden(e)) ui.setAiOff();
      const msg = aiErrorCopy(e);
      if (msg) ui.toast("Samenvatting bijwerken: " + msg.charAt(0).toLowerCase() + msg.slice(1), "error");
    } finally {
      ui.setBusy(key, false);
    }
  }
  function BouwlogTab({ car }) {
    const d = useData();
    const ui = useUi();
    const steps = d.stepsOf(car.id);
    const [filter, setFilter] = React.useState("alles");
    const shown = filter === "mijlpalen" ? steps.filter((s2) => s2.mijlpaal || s2.bezig) : steps;
    const busy = ui.busy["sum-" + car.id];
    const s = car.samenvatting;
    const stale = s && s.stappen !== steps.length;
    const aiOn = ui.ai.sample && !ui.ai.off;
    return /* @__PURE__ */ React.createElement("div", { className: "stack" }, (steps.length > 0 || s) && /* @__PURE__ */ React.createElement("section", { className: "card summary-card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, "Samenvatting ", /* @__PURE__ */ React.createElement("span", { className: "muted small" }, s && s.bron === "sjabloon" ? "uit je bouwstappen" : "door de assistent")), (aiOn || ui.ai.disabled) && /* @__PURE__ */ React.createElement(IconBtn, { icon: "refresh", label: "Samenvatting bijwerken", className: busy ? "is-spinning" : "", onClick: () => refreshSummary(ui.latest, ui, car.id), size: 18 })), s && s.tekst ? /* @__PURE__ */ React.createElement("p", { className: "body-text" }, s.tekst) : /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, busy ? "De assistent schrijft de samenvatting…" : aiOn || ui.ai.disabled ? "Nog geen samenvatting." : "De assistent is hier niet beschikbaar."), /* @__PURE__ */ React.createElement("p", { className: "small muted" }, busy ? "Wordt bijgewerkt…" : s && s.at ? "Bijgewerkt " + relTime(s.at) + (stale ? " · er zijn nieuwe stappen" : "") : ""), (aiOn || ui.ai.disabled) && !s && !busy && /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "secondary", icon: aiOn ? "spark" : "refresh", onClick: () => refreshSummary(ui.latest, ui, car.id) }, "Samenvatting maken")), /* @__PURE__ */ React.createElement("div", { className: "row-between" }, /* @__PURE__ */ React.createElement(Btn, { kind: "primary", icon: "plus", onClick: () => ui.openSheet("step", { carId: car.id }) }, "Bouwstap"), steps.length > 0 && /* @__PURE__ */ React.createElement(
      Seg,
      {
        small: true,
        label: "Filter",
        options: [
          ["alles", "Alles"],
          ["mijlpalen", "Mijlpalen"]
        ],
        value: filter,
        onChange: setFilter
      }
    )), steps.length === 0 ? /* @__PURE__ */ React.createElement(
      Empty,
      {
        art: "wrench",
        title: "Nog geen bouwstappen",
        text: aiOn ? "Schrijf na een klus kort op wat je gedaan hebt. De assistent maakt er één regel van en houdt de samenvatting bij, zodat je bouwlog kort en overzichtelijk blijft." : "Schrijf na een klus een korte titel en wat je gedaan hebt. De samenvatting wordt bijgehouden uit je stappen."
      }
    ) : /* @__PURE__ */ React.createElement("ol", { className: "timeline" }, shown.map((st) => /* @__PURE__ */ React.createElement("li", { key: st.id, className: "tl-item" + (st.mijlpaal ? " is-mile" : "") + (st.bezig ? " is-now" : "") }, /* @__PURE__ */ React.createElement("span", { className: "tl-dot", "aria-hidden": "true" }), /* @__PURE__ */ React.createElement("button", { type: "button", className: "tl-body", onClick: () => ui.openSheet("step", { id: st.id }) }, /* @__PURE__ */ React.createElement("span", { className: "tl-date" }, st.bezig ? "Nu bezig" : fmtRange(st.vanaf, st.datum)), /* @__PURE__ */ React.createElement("span", { className: "tl-title" }, st.titel || "Bouwstap"), st.tekst && /* @__PURE__ */ React.createElement("span", { className: "tl-text" }, st.tekst)))), car.aankoopdatum && filter === "alles" && /* @__PURE__ */ React.createElement("li", { className: "tl-item is-start" }, /* @__PURE__ */ React.createElement("span", { className: "tl-dot", "aria-hidden": "true" }), /* @__PURE__ */ React.createElement("div", { className: "tl-body" }, /* @__PURE__ */ React.createElement("span", { className: "tl-date" }, fmtMonth(car.aankoopdatum)), /* @__PURE__ */ React.createElement("span", { className: "tl-title" }, "Gekocht"), /* @__PURE__ */ React.createElement("span", { className: "tl-text" }, "Begin van het paspoort")))));
  }
  var WOORD = { nog: "nog kopen", besteld: "gekocht, onderweg", binnen: "gekocht" };
  function PartRow({ p, first }) {
    const d = useData();
    const ui = useUi();
    const status = p.status || "nog";
    const binnen = status === "binnen";
    const deal = p.deal && !binnen;
    const winkel = p.winkel || (p.link ? hostOf(p.link) : "");
    let sub;
    const metFactuur = (p.facturen || []).length > 0;
    if (p.linkStatus === "wachten" && !binnen && d.aiEnabled) sub = d.settings.helperTriggerId ? "Link wordt gelezen door de assistent…" : "Link wacht op de assistent";
    else if (binnen) sub = ["Gekocht", winkel, metFactuur ? "factuur gekoppeld" : null].filter(Boolean).join(" · ");
    else if (status === "besteld") sub = ["Gekocht, onderweg", winkel, metFactuur ? "factuur gekoppeld" : null].filter(Boolean).join(" · ");
    else if (deal) sub = "Aanbieding" + (winkel ? " bij " + winkel : "") + (p.deal.pct ? " · −" + Math.round(p.deal.pct) + "%" : "");
    else sub = [p.prioriteit === "wil" ? "Wil" : "Moet", winkel, p.volgen && p.link ? "prijs gevolgd" : null].filter(Boolean).join(" · ");
    const qty = p.aantal > 1 ? p.aantal + "× " : "";
    return /* @__PURE__ */ React.createElement("div", { className: "part-row" + (first ? "" : " has-line") }, /* @__PURE__ */ React.createElement("span", { className: "status-btn", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement("span", { className: "status-dot is-" + status }, binnen && /* @__PURE__ */ React.createElement(Icon, { name: "check", size: 13, stroke: 3.2 }))), /* @__PURE__ */ React.createElement("button", { type: "button", className: "part-main", "aria-label": (p.naam || "Onderdeel") + ", " + WOORD[status] + ". Open het onderdeel", onClick: () => ui.openSheet("part", { id: p.id }) }, /* @__PURE__ */ React.createElement("span", { className: "part-text" }, /* @__PURE__ */ React.createElement("span", { className: "part-name" + (binnen ? " is-done" : "") }, p.naam || "Onderdeel"), /* @__PURE__ */ React.createElement("span", { className: "part-sub" + (deal ? " tone-accent" : "") }, sub)), /* @__PURE__ */ React.createElement("span", { className: "part-price" + (deal ? " tone-accent" : binnen ? " is-done" : "") }, typeof p.prijs === "number" ? qty + eurR(p.prijs * (p.aantal || 1)) : /* @__PURE__ */ React.createElement("span", { className: "muted" }, "prijs ?"))));
  }
  function PlannenTab({ car }) {
    const d = useData();
    const ui = useUi();
    const parts = d.partsOf(car.id);
    const [filter, setFilter] = React.useState("alles");
    const ps = planStats(parts, car.budget);
    const visible = (p) => filter === "alles" || (filter === "binnen" ? p.status === "binnen" : p.status !== "binnen");
    const groups = CATS.map(([id, naam]) => {
      const inCat = parts.filter((p) => (p.categorie || "overig") === id);
      const list = inCat.filter(visible).sort((a, b) => (a.status === "binnen") - (b.status === "binnen") || (a.prioriteit === "wil") - (b.prioriteit === "wil"));
      const open = inCat.filter((p) => p.status !== "binnen");
      const openSum = sum(open, (p) => typeof p.prijs === "number" ? p.prijs * (p.aantal || 1) : 0);
      const binnenSum = sum(inCat.filter((p) => p.status === "binnen"), (p) => typeof p.prijs === "number" ? p.prijs * (p.aantal || 1) : 0);
      const right = filter === "binnen" ? binnenSum ? eurR(binnenSum) : "" : open.length === 0 ? "alles gekocht" : open.length + " nodig" + (openSum ? " · " + eurR(openSum) : "");
      return { id, naam, list, right };
    }).filter((g) => g.list.length);
    return /* @__PURE__ */ React.createElement("div", { className: "stack" }, /* @__PURE__ */ React.createElement("section", { className: "card plan-card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, car.planNaam || "Plannen"), /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => ui.openSheet("budget", { carId: car.id }) }, ps.budget !== null ? /* @__PURE__ */ React.createElement(React.Fragment, null, "Budget ", /* @__PURE__ */ React.createElement("strong", null, eurR(ps.budget))) : "Budget instellen")), /* @__PURE__ */ React.createElement("div", { className: "plan-stats" }, /* @__PURE__ */ React.createElement("div", null, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, "Nog nodig"), /* @__PURE__ */ React.createElement("span", { className: "plan-value" }, !ps.nodig && ps.onbekend ? "–" : eurR(ps.nodig), ps.onbekend && ps.nodig ? /* @__PURE__ */ React.createElement("span", { className: "muted small" }, " + ", ps.onbekend, " zonder prijs") : null), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, !ps.nodig && ps.onbekend ? plural(ps.onbekend, "onderdeel", "onderdelen") + " nog zonder prijs" : "moet " + eurR(ps.moet) + " · wil " + eurR(ps.wil))), /* @__PURE__ */ React.createElement("div", null, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, "Gekocht"), /* @__PURE__ */ React.createElement("span", { className: "plan-value" }, eurR(ps.binnenSom)), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, ps.binnen, " van ", plural(ps.count, "onderdeel", "onderdelen")))), /* @__PURE__ */ React.createElement(Bar, { pct: ps.pct }), ps.budget !== null && /* @__PURE__ */ React.createElement("p", { className: "small " + (ps.ruimte < 0 ? "tone-danger" : "muted") }, ps.ruimte < 0 ? "Het plan is " + eurR(-ps.ruimte) + " duurder dan je budget" : "Binnen budget · " + eurR(ps.ruimte) + " ruimte")), /* @__PURE__ */ React.createElement(DealBanner, { carId: car.id }), parts.length > 0 && /* @__PURE__ */ React.createElement(
      Seg,
      {
        label: "Filter",
        options: [
          ["alles", "Alles"],
          ["nog", "Nog kopen"],
          ["binnen", "Gekocht"]
        ],
        value: filter,
        onChange: setFilter
      }
    ), parts.length === 0 ? /* @__PURE__ */ React.createElement(
      Empty,
      {
        art: "list",
        title: "Nog geen onderdelen op de lijst",
        text: d.aiEnabled ? "Plak de link van een product of typ een naam. De assistent leest prijs en winkel van de pagina" + (IS_TEST ? "." : " en houdt de prijs elke ochtend in de gaten.") : "Typ de naam van een onderdeel met de prijs, en plak eventueel de link naar de winkel. Gekocht? Open het onderdeel en tik op Gekocht."
      }
    ) : groups.length === 0 ? /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, "Niets in deze selectie.") : groups.map((g) => /* @__PURE__ */ React.createElement("section", { className: "group", key: g.id }, /* @__PURE__ */ React.createElement("div", { className: "group-head" }, /* @__PURE__ */ React.createElement("h3", null, g.naam), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, g.right)), /* @__PURE__ */ React.createElement("div", { className: "list-card" }, g.list.map((p, i) => /* @__PURE__ */ React.createElement(PartRow, { key: p.id, p, first: i === 0 }))))), /* @__PURE__ */ React.createElement("button", { type: "button", className: "dashed-btn", onClick: () => ui.openSheet("part", { carId: car.id }) }, /* @__PURE__ */ React.createElement(Icon, { name: "plus", size: 16, stroke: 2.4 }), "Onderdeel toevoegen"));
  }
  var SOORT = {
    "let-op": ["Let op", "danger", "alert"],
    tip: ["Tip", "info", "spark"],
    afgeraden: ["Afgeraden", "warn", "close"],
    goed: ["Goed bezig", "ok", "okCircle"],
    onderhoud: ["Onderhoud", "muted", "wrench"]
  };
  function AdviceCard({ k, car, compact }) {
    const d = useData();
    const ui = useUi();
    const [label, tone, icon] = SOORT[k.soort] || SOORT.tip;
    const onList = k.onderdeel && d.partsOf(car.id).some((p) => (p.naam || "").toLowerCase() === k.onderdeel.naam.toLowerCase());
    async function addPart() {
      const id = await d.act.add("parts", "p", { carId: car.id, naam: k.onderdeel.naam, categorie: k.onderdeel.categorie || "overig", prioriteit: "wil", status: "nog", aantal: 1, volgen: false, bron: "advies" });
      if (id) ui.toast("Op je lijst gezet");
    }
    return /* @__PURE__ */ React.createElement("div", { className: "advice" + (compact ? " is-compact" : "") + " advice-" + tone }, /* @__PURE__ */ React.createElement("span", { className: "advice-label tone-" + tone }, /* @__PURE__ */ React.createElement(Icon, { name: icon, size: 14, stroke: 2.4 }), label), /* @__PURE__ */ React.createElement("strong", { className: "advice-title" }, k.titel), k.tekst && /* @__PURE__ */ React.createElement("p", { className: "advice-text" }, k.tekst), !compact && k.onderdeel && /* @__PURE__ */ React.createElement("div", { className: "advice-actions" }, onList ? /* @__PURE__ */ React.createElement("span", { className: "small tone-ok" }, "Staat op je lijst") : /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "secondary", icon: "plus", onClick: addPart }, clip(k.onderdeel.naam, 34), " op de lijst")));
  }
  function AdviesTab({ car }) {
    const d = useData();
    const ui = useUi();
    const adv = d.adviceOf(car.id);
    const [busy, setBusy] = React.useState(false);
    const [err, setErr] = React.useState(null);
    const ctl = React.useRef(null);
    const aiOn = ui.ai.sample && !ui.ai.off;
    React.useEffect(() => () => ctl.current && ctl.current.abort(), []);
    if (ui.ai.disabled) {
      const loc = localAdvice(d, car);
      return /* @__PURE__ */ React.createElement("div", { className: "stack" }, /* @__PURE__ */ React.createElement("section", { className: "card summary-card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, "Stand van zaken"), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, "vaste controles")), /* @__PURE__ */ React.createElement("p", { className: "body-text" }, loc.stand)), loc.kaarten.map((k, i) => /* @__PURE__ */ React.createElement(AdviceCard, { key: i, k, car })), /* @__PURE__ */ React.createElement("p", { className: "hint" }, "Zonder AI controleert de app APK, verzekering, onderhoud, terugkerende APK-punten en je plan. Advies op maat is er niet."));
    }
    async function run() {
      setBusy(true);
      setErr(null);
      ctl.current = new AbortController();
      try {
        const res = await makeAdvice(ui.ai.sample, { d, car, signal: ctl.current.signal });
        await d.act.put("advice", car.id, Object.assign(res, { at: nowISO(), basis: { facturen: d.invoicesOf(car.id).length, stappen: d.stepsOf(car.id).length, onderdelen: d.partsOf(car.id).length } }));
      } catch (e) {
        if (aiHidden(e)) ui.setAiOff();
        setErr(aiErrorCopy(e));
      } finally {
        setBusy(false);
      }
    }
    return /* @__PURE__ */ React.createElement("div", { className: "stack" }, /* @__PURE__ */ React.createElement("section", { className: "card summary-card" }, /* @__PURE__ */ React.createElement("div", { className: "card-head" }, /* @__PURE__ */ React.createElement("h3", null, "Stand van zaken"), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, adv && adv.at ? relTime(adv.at) : "")), busy ? /* @__PURE__ */ React.createElement("p", { className: "body-text muted thinking" }, /* @__PURE__ */ React.createElement(Spinner, null), " De assistent bekijkt de gegevens, facturen, plannen en het bouwlog…") : adv && adv.stand ? /* @__PURE__ */ React.createElement("p", { className: "body-text" }, adv.stand) : /* @__PURE__ */ React.createElement("p", { className: "body-text muted" }, "Laat de assistent naar deze auto kijken. Hij gebruikt de RDW-gegevens, je facturen, plannen en bouwlog."), adv && adv.basis && !busy && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Op basis van ", plural(adv.basis.facturen || 0, "factuur", "facturen"), ", ", plural(adv.basis.stappen || 0, "bouwstap", "bouwstappen"), " en ", plural(adv.basis.onderdelen || 0, "onderdeel", "onderdelen")), err && /* @__PURE__ */ React.createElement("p", { className: "small tone-danger" }, err), /* @__PURE__ */ React.createElement("div", { className: "card-foot" }, aiOn ? busy ? /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", onClick: () => ctl.current && ctl.current.abort() }, "Stop") : /* @__PURE__ */ React.createElement(Btn, { small: true, kind: adv ? "ghost" : "primary", icon: adv ? "refresh" : "spark", onClick: run }, adv ? "Opnieuw bekijken" : "Advies vragen") : /* @__PURE__ */ React.createElement("span", { className: "small muted" }, "De assistent is in deze weergave niet beschikbaar."))), adv && (adv.kaarten || []).map((k, i) => /* @__PURE__ */ React.createElement(AdviceCard, { key: i, k, car })), /* @__PURE__ */ React.createElement("p", { className: "hint" }, "Adviezen helpen je nadenken; ze vervangen geen monteur of keuring."));
  }

  // src/screens/overall.jsx
  function PlannenAll() {
    const d = useData();
    const ui = useUi();
    const withParts = d.cars.filter((c) => d.partsOf(c.id).length > 0);
    const totalNodig = sum(withParts, (c) => planStats(d.partsOf(c.id), c.budget).nodig);
    const openCount = sum(withParts, (c) => planStats(d.partsOf(c.id), c.budget).open);
    const deals = d.alerts.filter((a) => a.soort === "korting").slice(0, 20);
    const open = deals.filter((a) => !a.gelezen);
    const seen = deals.filter((a) => a.gelezen).slice(0, 5);
    return /* @__PURE__ */ React.createElement("div", { className: "page" }, /* @__PURE__ */ React.createElement("header", { className: "page-head" }, /* @__PURE__ */ React.createElement("h1", { className: "h1" }, "Plannen"), /* @__PURE__ */ React.createElement("p", { className: "sub" }, withParts.length ? totalNodig ? eurR(totalNodig) + " nog nodig voor " + plural(withParts.length, "auto", "auto’s") : plural(openCount, "onderdeel", "onderdelen") + " nog te kopen" : "Wensenlijst en begroting per auto")), /* @__PURE__ */ React.createElement(HelperBanner, null), open.length > 0 && /* @__PURE__ */ React.createElement(Section, { title: "Aanbiedingen" }, /* @__PURE__ */ React.createElement("div", { className: "stack-tight" }, open.map((a) => /* @__PURE__ */ React.createElement(DealCard, { key: a.id, a })))), d.active.length === 0 && withParts.length === 0 ? /* @__PURE__ */ React.createElement(
      Empty,
      {
        art: "list",
        title: "Nog geen plannen",
        text: "Voeg eerst een auto toe. Daarna zet je per auto onderdelen op een lijst, met prijs, link en budget.",
        action: /* @__PURE__ */ React.createElement(Btn, { kind: "primary", icon: "plus", onClick: () => ui.openSheet("car", {}) }, "Auto toevoegen")
      }
    ) : /* @__PURE__ */ React.createElement(Section, { title: "Per auto" }, /* @__PURE__ */ React.createElement("div", { className: "plan-cars" }, d.active.concat(d.previous.filter((c) => d.partsOf(c.id).length)).map((c) => {
      const ps = planStats(d.partsOf(c.id), c.budget);
      return /* @__PURE__ */ React.createElement("button", { key: c.id, type: "button", className: "card plan-car", onClick: () => ui.navigate({ name: "car", carId: c.id, tab: "plannen" }) }, /* @__PURE__ */ React.createElement("div", { className: "plan-car-head" }, /* @__PURE__ */ React.createElement(CarThumb, { car: c, size: 44 }), /* @__PURE__ */ React.createElement("span", { className: "plan-car-name" }, /* @__PURE__ */ React.createElement("strong", null, c.naam || "Auto"), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, c.planNaam || (ps.count ? plural(ps.count, "onderdeel", "onderdelen") : "Nog geen lijst"))), /* @__PURE__ */ React.createElement(Plate, { kenteken: c.kenteken, size: "sm" })), ps.count ? /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement("div", { className: "plan-car-nums" }, /* @__PURE__ */ React.createElement("span", null, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, "Nog nodig"), /* @__PURE__ */ React.createElement("strong", { className: "plan-value" }, !ps.nodig && ps.onbekend ? "–" : eurR(ps.nodig))), /* @__PURE__ */ React.createElement("span", null, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, "Gekocht"), /* @__PURE__ */ React.createElement("strong", { className: "plan-value" }, ps.binnen, "/", ps.count)), ps.budget !== null && /* @__PURE__ */ React.createElement("span", null, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, "Budget"), /* @__PURE__ */ React.createElement("strong", { className: "plan-value" + (ps.ruimte < 0 ? " tone-danger" : "") }, eurR(ps.budget)))), /* @__PURE__ */ React.createElement(Bar, { pct: ps.pct }), ps.deals.length > 0 && /* @__PURE__ */ React.createElement("span", { className: "small tone-accent" }, plural(ps.deals.length, "aanbieding", "aanbiedingen"))) : /* @__PURE__ */ React.createElement("span", { className: "small muted" }, "Tik om onderdelen toe te voegen"));
    }))), seen.length > 0 && /* @__PURE__ */ React.createElement(Section, { title: "Eerder gezien" }, /* @__PURE__ */ React.createElement("div", { className: "stack-tight" }, seen.map((a) => /* @__PURE__ */ React.createElement(DealCard, { key: a.id, a })))), d.aiEnabled && /* @__PURE__ */ React.createElement("p", { className: "hint" }, d.settings.prijzenVolgen ? IS_TEST ? "De assistent checkt de prijzen als je bij Meer een ronde start." : "De assistent checkt elke ochtend de prijzen van onderdelen met een link." : "Prijzen volgen staat uit. Zet het aan bij Meer."));
  }
  function DealCard({ a }) {
    const d = useData();
    const ui = useUi();
    const car = d.car(a.carId);
    const part = d.parts.find((p) => p.id === a.partId);
    return /* @__PURE__ */ React.createElement("div", { className: "deal" + (a.gelezen ? " is-seen" : "") }, /* @__PURE__ */ React.createElement("span", { className: "deal-icon", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement(Icon, { name: "tag", size: 18 })), /* @__PURE__ */ React.createElement("div", { className: "deal-main" }, /* @__PURE__ */ React.createElement("span", { className: "deal-title" }, a.titel || (part ? part.naam : "Aanbieding")), /* @__PURE__ */ React.createElement("span", { className: "deal-sub" }, [car ? car.naam : null, a.winkel || (a.link ? hostOf(a.link) : null)].filter(Boolean).join(" · "), typeof a.nieuw === "number" && /* @__PURE__ */ React.createElement(React.Fragment, null, " · ", /* @__PURE__ */ React.createElement("strong", null, eur(a.nieuw)), typeof a.oud === "number" && /* @__PURE__ */ React.createElement("span", { className: "strike" }, " ", eur(a.oud)), a.pct ? " (−" + Math.round(a.pct) + "%)" : "")), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, relTime(a.createdAt))), /* @__PURE__ */ React.createElement("div", { className: "deal-actions" }, a.link && /* @__PURE__ */ React.createElement("a", { className: "btn btn-ghost btn-small", href: a.link, target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement("span", null, "Bekijk")), !a.gelezen ? /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", onClick: () => d.act.patch("alerts", a.id, { gelezen: true }) }, "Gezien") : part ? /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", onClick: () => ui.openSheet("part", { id: part.id }) }, "Onderdeel") : null));
  }
  function KostenAll() {
    const d = useData();
    const ui = useUi();
    const [busy, setBusy] = React.useState(false);
    const inv = d.invoices.filter((i) => !i.voorstel);
    const none = d.invoicesOf(null);
    const years = Array.from(new Set(inv.map((i) => yearOf(i.datum)).filter(Boolean))).sort().reverse();
    const max = Math.max(1, ...d.cars.map((c) => d.totals[c.id] || 0));
    async function excel() {
      setBusy(true);
      try {
        await downloadExcel(d, ui);
      } finally {
        setBusy(false);
      }
    }
    return /* @__PURE__ */ React.createElement("div", { className: "page" }, /* @__PURE__ */ React.createElement("header", { className: "page-head" }, /* @__PURE__ */ React.createElement("div", { className: "page-head-row" }, /* @__PURE__ */ React.createElement("h1", { className: "h1" }, "Kosten"), inv.length > 0 && d.caps.downloads && /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "secondary", icon: "download", busy, onClick: excel }, "Excel")), /* @__PURE__ */ React.createElement("p", { className: "sub" }, inv.length ? eurR(d.totalAll) + " aan " + plural(inv.length, "factuur", "facturen") : "Alle facturen van al je auto’s")), d.suggestions.length > 0 && /* @__PURE__ */ React.createElement(Section, { title: "Uit je mail, nog controleren" }, /* @__PURE__ */ React.createElement("div", { className: "list-card" }, d.suggestions.map((i) => /* @__PURE__ */ React.createElement(InvoiceRow, { key: i.id, inv: i, showCar: true })))), inv.length === 0 ? /* @__PURE__ */ React.createElement(
      Empty,
      {
        art: "chart",
        title: "Nog geen kosten",
        text: d.cars.length ? "Scan een factuur of voeg kosten zelf toe. Ze komen in het paspoort van de auto." : "Voeg eerst een auto toe, daarna kun je facturen scannen.",
        action: d.cars.length ? /* @__PURE__ */ React.createElement(Btn, { kind: "primary", icon: "camera", onClick: () => ui.openSheet("scan", {}) }, "Factuur scannen") : /* @__PURE__ */ React.createElement(Btn, { kind: "primary", icon: "plus", onClick: () => ui.openSheet("car", {}) }, "Auto toevoegen")
      }
    ) : /* @__PURE__ */ React.createElement("div", { className: "kosten-grid" }, /* @__PURE__ */ React.createElement("div", { className: "stack" }, /* @__PURE__ */ React.createElement(Section, { title: "Per auto" }, /* @__PURE__ */ React.createElement("div", { className: "card cat-bars" }, d.cars.filter((c) => d.invoicesOf(c.id).length).sort((a, b) => (d.totals[b.id] || 0) - (d.totals[a.id] || 0)).map((c) => /* @__PURE__ */ React.createElement("button", { key: c.id, type: "button", className: "cat-row cat-row-btn", onClick: () => ui.navigate({ name: "car", carId: c.id, tab: "kosten" }) }, /* @__PURE__ */ React.createElement("span", { className: "cat-name" }, c.naam || "Auto", c.status === "verkocht" ? /* @__PURE__ */ React.createElement("span", { className: "muted" }, " · eerder gehad") : null), /* @__PURE__ */ React.createElement("span", { className: "cat-amount" }, eurR(d.totals[c.id] || 0)), /* @__PURE__ */ React.createElement(Bar, { pct: (d.totals[c.id] || 0) / max }))), none.length > 0 && /* @__PURE__ */ React.createElement("div", { className: "cat-row" }, /* @__PURE__ */ React.createElement("span", { className: "cat-name muted" }, "Niet aan een auto"), /* @__PURE__ */ React.createElement("span", { className: "cat-amount" }, eurR(sum(none, (i) => i.totaal))), /* @__PURE__ */ React.createElement(Bar, { pct: sum(none, (i) => i.totaal) / max })))), years.length > 0 && /* @__PURE__ */ React.createElement(Section, { title: "Per jaar" }, /* @__PURE__ */ React.createElement("div", { className: "card year-grid year-grid-wide" }, years.slice(0, 6).map((y) => /* @__PURE__ */ React.createElement("div", { key: y, className: "year-cell" }, /* @__PURE__ */ React.createElement("span", { className: "eyebrow" }, y === d.thisYear ? y + " tot nu" : y), /* @__PURE__ */ React.createElement("span", { className: "year-value" }, eurR(sum(inv.filter((i) => yearOf(i.datum) === y), (i) => i.totaal))))))), /* @__PURE__ */ React.createElement(Section, { title: "Per categorie" }, /* @__PURE__ */ React.createElement("div", { className: "card" }, /* @__PURE__ */ React.createElement(CategoryBars, { invoices: inv })))), /* @__PURE__ */ React.createElement(
      Section,
      {
        title: "Laatste facturen",
        right: /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: "plus", onClick: () => ui.openSheet("invoice", {}) }, "Kosten")
      },
      /* @__PURE__ */ React.createElement("div", { className: "list-card" }, inv.slice(0, 40).map((i) => /* @__PURE__ */ React.createElement(InvoiceRow, { key: i.id, inv: i, showCar: true }))),
      inv.length > 40 && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "De nieuwste 40. Alle facturen staan per auto onder Kosten, en in de Excel.")
    )));
  }
  async function downloadExcel(d, ui) {
    if (!d.caps.downloads) return ui.toast("Downloaden kan in deze weergave niet.", "error");
    let bytes;
    try {
      bytes = await buildWorkbook(d);
    } catch (e) {
      return ui.toast("De Excel kon niet worden gemaakt. Controleer je verbinding.", "error");
    }
    try {
      await d.caps.downloads.save({ filename: "Autopaspoort kosten " + todayISO() + ".xlsx", data: new Blob([bytes]) });
    } catch (e) {
      if (e && e.code !== "declined") ui.toast(e && e.code === "rate_limited" ? "Er staat al een download klaar." : "Downloaden lukte niet.", "error");
    }
  }
  var MORNING = IS_TEST ? "" : " De assistent komt elke ochtend vanzelf langs.";
  var FIRE_COPY = {
    ai_off: "AI staat uit, dus de assistent start niet.",
    no_trigger: "De assistent is nog niet gekoppeld aan deze app.",
    no_mcp: "Starten kan in deze weergave niet." + MORNING,
    not_granted: "Starten kan in deze weergave niet." + MORNING,
    capability_disabled: "Starten kan in deze weergave niet." + MORNING,
    not_in_manifest: "Je hebt de app geen toestemming gegeven om de assistent te starten. Herlaad de pagina om het opnieuw te vragen.",
    needs_reauth: "Verbind Claude Code Remote opnieuw in claude.ai → Instellingen → Connectors.",
    server_not_connected: "Voeg Claude Code Remote toe in claude.ai → Instellingen → Connectors.",
    selection_required: "Kies eerst welke Claude Code Remote-koppeling de app mag gebruiken.",
    blocked_by_policy: "Je organisatie staat starten vanuit de app niet toe.",
    approval_required: "Je organisatie vraagt per keer om goedkeuring; dat kan in de app nog niet.",
    tool_error: "De assistent kon niet worden gestart. Misschien is de geplande taak verwijderd of gepauzeerd."
  };
  function Meer() {
    const d = useData();
    const ui = useUi();
    const s = d.settings;
    const h = d.helper || {};
    const helperOk = d.caps.helperTool === "ok" && !!s.helperTriggerId;
    const open = d.openRequests;
    const [xl, setXl] = React.useState(false);
    const busy = h.status === "bezig" && h.startedAt && Date.now() - new Date(h.startedAt).getTime() < 20 * 6e4;
    const fire = d.fire;
    async function backup() {
      if (!d.caps.downloads) return ui.toast("Downloaden kan in deze weergave niet.", "error");
      const keep = (rows) => rows.map((r) => Object.assign({}, r));
      const dump = { gemaakt: (/* @__PURE__ */ new Date()).toISOString(), cars: keep(d.cars), invoices: keep(d.invoices), steps: keep(d.steps), parts: keep(d.parts), mods: keep(d.mods), apkNotes: keep(d.apkNotes), alerts: keep(d.alerts) };
      try {
        await d.caps.downloads.save({ filename: "Autopaspoort back-up " + todayISO() + ".json", data: JSON.stringify(dump, null, 2) });
      } catch (e) {
        if (e && e.code !== "declined") ui.toast("Downloaden lukte niet.", "error");
      }
    }
    return /* @__PURE__ */ React.createElement("div", { className: "page" }, /* @__PURE__ */ React.createElement("header", { className: "page-head" }, /* @__PURE__ */ React.createElement("h1", { className: "h1" }, "Meer"), /* @__PURE__ */ React.createElement("p", { className: "sub" }, IS_WEB ? "Back-up, exporteren en installeren" : d.aiEnabled ? "Koppelingen, de assistent en exporteren" : "Zonder AI en exporteren")), /* @__PURE__ */ React.createElement("div", { className: "meer-grid" }, /* @__PURE__ */ React.createElement("div", { className: "stack" }, IS_TEST && /* @__PURE__ */ React.createElement(Section, { title: "AI" }, /* @__PURE__ */ React.createElement("div", { className: "list-card switch-list" }, /* @__PURE__ */ React.createElement(Switch, { id: "set-ai", checked: d.aiEnabled, onChange: (v) => d.act.setting("ai", v), label: "AI-functies", sub: d.aiEnabled ? "Aan: facturen lezen, advies en teksten, plus de assistent op de achtergrond" : "Uit: de app werkt volledig zonder AI" }))), IS_WEB && EXTRA.MeerTop && /* @__PURE__ */ React.createElement(EXTRA.MeerTop, null), !d.aiEnabled && !IS_WEB && /* @__PURE__ */ React.createElement(Section, { title: "Zonder AI" }, /* @__PURE__ */ React.createElement("div", { className: "card" }, /* @__PURE__ */ React.createElement("ul", { className: "plain-list" }, /* @__PURE__ */ React.createElement("li", null, "Facturen vul je zelf in. Bij een pdf van een webshop vult de app bedrag, datum en leverancier alvast in."), /* @__PURE__ */ React.createElement("li", null, "RDW-gegevens worden niet automatisch opgehaald. Vul de APK-datum zelf in bij Auto bewerken; de app waarschuwt op tijd."), /* @__PURE__ */ React.createElement("li", null, "De samenvatting van het bouwlog, de controles bij Advies en de verkooptekst komen uit vaste regels."), /* @__PURE__ */ React.createElement("li", null, "Geen prijzen volgen, geen Google Drive en geen facturen uit je mail: dat doet de assistent.")))), d.aiEnabled && /* @__PURE__ */ React.createElement(Section, { title: "Assistent op de achtergrond" }, /* @__PURE__ */ React.createElement("div", { className: "card" }, /* @__PURE__ */ React.createElement("p", { className: "body-text" }, IS_TEST ? "Haalt RDW-gegevens op, leest productlinks en zet facturen in je Drive-map. In de testversie komt hij niet elke ochtend langs: start hieronder een ronde om prijzen, RDW en het Excel-overzicht bij te werken." : "Haalt RDW-gegevens op, leest productlinks, checkt elke ochtend de prijzen en zet facturen in je Google Drive."), /* @__PURE__ */ React.createElement("dl", { className: "kv" }, /* @__PURE__ */ React.createElement("div", { className: "kv-row" }, /* @__PURE__ */ React.createElement("dt", null, "Status"), /* @__PURE__ */ React.createElement("dd", null, busy ? "Nu bezig" : h.lastRun ? "Klaar" : "Nog niet gedraaid")), /* @__PURE__ */ React.createElement("div", { className: "kv-row" }, /* @__PURE__ */ React.createElement("dt", null, "Laatste ronde"), /* @__PURE__ */ React.createElement("dd", null, h.lastRun ? relTime(h.lastRun) : "–")), h.samenvatting && /* @__PURE__ */ React.createElement("div", { className: "kv-row" }, /* @__PURE__ */ React.createElement("dt", null, "Resultaat"), /* @__PURE__ */ React.createElement("dd", null, h.samenvatting)), /* @__PURE__ */ React.createElement("div", { className: "kv-row" }, /* @__PURE__ */ React.createElement("dt", null, "Wacht op de assistent"), /* @__PURE__ */ React.createElement("dd", null, open.length ? plural(open.length, "verzoek", "verzoeken") : "niets"))), helperOk ? /* @__PURE__ */ React.createElement(Btn, { kind: "secondary", icon: "refresh", busy: fire.busy, onClick: () => d.act.fireHelper(true).then((ok) => ok && ui.toast("De assistent is gestart. Dat duurt meestal een paar minuten.")) }, "Nu een ronde laten doen") : /* @__PURE__ */ React.createElement("p", { className: "small muted" }, s.helperTriggerId ? "Starten kan in deze weergave niet" + (IS_TEST ? "." : "; de assistent komt elke ochtend vanzelf langs.") : "De assistent is nog niet gekoppeld."), fire.ok === false && fire.code && /* @__PURE__ */ React.createElement("p", { className: "small tone-danger" }, FIRE_COPY[fire.code] || "Starten lukte niet. Probeer het later opnieuw."))), d.aiEnabled && /* @__PURE__ */ React.createElement(Section, { title: "Koppelingen" }, /* @__PURE__ */ React.createElement("div", { className: "list-card switch-list" }, /* @__PURE__ */ React.createElement(Switch, { id: "set-drive", checked: s.driveSync, onChange: (v) => d.act.setting("driveSync", v), label: "Facturen naar Google Drive", sub: "Map " + (s.driveRootNaam || "automotive invoices") + ", per auto een submap, plus een Excel-overzicht" }), /* @__PURE__ */ React.createElement(Switch, { id: "set-prijzen", checked: s.prijzenVolgen, onChange: (v) => d.act.setting("prijzenVolgen", v), label: "Prijzen volgen", sub: IS_TEST ? "Bij elke ronde een check, melding bij korting" : "Elke ochtend een check, melding bij korting" }), /* @__PURE__ */ React.createElement(Switch, { id: "set-rdw", checked: s.rdwBijwerken, onChange: (v) => d.act.setting("rdwBijwerken", v), label: "RDW bijwerken", sub: IS_TEST ? "Bij elke ronde: APK, verzekering en keuringen" : "Elke week APK, verzekering en keuringen" }), !IS_TEST && /* @__PURE__ */ React.createElement(Switch, { id: "set-gmail", checked: s.gmailImport, onChange: (v) => d.act.setting("gmailImport", v), label: "Facturen uit je mail", sub: "Nieuwe facturen in Gmail worden voorgesteld; jij bevestigt" })), IS_TEST && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "In de testversie blijft je mail erbuiten. Facturen gaan naar een eigen Drive-map, los van je echte administratie."), /* @__PURE__ */ React.createElement("div", { className: "link-list" }, s.driveRootId && /* @__PURE__ */ React.createElement("a", { className: "link-row", href: "https://drive.google.com/drive/folders/" + s.driveRootId, target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement(Icon, { name: "cloud", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Map in Google Drive"), /* @__PURE__ */ React.createElement(Icon, { name: "out", size: 16 })), h.excelFileId && /* @__PURE__ */ React.createElement("a", { className: "link-row", href: "https://drive.google.com/file/d/" + h.excelFileId + "/view", target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement(Icon, { name: "chart", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Kostenoverzicht in Drive", h.excelAt ? " · " + relTime(h.excelAt) : ""), /* @__PURE__ */ React.createElement(Icon, { name: "out", size: 16 }))))), /* @__PURE__ */ React.createElement("div", { className: "stack" }, d.aiEnabled && /* @__PURE__ */ React.createElement(Section, { title: "Assistent in de app" }, /* @__PURE__ */ React.createElement("div", { className: "list-card switch-list" }, /* @__PURE__ */ React.createElement(Switch, { id: "set-twijfel", checked: s.bijTwijfelVragen, onChange: (v) => d.act.setting("bijTwijfelVragen", v), label: "Bij twijfel altijd vragen", sub: "Is het een gok, dan kies jij de auto" })), /* @__PURE__ */ React.createElement("p", { className: "small muted" }, ui.ai.sample && !ui.ai.off ? "Facturen lezen, bouwstappen inkorten, advies en verkoopteksten gebruiken je eigen Claude-account." : "De assistent is in deze weergave niet beschikbaar; je kunt alles ook zelf invullen.")), /* @__PURE__ */ React.createElement(Section, { title: "Exporteren" }, /* @__PURE__ */ React.createElement("div", { className: "list-card" }, /* @__PURE__ */ React.createElement(
      "button",
      {
        type: "button",
        className: "list-row",
        disabled: xl,
        onClick: async () => {
          setXl(true);
          await downloadExcel(d, ui);
          setXl(false);
        }
      },
      xl ? /* @__PURE__ */ React.createElement(Spinner, null) : /* @__PURE__ */ React.createElement(Icon, { name: "chart", size: 20 }),
      /* @__PURE__ */ React.createElement("span", { className: "list-row-main" }, /* @__PURE__ */ React.createElement("span", { className: "list-row-title" }, "Excel downloaden"), /* @__PURE__ */ React.createElement("span", { className: "list-row-sub" }, "Overzicht, een blad per auto en de plannen")),
      /* @__PURE__ */ React.createElement(Icon, { name: "download", size: 18, className: "chev" })
    ), !IS_WEB && /* @__PURE__ */ React.createElement("button", { type: "button", className: "list-row", onClick: backup }, /* @__PURE__ */ React.createElement(Icon, { name: "doc", size: 20 }), /* @__PURE__ */ React.createElement("span", { className: "list-row-main" }, /* @__PURE__ */ React.createElement("span", { className: "list-row-title" }, "Back-up downloaden"), /* @__PURE__ */ React.createElement("span", { className: "list-row-sub" }, "Alle gegevens als json-bestand")), /* @__PURE__ */ React.createElement(Icon, { name: "download", size: 18, className: "chev" })))), IS_WEB && /* @__PURE__ */ React.createElement(Section, { title: "Zo werkt deze app" }, /* @__PURE__ */ React.createElement("div", { className: "card" }, /* @__PURE__ */ React.createElement("ul", { className: "plain-list" }, /* @__PURE__ */ React.createElement("li", null, "Facturen vul je zelf in. Bij een pdf van een webshop vult de app bedrag, datum en leverancier alvast in."), /* @__PURE__ */ React.createElement("li", null, "Merk, model, APK, verzekering en keuringen haalt de app zelf bij de RDW op, met alleen het kenteken."), /* @__PURE__ */ React.createElement("li", null, "De samenvatting van het bouwlog, de controles bij Advies en de verkooptekst komen uit vaste regels."), /* @__PURE__ */ React.createElement("li", null, "Geen account en geen server. Werkt ook zonder internet; alleen de RDW heeft verbinding nodig.")))), d.aiEnabled && d.requests.length > 0 && /* @__PURE__ */ React.createElement(Section, { title: "Recente verzoeken" }, /* @__PURE__ */ React.createElement("div", { className: "list-card" }, d.requests.slice(0, 8).map((r) => /* @__PURE__ */ React.createElement("div", { key: r.id, className: "list-row is-static" }, /* @__PURE__ */ React.createElement("span", { className: "list-row-main" }, /* @__PURE__ */ React.createElement("span", { className: "list-row-title" }, REQ[r.type] || r.type), /* @__PURE__ */ React.createElement("span", { className: "list-row-sub" }, [relTime(r.createdAt), r.carId && d.car(r.carId) ? d.car(r.carId).naam : null, r.error || r.result].filter(Boolean).join(" · "))), /* @__PURE__ */ React.createElement("span", { className: "pill " + (r.status === "klaar" ? "pill-ok" : r.status === "fout" ? "pill-warn" : "pill-info") }, STATUS[r.status] || r.status))))))), /* @__PURE__ */ React.createElement("p", { className: "hint" }, IS_WEB ? "Autopaspoort · je gegevens staan alleen op dit apparaat." : IS_NOAI ? "Dit is de versie zonder AI, met een eigen database. Wat je hier doet, komt niet in Mijn Autopaspoort." : IS_TEST ? "Dit is de testversie, met een eigen database. Wat je hier doet, komt niet in Mijn Autopaspoort." : "Je gegevens staan in deze app op claude.ai. Alleen mensen met wie je de app deelt, kunnen ze zien."));
  }
  var REQ = { rdw: "RDW-gegevens ophalen", link: "Productpagina lezen", drive: "Naar Google Drive", ronde: "Ronde" };
  var STATUS = { open: "wacht", bezig: "bezig", klaar: "klaar", fout: "mislukt" };

  // src/screens/bought.jsx
  var round22 = (n) => Math.round(n * 100) / 100;
  function InvoiceChoice({ inv, on, onToggle, why }) {
    return /* @__PURE__ */ React.createElement("button", { type: "button", role: "checkbox", "aria-checked": on, className: "choice" + (on ? " is-on" : ""), onClick: onToggle }, /* @__PURE__ */ React.createElement("span", { className: "choice-icon", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement(Icon, { name: inv.voorstel ? "mail" : inv.file || inv.driveFileId ? "doc" : "euro", size: 18 })), /* @__PURE__ */ React.createElement("span", { className: "choice-main" }, /* @__PURE__ */ React.createElement("span", { className: "choice-title" }, inv.leverancier || inv.omschrijving || "Factuur"), /* @__PURE__ */ React.createElement("span", { className: "choice-sub" }, [fmtDate(inv.datum), inv.leverancier && inv.omschrijving ? clip(inv.omschrijving, 46) : null].filter(Boolean).join(" · ")), why && why.length > 0 && /* @__PURE__ */ React.createElement("span", { className: "reasons" }, why.map((w) => /* @__PURE__ */ React.createElement("span", { key: w, className: "reason" }, w)))), /* @__PURE__ */ React.createElement("span", { className: "choice-amount" }, eur(inv.totaal)), /* @__PURE__ */ React.createElement("span", { className: "checkbox", "aria-hidden": "true" }, on && /* @__PURE__ */ React.createElement(Icon, { name: "check", size: 13, stroke: 3 })));
  }
  function BoughtSheet({ partId, onClose }) {
    const d = useData();
    const ui = useUi();
    const part = d.parts.find((p) => p.id === partId) || null;
    const sugg = React.useMemo(() => part ? suggestInvoices(d, part) : [], [part, d.invoices, d.parts]);
    const [sel, setSel] = React.useState(() => new Set(part && (part.facturen || []).length ? part.facturen : sugg.filter((s) => s.score >= 7).map((s) => s.inv.id)));
    const [showAll, setShowAll] = React.useState(false);
    const [q, setQ] = React.useState("");
    const [onderweg, setOnderweg] = React.useState(!!(part && part.status === "besteld"));
    const [prijs, setPrijs] = React.useState(() => toInput(part && part.prijs));
    const [datum, setDatum] = React.useState(part && part.gekochtOp || "");
    const [busy, setBusy] = React.useState(false);
    if (!part) return null;
    const car = d.car(part.carId);
    const suggIds = new Set(sugg.map((s) => s.inv.id));
    const pool = d.invoices.filter((i) => !i.carId || i.carId === part.carId || i.voorstel);
    const needle = q.trim().toLowerCase();
    const list = pool.filter((i) => !suggIds.has(i.id) && (!needle || [i.leverancier, i.omschrijving, i.factuurnummer].join(" ").toLowerCase().includes(needle)));
    const chosen = d.invoices.filter((i) => sel.has(i.id));
    const autoDatum = chosen.map((i) => i.datum).filter(Boolean).sort()[0] || "";
    const perStuk = (part.aantal || 1) > 1;
    const toggle = (id) => setSel((s) => {
      const n = new Set(s);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });
    async function save() {
      setBusy(true);
      const ids = [...sel];
      const fields = { status: onderweg ? "besteld" : "binnen", facturen: ids, gekochtOp: datum || autoDatum || todayISO(), volgen: false, deal: null };
      const p = parseNum(prijs);
      if (p !== null) {
        fields.prijs = p;
        fields.prijsVanLink = false;
      } else if (typeof part.prijs !== "number" && chosen.length) {
        fields.prijs = round22(sum(chosen, (i) => i.totaal) / (part.aantal || 1));
        fields.prijsVanLink = false;
      }
      const ok = await d.act.patch("parts", part.id, fields);
      for (const inv of chosen) if (inv.voorstel || !inv.carId) await d.act.patch("invoices", inv.id, { voorstel: false, carId: inv.carId || part.carId }, true);
      setBusy(false);
      if (ok) {
        onClose();
        ui.toast(ids.length ? "Gekocht · " + plural(ids.length, "factuur", "facturen") + " gekoppeld" : "Op gekocht gezet");
      }
    }
    function addNew() {
      onClose();
      const viaScan = !!(ui.ai.sample && !ui.ai.off);
      setTimeout(() => ui.openSheet(viaScan ? "scan" : "invoice", { carId: part.carId, linkPartId: part.id, linkOnderweg: onderweg }), 0);
    }
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: "Gekocht",
        onClose,
        right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: save, disabled: busy }, "Bewaar")
      },
      /* @__PURE__ */ React.createElement("p", { className: "small muted sheet-context" }, part.naam || "Onderdeel", car ? " · " + (car.naam || "je auto") : ""),
      /* @__PURE__ */ React.createElement("h3", { className: "sheet-h" }, "Welke factuur hoort erbij?"),
      sugg.length > 0 ? /* @__PURE__ */ React.createElement("div", { className: "choice-list" }, /* @__PURE__ */ React.createElement("p", { className: "eyebrow" }, "Gevonden"), sugg.map(({ inv, why }) => /* @__PURE__ */ React.createElement(InvoiceChoice, { key: inv.id, inv, why, on: sel.has(inv.id), onToggle: () => toggle(inv.id) }))) : /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Geen factuur gevonden die er duidelijk bij hoort. Kies er zelf een of voeg hem toe."),
      [...sel].filter((id) => !suggIds.has(id)).length > 0 && !showAll && /* @__PURE__ */ React.createElement("div", { className: "choice-list" }, /* @__PURE__ */ React.createElement("p", { className: "eyebrow" }, "Gekozen"), chosen.filter((i) => !suggIds.has(i.id)).map((inv) => /* @__PURE__ */ React.createElement(InvoiceChoice, { key: inv.id, inv, on: true, onToggle: () => toggle(inv.id) }))),
      /* @__PURE__ */ React.createElement("div", { className: "row-gap" }, /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "secondary", icon: "plus", onClick: addNew }, "Factuur toevoegen"), /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "ghost", icon: showAll ? "minus" : "search", onClick: () => setShowAll(!showAll) }, showAll ? "Minder tonen" : "Andere factuur kiezen")),
      showAll && /* @__PURE__ */ React.createElement("div", { className: "choice-list" }, /* @__PURE__ */ React.createElement("input", { className: "input", type: "search", value: q, onChange: (e) => setQ(e.target.value), placeholder: "Zoek op winkel of omschrijving", "aria-label": "Zoek een factuur" }), list.slice(0, 40).map((inv) => /* @__PURE__ */ React.createElement(InvoiceChoice, { key: inv.id, inv, on: sel.has(inv.id), onToggle: () => toggle(inv.id) })), list.length === 0 && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Geen facturen gevonden.")),
      /* @__PURE__ */ React.createElement(Switch, { id: "bought-onderweg", checked: onderweg, onChange: setOnderweg, label: "Nog onderweg", sub: "Besteld, maar nog niet binnen" }),
      /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: perStuk ? "Prijs per stuk" : "Betaald", id: "bought-prijs" }, /* @__PURE__ */ React.createElement("input", { id: "bought-prijs", className: "input", inputMode: "decimal", value: prijs, onChange: (e) => setPrijs(e.target.value), placeholder: chosen.length ? toInput(round22(sum(chosen, (i) => i.totaal) / (part.aantal || 1))) : "€" })), /* @__PURE__ */ React.createElement(Field, { label: "Gekocht op", id: "bought-datum" }, /* @__PURE__ */ React.createElement("input", { id: "bought-datum", className: "input", type: "date", value: datum || autoDatum || todayISO(), onChange: (e) => setDatum(e.target.value) }))),
      /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, busy, onClick: save }, sel.size ? "Bewaar als gekocht" : "Gekocht zonder factuur")
    );
  }
  function PartStatus({ part, onClose }) {
    const d = useData();
    const ui = useUi();
    const st = part.status || "nog";
    const invs = (part.facturen || []).map((id) => d.invoices.find((i) => i.id === id)).filter(Boolean);
    const openBought = () => {
      onClose();
      setTimeout(() => ui.openSheet("bought", { partId: part.id }), 0);
    };
    if (st === "nog") {
      return /* @__PURE__ */ React.createElement("div", { className: "status-card" }, /* @__PURE__ */ React.createElement("div", { className: "status-card-head" }, /* @__PURE__ */ React.createElement("span", { className: "status-label" }, /* @__PURE__ */ React.createElement("span", { className: "small muted" }, "Status"), /* @__PURE__ */ React.createElement("strong", null, "Nog kopen")), /* @__PURE__ */ React.createElement(Btn, { kind: "primary", icon: "check", onClick: openBought }, "Gekocht")));
    }
    return /* @__PURE__ */ React.createElement("div", { className: "status-card is-done" }, /* @__PURE__ */ React.createElement("div", { className: "status-card-head" }, /* @__PURE__ */ React.createElement("span", { className: "status-label" }, /* @__PURE__ */ React.createElement("span", { className: "small muted" }, "Status"), /* @__PURE__ */ React.createElement("strong", null, st === "besteld" ? "Gekocht, nog onderweg" : "Gekocht", part.gekochtOp ? /* @__PURE__ */ React.createElement("span", { className: "muted" }, " · ", fmtDate(part.gekochtOp)) : null)), st === "besteld" && /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "secondary", icon: "check", onClick: () => d.act.patch("parts", part.id, { status: "binnen" }) }, "Binnen")), invs.length > 0 ? /* @__PURE__ */ React.createElement("div", { className: "link-list" }, invs.map((i) => /* @__PURE__ */ React.createElement("button", { key: i.id, type: "button", className: "link-row", onClick: () => ui.openSheet("invoice", { id: i.id }) }, /* @__PURE__ */ React.createElement(Icon, { name: "doc", size: 18 }), /* @__PURE__ */ React.createElement("span", null, [i.leverancier || i.omschrijving || "Factuur", fmtDate(i.datum), eur(i.totaal)].filter(Boolean).join(" · ")), /* @__PURE__ */ React.createElement(Icon, { name: "next", size: 16 })))) : /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Nog geen factuur gekoppeld."), /* @__PURE__ */ React.createElement("div", { className: "row-gap" }, /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "secondary", icon: invs.length ? "edit" : "plus", onClick: openBought }, invs.length ? "Factuur wijzigen" : "Factuur koppelen"), /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => d.act.patch("parts", part.id, { status: "nog", facturen: [], gekochtOp: null }) }, "Terug naar nog kopen")));
  }

  // src/screens/sheets.jsx
  var val = (e) => e && e.target ? e.target.type === "checkbox" ? e.target.checked : e.target.value : e;
  function useForm(init) {
    const [f, setF] = React.useState(init);
    const set = (k) => (e) => {
      const v = val(e);
      setF((p) => ({ ...p, [k]: v }));
    };
    return [f, set, setF];
  }
  async function rdwAfterSave(d, ui, car, isNew) {
    const r = await syncRdw(d.act, car, { quiet: true });
    if (r.ok) return ui.toast(isNew ? "Auto toegevoegd: " + r.naam : "RDW-gegevens bijgewerkt");
    const why = RDW_COPY[r.code] || "De RDW-gegevens konden niet worden opgehaald.";
    ui.toast(isNew ? "Auto toegevoegd. " + why : why, "error");
  }
  function CarSheet({ id, focus, onClose }) {
    const d = useData();
    const ui = useUi();
    const existing = id ? d.car(id) : null;
    const rdwAuto = d.aiEnabled || RDW_DIRECT;
    const [f, set] = useForm(
      () => existing ? {
        kenteken: existing.kenteken ? fmtPlate(existing.kenteken) : "",
        naam: existing.naam || "",
        bouwjaar: existing.bouwjaar ? String(existing.bouwjaar) : "",
        ondertitel: existing.ondertitel || "",
        gebruik: existing.gebruik || "dagelijks",
        km: existing.km ? String(existing.km) : "",
        aankoopdatum: existing.aankoopdatum || "",
        aankoopprijs: toInput(existing.aankoopprijs),
        weg: existing.status === "verkocht",
        verkoopdatum: existing.verkoopdatum || "",
        verkoopprijs: toInput(existing.verkoopprijs),
        apkTot: existing.apkTot || ""
      } : { kenteken: "", naam: "", bouwjaar: "", ondertitel: "", gebruik: "dagelijks", km: "", aankoopdatum: "", aankoopprijs: "", weg: false, verkoopdatum: "", verkoopprijs: "", apkTot: "" }
    );
    const [photo, setPhoto] = React.useState({ id: existing ? existing.foto || null : null, preview: null, busy: false });
    const [err, setErr] = React.useState("");
    const [saving, setSaving] = React.useState(false);
    const uploaded = React.useRef([]);
    React.useEffect(() => {
      if (focus) {
        const el = document.getElementById("car-" + focus);
        if (el) setTimeout(() => el.focus(), 80);
      }
    }, []);
    async function pick(file) {
      setPhoto((p) => ({ ...p, busy: true }));
      try {
        const blob = await downscale(file, 1800, 0.84);
        const res = await d.act.upload(blob, "image/jpeg");
        uploaded.current.push(res.id);
        setPhoto({ id: res.id, preview: URL.createObjectURL(blob), busy: false });
      } catch (e) {
        setPhoto((p) => ({ ...p, busy: false }));
        ui.toast("De foto kon niet worden opgeslagen.", "error");
      }
    }
    function cancel() {
      for (const a of uploaded.current) if (a !== (existing && existing.foto)) d.act.deleteAsset(a);
      onClose();
    }
    async function save() {
      const kent = normPlate(f.kenteken);
      if (!kent && !f.naam.trim()) {
        setErr("Vul een kenteken of een naam in.");
        return;
      }
      if (kent && kent.length < 4) {
        setErr("Dit kenteken lijkt niet compleet.");
        return;
      }
      if (!rdwAuto && !f.naam.trim()) {
        setErr("Vul merk en model in. Zonder AI haalt de app die niet bij de RDW op.");
        return;
      }
      setSaving(true);
      const km = parseNum(f.km);
      const data = {
        naam: f.naam.trim(),
        kenteken: kent || null,
        bouwjaar: parseInt(f.bouwjaar, 10) || null,
        ondertitel: f.ondertitel.trim() || null,
        gebruik: f.gebruik,
        km: km || null,
        kmDatum: km && (!existing || km !== existing.km) ? todayISO() : existing ? existing.kmDatum || null : null,
        aankoopdatum: f.aankoopdatum || null,
        aankoopprijs: parseNum(f.aankoopprijs),
        status: f.weg ? "verkocht" : "actief",
        verkoopdatum: f.weg ? f.verkoopdatum || null : null,
        verkoopprijs: f.weg ? parseNum(f.verkoopprijs) : null,
        foto: photo.id || null,
        apkTot: f.apkTot || null
      };
      for (const a of uploaded.current) if (a !== photo.id) d.act.deleteAsset(a);
      if (!existing) {
        const carId = newId("c");
        const ok = await d.act.put("cars", carId, Object.assign(data, { specs: {}, order: Date.now(), createdAt: nowISO(), rdwStatus: kent && rdwAuto ? "wachten" : null }));
        setSaving(false);
        if (!ok) return;
        if (kent && RDW_DIRECT) rdwAfterSave(d, ui, Object.assign({ id: carId, specs: {} }, data), true);
        else if (kent) await d.act.queue({ type: "rdw", carId, kenteken: kent });
        onClose();
        ui.navigate({ name: "car", carId });
        if (!(kent && RDW_DIRECT)) ui.toast(kent && d.aiEnabled && d.settings.helperTriggerId ? "Auto toegevoegd. De assistent haalt de RDW-gegevens op." : "Auto toegevoegd");
      } else {
        const changed = kent !== (existing.kenteken || null);
        if (changed && kent && rdwAuto) data.rdwStatus = "wachten";
        if (changed && RDW_DIRECT) Object.assign(data, { rdw: null, apk: [] });
        const ok = await d.act.patch("cars", existing.id, data);
        setSaving(false);
        if (!ok) return;
        if (existing.foto && existing.foto !== photo.id) d.act.deleteAsset(existing.foto);
        if (changed && kent && RDW_DIRECT) rdwAfterSave(d, ui, Object.assign({}, existing, data, { rdw: null }), false);
        else if (changed && kent) await d.act.queue({ type: "rdw", carId: existing.id, kenteken: kent });
        onClose();
        ui.toast("Opgeslagen");
      }
    }
    function askDelete() {
      const c = existing;
      const inv = d.invoices.filter((i) => i.carId === c.id);
      const n = [inv.length ? inv.length + " facturen" : null, d.stepsOf(c.id).length ? d.stepsOf(c.id).length + " bouwstappen" : null, d.partsOf(c.id).length ? d.partsOf(c.id).length + " onderdelen" : null].filter(Boolean).join(", ");
      ui.openSheet("confirm", {
        title: "Auto verwijderen",
        text: (c.naam || "Deze auto") + (n ? " en alles wat erbij hoort (" + n + ")" : "") + " wordt uit de app verwijderd. Dit kan niet ongedaan worden gemaakt. Bestanden in je Google Drive blijven staan. Heb je hem verkocht? Zet hem dan liever bij Eerder gehad.",
        confirmLabel: "Verwijderen",
        danger: true,
        onConfirm: async () => {
          await deleteCar(d, c);
          onClose();
          ui.navigate({ name: "garage" });
          ui.toast("Auto verwijderd");
        }
      });
    }
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: existing ? "Auto bewerken" : "Auto toevoegen",
        onClose: cancel,
        right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: save, disabled: saving || photo.busy }, saving ? "Bezig…" : "Bewaar")
      },
      /* @__PURE__ */ React.createElement(Field, { label: "Kenteken", id: "car-kenteken", hint: existing || !rdwAuto ? null : "Merk, model, APK en keuringen komen automatisch van de RDW." }, /* @__PURE__ */ React.createElement("div", { className: "plate-input" }, /* @__PURE__ */ React.createElement("span", { className: "plate-nl", "aria-hidden": "true" }, "NL"), /* @__PURE__ */ React.createElement(
        "input",
        {
          id: "car-kenteken",
          "data-autofocus": true,
          value: f.kenteken,
          onChange: set("kenteken"),
          onBlur: () => f.kenteken && set("kenteken")(fmtPlate(f.kenteken)),
          placeholder: "AB-123-C",
          autoCapitalize: "characters",
          autoComplete: "off",
          autoCorrect: "off",
          spellCheck: "false",
          maxLength: 10
        }
      ))),
      /* @__PURE__ */ React.createElement(Field, { label: "Merk en model", id: "car-naam", hint: existing || !rdwAuto ? null : "Mag leeg blijven: dan vult de RDW het in." }, /* @__PURE__ */ React.createElement("input", { id: "car-naam", className: "input", value: f.naam, onChange: set("naam"), placeholder: "Bijv. BMW 330i", autoComplete: "off" })),
      /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: "Bouwjaar", id: "car-bouwjaar" }, /* @__PURE__ */ React.createElement("input", { id: "car-bouwjaar", className: "input", inputMode: "numeric", value: f.bouwjaar, onChange: set("bouwjaar"), placeholder: "2005", maxLength: 4 })), /* @__PURE__ */ React.createElement(Field, { label: "Km-stand", id: "car-km" }, /* @__PURE__ */ React.createElement("input", { id: "car-km", className: "input", inputMode: "numeric", value: f.km, onChange: set("km"), placeholder: "185.000" }))),
      (!rdwAuto || existing && existing.apkTot && !existing.rdw || RDW_DIRECT && (!normPlate(f.kenteken) || existing && !existing.rdw && (existing.rdwStatus === "niet-gevonden" || existing.rdwStatus === "fout"))) && /* @__PURE__ */ React.createElement(Field, { label: "APK geldig tot", id: "car-apk", hint: "Staat op je APK-bewijs of bij de RDW. De app waarschuwt op tijd." }, /* @__PURE__ */ React.createElement("input", { id: "car-apk", className: "input", type: "date", value: f.apkTot, onChange: set("apkTot") })),
      /* @__PURE__ */ React.createElement(Field, { label: "Korte omschrijving", id: "car-ondertitel", hint: "Komt onder de naam, bijv. de motor of het project." }, /* @__PURE__ */ React.createElement("input", { id: "car-ondertitel", className: "input", value: f.ondertitel, onChange: set("ondertitel"), placeholder: "Bijv. handbak, in opbouw", maxLength: 60 })),
      d.caps.assets && /* @__PURE__ */ React.createElement(Field, { label: "Foto" }, /* @__PURE__ */ React.createElement("div", { className: "photo-pick" }, photo.preview || photo.id ? /* @__PURE__ */ React.createElement("img", { className: "photo-preview", src: photo.preview || assetUrl(photo.id), alt: "" }) : /* @__PURE__ */ React.createElement("span", { className: "photo-preview ph", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement(Icon, { name: "image", size: 28, stroke: 1.6 })), /* @__PURE__ */ React.createElement("div", { className: "photo-btns" }, /* @__PURE__ */ React.createElement(FilePick, { accept: "image/*", capture: "environment", onFile: pick, className: "btn btn-secondary", id: "car-foto-cam" }, photo.busy ? /* @__PURE__ */ React.createElement(Spinner, null) : /* @__PURE__ */ React.createElement(Icon, { name: "camera", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Foto maken")), /* @__PURE__ */ React.createElement(FilePick, { accept: "image/*", onFile: pick, className: "btn btn-secondary", id: "car-foto-lib" }, /* @__PURE__ */ React.createElement(Icon, { name: "image", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Uit je foto’s"))))),
      /* @__PURE__ */ React.createElement(Field, { label: "Hoe gebruik je deze auto?" }, /* @__PURE__ */ React.createElement(Seg, { label: "Gebruik", options: GEBRUIK, value: f.gebruik, onChange: set("gebruik") })),
      /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: "Gekocht op", id: "car-aankoop" }, /* @__PURE__ */ React.createElement("input", { id: "car-aankoop", className: "input", type: "date", value: f.aankoopdatum, onChange: set("aankoopdatum") })), /* @__PURE__ */ React.createElement(Field, { label: "Aankoopprijs", id: "car-aankoopprijs" }, /* @__PURE__ */ React.createElement("input", { id: "car-aankoopprijs", className: "input", inputMode: "decimal", value: f.aankoopprijs, onChange: set("aankoopprijs"), placeholder: "€" }))),
      /* @__PURE__ */ React.createElement(Switch, { id: "car-weg", checked: f.weg, onChange: set("weg"), label: "Deze auto heb ik niet meer", sub: "Hij komt bij Eerder gehad, met zijn hele paspoort." }),
      f.weg && /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: "Weg sinds", id: "car-verkoop" }, /* @__PURE__ */ React.createElement("input", { id: "car-verkoop", className: "input", type: "date", value: f.verkoopdatum, onChange: set("verkoopdatum") })), /* @__PURE__ */ React.createElement(Field, { label: "Verkoopprijs", id: "car-verkoopprijs" }, /* @__PURE__ */ React.createElement("input", { id: "car-verkoopprijs", className: "input", inputMode: "decimal", value: f.verkoopprijs, onChange: set("verkoopprijs"), placeholder: "€" }))),
      err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err),
      /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, onClick: save, busy: saving, disabled: photo.busy }, existing ? "Opslaan" : "Toevoegen aan garage"),
      existing && /* @__PURE__ */ React.createElement(Btn, { kind: "danger-ghost", block: true, icon: "trash", onClick: askDelete }, "Auto verwijderen")
    );
  }
  async function deleteCar(d, c) {
    const inv = d.invoices.filter((i) => i.carId === c.id);
    const assets2 = [c.foto];
    for (const i of inv) if (i.file) assets2.push(i.file.id, i.file.thumb);
    const jobs = [];
    for (const i of inv) jobs.push(d.act.remove("invoices", i.id, true));
    for (const s of d.stepsOf(c.id)) jobs.push(d.act.remove("steps", s.id, true));
    for (const p of d.partsOf(c.id)) jobs.push(d.act.remove("parts", p.id, true));
    for (const m of d.modsOf(c.id)) jobs.push(d.act.remove("mods", m.id, true));
    for (const n of d.apkNotesOf(c.id)) jobs.push(d.act.remove("apkNotes", n.id, true));
    for (const a of d.alerts.filter((x) => x.carId === c.id)) jobs.push(d.act.remove("alerts", a.id, true));
    jobs.push(d.act.remove("advice", c.id, true));
    await Promise.all(jobs);
    await d.act.remove("cars", c.id);
    for (const a of assets2.filter(Boolean)) d.act.deleteAsset(a);
  }
  function PartSheet({ id, carId, onClose }) {
    const d = useData();
    const ui = useUi();
    const existing = id ? d.parts.find((p) => p.id === id) : null;
    const cid = existing ? existing.carId : carId;
    const car = d.car(cid);
    const [f, set] = useForm(() => ({
      link: existing ? existing.link || "" : "",
      naam: existing && !existing.naamVanLink ? existing.naam || "" : "",
      categorie: existing ? existing.categorie || "overig" : "motor",
      prioriteit: existing ? existing.prioriteit || "moet" : "moet",
      prijs: existing && !existing.prijsVanLink ? toInput(existing.prijs) : "",
      aantal: existing ? existing.aantal || 1 : 1,
      notitie: existing ? existing.notitie || "" : "",
      volgen: existing ? existing.volgen !== false : true,
      drempel: existing ? existing.drempel || "elke" : "elke"
    }));
    const [err, setErr] = React.useState("");
    const [saving, setSaving] = React.useState(false);
    const link = cleanUrl(f.link);
    const hasLink = isUrl(link);
    async function save() {
      if (!f.naam.trim() && (!hasLink || !d.aiEnabled)) {
        setErr(d.aiEnabled ? "Plak een link of typ de naam van het onderdeel." : "Typ de naam van het onderdeel.");
        return;
      }
      if (f.link.trim() && !hasLink) {
        setErr("Deze link klopt niet. Begin met https://");
        return;
      }
      setSaving(true);
      const linkChanged = (hasLink ? link : "") !== (existing && existing.link || "");
      const prijs = parseNum(f.prijs);
      const data = {
        carId: cid,
        naam: f.naam.trim() || (existing && existing.naamVanLink && !linkChanged ? existing.naam : hostOf(link)),
        naamVanLink: !f.naam.trim() && d.aiEnabled,
        categorie: f.categorie,
        prioriteit: f.prioriteit,
        prijs: prijs !== null ? prijs : existing && existing.prijsVanLink && !linkChanged ? existing.prijs ?? null : null,
        prijsVanLink: prijs === null && hasLink && d.aiEnabled,
        aantal: Math.max(1, parseInt(f.aantal, 10) || 1),
        notitie: f.notitie.trim() || null,
        link: hasLink ? link : null,
        volgen: hasLink && !!f.volgen && d.aiEnabled,
        drempel: f.drempel
      };
      if (linkChanged) {
        data.linkStatus = hasLink && d.aiEnabled ? "wachten" : null;
        data.deal = null;
        if (!hasLink) Object.assign(data, { winkel: null, voorraad: null, laatstePrijs: null, laagstePrijs: null, prijsHistorie: null });
      }
      let pid = existing && existing.id;
      let ok;
      if (existing) ok = await d.act.patch("parts", existing.id, data);
      else {
        pid = await d.act.add("parts", "p", Object.assign(data, { status: "nog" }));
        ok = !!pid;
      }
      setSaving(false);
      if (!ok) return;
      if (linkChanged && hasLink) await d.act.queue({ type: "link", partId: pid, carId: cid, url: link });
      onClose();
      ui.toast(linkChanged && hasLink && d.aiEnabled ? "Bewaard. De assistent leest de productpagina." : "Bewaard");
    }
    function askDelete() {
      ui.openSheet("confirm", {
        title: "Onderdeel verwijderen",
        text: (existing.naam || "Dit onderdeel") + " gaat van de lijst af.",
        confirmLabel: "Verwijderen",
        danger: true,
        onConfirm: async () => {
          for (const a of d.alerts.filter((x) => x.partId === existing.id)) d.act.remove("alerts", a.id, true);
          await d.act.remove("parts", existing.id);
          onClose();
        }
      });
    }
    const read = existing && existing.link && existing.linkStatus;
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: "Onderdeel",
        onClose,
        right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: save, disabled: saving }, "Bewaar")
      },
      car && /* @__PURE__ */ React.createElement("p", { className: "small muted sheet-context" }, "Voor ", car.naam || "je auto"),
      existing && /* @__PURE__ */ React.createElement(PartStatus, { part: existing, onClose }),
      /* @__PURE__ */ React.createElement(Field, { label: "Link naar het product", id: "part-link", hint: existing || !d.aiEnabled ? null : "De assistent leest naam, prijs en winkel van de pagina." }, /* @__PURE__ */ React.createElement("input", { id: "part-link", "data-autofocus": true, className: "input", type: "url", inputMode: "url", value: f.link, onChange: set("link"), placeholder: "https://…", autoComplete: "off", autoCapitalize: "none", spellCheck: "false" })),
      existing && existing.link && !read && /* @__PURE__ */ React.createElement("a", { className: "small link", href: existing.link, target: "_blank", rel: "noopener noreferrer" }, "Pagina openen"),
      read && /* @__PURE__ */ React.createElement("div", { className: "read-card" + (existing.deal ? " is-deal" : "") }, existing.linkStatus === "wachten" ? d.settings.helperTriggerId ? /* @__PURE__ */ React.createElement("p", { className: "small muted thinking" }, /* @__PURE__ */ React.createElement(Spinner, null), " De assistent leest de pagina. Dat duurt meestal een paar minuten.") : /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "De pagina wordt gelezen zodra de assistent aan deze app is gekoppeld. Vul naam en prijs voorlopig zelf in.") : existing.linkStatus === "fout" ? /* @__PURE__ */ React.createElement("p", { className: "small tone-danger" }, "De pagina kon niet worden gelezen", existing.linkFout ? ": " + existing.linkFout : "", ". Vul naam en prijs zelf in.") : /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement("span", { className: "small muted" }, "Gelezen van de pagina", existing.laatsteCheck ? " · " + relTime(existing.laatsteCheck) : ""), /* @__PURE__ */ React.createElement("span", { className: "read-name" }, existing.naam), /* @__PURE__ */ React.createElement("span", { className: "small muted" }, [existing.winkel, existing.voorraad].filter(Boolean).join(" · ")), /* @__PURE__ */ React.createElement("div", { className: "read-prices" }, /* @__PURE__ */ React.createElement("span", { className: "read-price" }, eur(existing.laatstePrijs)), typeof existing.laagstePrijs === "number" && existing.laagstePrijs < (existing.laatstePrijs || Infinity) + 1e-3 && /* @__PURE__ */ React.createElement("span", { className: "small muted" }, "laagste tot nu ", eur(existing.laagstePrijs))), existing.deal && /* @__PURE__ */ React.createElement("span", { className: "small tone-accent" }, "Aanbieding: −", Math.round(existing.deal.pct || 0), "% sinds ", relTime(existing.deal.at)), /* @__PURE__ */ React.createElement("a", { className: "small link", href: existing.link, target: "_blank", rel: "noopener noreferrer" }, "Pagina openen"))),
      hasLink && d.aiEnabled && /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement(Switch, { id: "part-volgen", checked: f.volgen, onChange: set("volgen"), label: IS_TEST ? "Prijs volgen" : "Prijs elke ochtend checken", sub: IS_TEST ? "Bij elke ronde van de assistent; melding bij korting." : "Je krijgt een melding als hij in de aanbieding is." }), f.volgen && /* @__PURE__ */ React.createElement(Field, { label: "Melding bij" }, /* @__PURE__ */ React.createElement(
        Seg,
        {
          label: "Melding bij",
          options: [
            ["elke", "Elke daling"],
            ["10", "10% korting"],
            ["20", "20% korting"]
          ],
          value: f.drempel,
          onChange: set("drempel")
        }
      ))),
      /* @__PURE__ */ React.createElement(Field, { label: "Naam", id: "part-naam", hint: hasLink && !f.naam && d.aiEnabled ? "Leeg laten: de assistent vult de naam van de pagina in." : null }, /* @__PURE__ */ React.createElement("input", { id: "part-naam", className: "input", value: f.naam, onChange: set("naam"), placeholder: existing && existing.naamVanLink ? existing.naam : "Bijv. koppelingsset" })),
      /* @__PURE__ */ React.createElement(Field, { label: "Categorie", id: "part-cat" }, /* @__PURE__ */ React.createElement("select", { id: "part-cat", className: "input", value: f.categorie, onChange: set("categorie") }, CATS.map(([v, l]) => /* @__PURE__ */ React.createElement("option", { key: v, value: v }, l)))),
      /* @__PURE__ */ React.createElement(Field, { label: "Nodig?" }, /* @__PURE__ */ React.createElement(
        Seg,
        {
          label: "Nodig",
          options: [
            ["moet", "Moet"],
            ["wil", "Wil"]
          ],
          value: f.prioriteit,
          onChange: set("prioriteit")
        }
      )),
      /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: "Prijs per stuk", id: "part-prijs", hint: hasLink && !f.prijs && d.aiEnabled ? "Leeg: prijs van de pagina." : null }, /* @__PURE__ */ React.createElement("input", { id: "part-prijs", className: "input", inputMode: "decimal", value: f.prijs, onChange: set("prijs"), placeholder: existing && existing.prijsVanLink && typeof existing.prijs === "number" ? toInput(existing.prijs) : "€" })), /* @__PURE__ */ React.createElement(Field, { label: "Aantal" }, /* @__PURE__ */ React.createElement("div", { className: "stepper" }, /* @__PURE__ */ React.createElement("button", { type: "button", "aria-label": "Minder", onClick: () => set("aantal")(Math.max(1, (parseInt(f.aantal, 10) || 1) - 1)) }, /* @__PURE__ */ React.createElement(Icon, { name: "minus", size: 18 })), /* @__PURE__ */ React.createElement("span", { "aria-live": "polite" }, f.aantal), /* @__PURE__ */ React.createElement("button", { type: "button", "aria-label": "Meer", onClick: () => set("aantal")((parseInt(f.aantal, 10) || 1) + 1) }, /* @__PURE__ */ React.createElement(Icon, { name: "plus", size: 18 }))))),
      /* @__PURE__ */ React.createElement(Field, { label: "Notitie", id: "part-notitie" }, /* @__PURE__ */ React.createElement("textarea", { id: "part-notitie", className: "input", rows: 2, value: f.notitie, onChange: set("notitie"), placeholder: "Maat, merk, alternatief…" })),
      err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err),
      /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, onClick: save, busy: saving }, "Bewaar"),
      existing && /* @__PURE__ */ React.createElement(Btn, { kind: "danger-ghost", block: true, icon: "trash", onClick: askDelete }, "Van de lijst halen")
    );
  }
  function StepSheet({ id, carId, onClose }) {
    const d = useData();
    const ui = useUi();
    const existing = id ? d.steps.find((s) => s.id === id) || null : null;
    const cid = existing ? existing.carId : carId;
    const car = d.car(cid);
    const aiOn = ui.ai.sample && !ui.ai.off;
    const [f, set, setF] = useForm(() => ({
      notitie: "",
      titel: existing ? existing.titel || "" : "",
      tekst: existing ? existing.tekst || "" : "",
      datum: existing ? existing.datum || todayISO() : todayISO(),
      mijlpaal: existing ? !!existing.mijlpaal : false,
      bezig: existing ? !!existing.bezig : false
    }));
    const [manual, setManual] = React.useState(!!existing || !aiOn);
    const [busy, setBusy] = React.useState(false);
    const [err, setErr] = React.useState("");
    const [merge2, setMerge] = React.useState(null);
    const ctl = React.useRef(null);
    React.useEffect(() => () => ctl.current && ctl.current.abort(), []);
    async function clearOtherBusy(keepId) {
      for (const s of d.stepsOf(cid)) if (s.bezig && s.id !== keepId) await d.act.patch("steps", s.id, { bezig: false }, true);
    }
    function finish(msg) {
      onClose();
      ui.toast(msg);
      if (car) setTimeout(() => refreshSummary(ui.latest, ui, cid), 400);
    }
    async function saveNew(titel, tekst) {
      const sid = await d.act.add("steps", "s", { carId: cid, datum: f.datum || todayISO(), titel, tekst, origineel: f.notitie.trim() || null, mijlpaal: !!f.mijlpaal, bezig: !!f.bezig });
      if (!sid) return false;
      if (f.bezig) await clearOtherBusy(sid);
      return true;
    }
    async function submit() {
      setErr("");
      if (existing) {
        if (!f.titel.trim()) return setErr("Geef de stap een titel.");
        setBusy(true);
        const ok = await d.act.patch("steps", existing.id, { titel: f.titel.trim(), tekst: f.tekst.trim(), datum: f.datum || existing.datum, mijlpaal: !!f.mijlpaal, bezig: !!f.bezig });
        if (ok && f.bezig) await clearOtherBusy(existing.id);
        setBusy(false);
        if (ok) finish("Bouwstap bijgewerkt");
        return;
      }
      if (manual) {
        if (!f.titel.trim()) return setErr("Geef de stap een titel.");
        setBusy(true);
        const ok = await saveNew(f.titel.trim(), f.tekst.trim());
        setBusy(false);
        if (ok) finish("Bouwstap bewaard");
        return;
      }
      if (!f.notitie.trim()) return setErr("Schrijf kort op wat je gedaan hebt.");
      setBusy(true);
      ctl.current = new AbortController();
      try {
        const recent = d.stepsOf(cid).slice(0, 3);
        const r = await condenseStep(ui.ai.sample, { car: car || {}, text: f.notitie, recent, signal: ctl.current.signal });
        if (!r.titel) throw { code: "empty_completion" };
        if (r.samenvoegen) {
          setMerge({ target: recent.find((s) => s.id === r.samenvoegen), r });
          setBusy(false);
          return;
        }
        const ok = await saveNew(r.titel, r.tekst);
        setBusy(false);
        if (ok) finish("Bouwstap bewaard: " + r.titel);
      } catch (e) {
        setBusy(false);
        if (e && e.code === "cancelled") return;
        if (aiHidden(e)) ui.setAiOff();
        const words2 = f.notitie.trim().split(/\s+/);
        setF((p) => ({ ...p, titel: clip(words2.slice(0, 6).join(" "), 60), tekst: clip(f.notitie.trim(), 220) }));
        setManual(true);
        setErr((aiErrorCopy(e) || "Inkorten lukte niet.") + " Controleer de titel en bewaar.");
      }
    }
    async function doMerge(yes) {
      const { target, r } = merge2;
      setBusy(true);
      let ok;
      if (yes && target) {
        ok = await d.act.patch("steps", target.id, {
          titel: r.titel,
          tekst: r.tekst,
          datum: (f.datum || todayISO()) > target.datum ? f.datum || todayISO() : target.datum,
          vanaf: target.vanaf || target.datum,
          origineel: [target.origineel, f.notitie.trim()].filter(Boolean).join("\n\n"),
          mijlpaal: !!(target.mijlpaal || f.mijlpaal),
          bezig: !!f.bezig
        });
        if (ok && f.bezig) await clearOtherBusy(target.id);
      } else ok = await saveNew(r.titel, r.tekst);
      setBusy(false);
      if (ok) finish(yes ? "Samengevoegd met " + (target ? target.titel : "de vorige stap") : "Bouwstap bewaard: " + r.titel);
    }
    function askDelete() {
      ui.openSheet("confirm", {
        title: "Bouwstap verwijderen",
        text: "“" + (existing.titel || "Deze stap") + "” gaat uit het bouwlog.",
        confirmLabel: "Verwijderen",
        danger: true,
        onConfirm: async () => {
          await d.act.remove("steps", existing.id);
          finish("Bouwstap verwijderd");
        }
      });
    }
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: existing ? "Bouwstap" : "Bouwstap toevoegen",
        onClose,
        right: !merge2 && /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: submit, disabled: busy }, "Bewaar")
      },
      car && /* @__PURE__ */ React.createElement("p", { className: "small muted sheet-context" }, "Bouwlog van ", car.naam || "je auto"),
      merge2 ? /* @__PURE__ */ React.createElement("div", { className: "merge" }, /* @__PURE__ */ React.createElement("p", { className: "body-text" }, "Dit lijkt over hetzelfde werk te gaan als ", /* @__PURE__ */ React.createElement("strong", null, merge2.target ? merge2.target.titel : "een recente stap"), ". Samenvoegen houdt je bouwlog kort."), /* @__PURE__ */ React.createElement("div", { className: "preview-step" }, /* @__PURE__ */ React.createElement("span", { className: "tl-title" }, merge2.r.titel), /* @__PURE__ */ React.createElement("span", { className: "tl-text" }, merge2.r.tekst)), /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, busy, onClick: () => doMerge(true) }, "Samenvoegen"), /* @__PURE__ */ React.createElement(Btn, { kind: "secondary", block: true, disabled: busy, onClick: () => doMerge(false) }, "Als nieuwe stap bewaren")) : /* @__PURE__ */ React.createElement(React.Fragment, null, !existing && !manual && /* @__PURE__ */ React.createElement(Field, { label: "Wat heb je gedaan?", id: "step-notitie", hint: "Schrijf het gewoon op. De assistent maakt er één korte regel van." }, /* @__PURE__ */ React.createElement("textarea", { id: "step-notitie", "data-autofocus": true, className: "input", rows: 5, value: f.notitie, onChange: set("notitie"), placeholder: "Bijv. vandaag de oude kabelboom eruit gehaald en de nieuwe ECU gemonteerd. Morgen verder met de sensoren." })), manual && /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement(Field, { label: "Titel", id: "step-titel" }, /* @__PURE__ */ React.createElement("input", { id: "step-titel", className: "input", value: f.titel, onChange: set("titel"), maxLength: 60, placeholder: "Bijv. Kabelboom vanaf nul" })), /* @__PURE__ */ React.createElement(Field, { label: "Korte uitleg", id: "step-tekst" }, /* @__PURE__ */ React.createElement("textarea", { id: "step-tekst", className: "input", rows: 3, value: f.tekst, onChange: set("tekst"), maxLength: 240, placeholder: "Eén of twee zinnen" }))), /* @__PURE__ */ React.createElement(Field, { label: "Datum", id: "step-datum" }, /* @__PURE__ */ React.createElement("input", { id: "step-datum", className: "input", type: "date", value: f.datum, onChange: set("datum") })), /* @__PURE__ */ React.createElement(Switch, { id: "step-mijlpaal", checked: f.mijlpaal, onChange: set("mijlpaal"), label: "Mijlpaal", sub: "Een belangrijke stap. Komt in de samenvatting en op de verkoopsamenvatting." }), /* @__PURE__ */ React.createElement(Switch, { id: "step-bezig", checked: f.bezig, onChange: set("bezig"), label: "Hier ben ik nu mee bezig", sub: "Staat bovenaan het paspoort tot je hem uitzet." }), existing && existing.origineel && /* @__PURE__ */ React.createElement("details", { className: "details" }, /* @__PURE__ */ React.createElement("summary", null, "Wat je zelf schreef"), /* @__PURE__ */ React.createElement("p", { className: "small muted pre" }, existing.origineel)), err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err), /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, onClick: submit, busy, icon: !existing && !manual ? "spark" : null }, busy && !existing && !manual ? "Inkorten…" : existing ? "Opslaan" : manual ? "Bewaar" : "Inkorten en bewaren"), busy && !manual && /* @__PURE__ */ React.createElement(Btn, { kind: "ghost", block: true, onClick: () => ctl.current && ctl.current.abort() }, "Stop"), !existing && !manual && /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn center", onClick: () => setManual(true) }, "Zelf een titel invullen"), existing && /* @__PURE__ */ React.createElement(Btn, { kind: "danger-ghost", block: true, icon: "trash", onClick: askDelete }, "Verwijderen"))
    );
  }
  function CarSelect({ value, onChange, id }) {
    const d = useData();
    return /* @__PURE__ */ React.createElement("select", { id, className: "input", value: value || "", onChange: (e) => onChange(e.target.value || null) }, d.active.map((c) => /* @__PURE__ */ React.createElement("option", { key: c.id, value: c.id }, c.naam || "Auto", c.kenteken ? " · " + fmtPlate(c.kenteken) : "")), d.previous.length > 0 && /* @__PURE__ */ React.createElement("optgroup", { label: "Eerder gehad" }, d.previous.map((c) => /* @__PURE__ */ React.createElement("option", { key: c.id, value: c.id }, c.naam || "Auto"))), /* @__PURE__ */ React.createElement("option", { value: "" }, "Niet aan een auto"));
  }
  function InvoiceSheet({ id, carId, linkPartId, linkOnderweg, onClose }) {
    const d = useData();
    const ui = useUi();
    const existing = id ? d.invoices.find((i) => i.id === id) : null;
    const linkPart = linkPartId ? d.parts.find((p) => p.id === linkPartId) : null;
    const linkedParts = existing ? d.parts.filter((p) => (p.facturen || []).includes(existing.id)) : [];
    const [f, set, setF] = useForm(() => ({
      carId: existing ? existing.carId || null : carId || d.active[0] && d.active[0].id || null,
      datum: existing ? existing.datum || "" : todayISO(),
      leverancier: existing ? existing.leverancier || "" : "",
      omschrijving: existing ? existing.omschrijving || "" : "",
      totaal: existing ? toInput(existing.totaal) : "",
      categorie: existing ? existing.categorie || "overig" : "onderhoud",
      factuurnummer: existing ? existing.factuurnummer || "" : "",
      notitie: existing ? existing.notitie || "" : ""
    }));
    const [file, setFile] = React.useState(null);
    const [err, setErr] = React.useState("");
    const [note, setNote] = React.useState("");
    const [saving, setSaving] = React.useState(false);
    const [showLines, setShowLines] = React.useState(false);
    async function pickFile(fl) {
      setFile(fl);
      setNote("");
      const isPdf = fl && (fl.type === "application/pdf" || /\.pdf$/i.test(fl.name || ""));
      if (!isPdf || existing) return;
      try {
        const pv = await pdfToImages(fl, { maxPages: 2, width: 400 });
        const g = parseInvoiceText(pv.text, d.cars);
        if (!g) return;
        setF((p) => ({
          ...p,
          totaal: p.totaal || (g.totaal != null ? toInput(g.totaal) : ""),
          datum: g.datum && (!p.datum || p.datum === todayISO()) ? g.datum : p.datum,
          leverancier: p.leverancier || g.leverancier || "",
          factuurnummer: p.factuurnummer || g.factuurnummer || "",
          categorie: g.categorie && p.categorie === "onderhoud" ? g.categorie : p.categorie,
          carId: !carId && g.carId ? g.carId : p.carId
        }));
        if (g.totaal != null || g.datum) setNote("Bedrag, datum en leverancier uit de pdf gehaald. Controleer ze.");
      } catch (e) {
      }
    }
    async function save(confirmSuggestion) {
      const totaal = parseNum(f.totaal);
      if (totaal === null) return setErr("Vul het bedrag in.");
      if (!f.omschrijving.trim() && !f.leverancier.trim()) return setErr("Vul een omschrijving of leverancier in.");
      setSaving(true);
      const data = {
        carId: f.carId || null,
        datum: f.datum || todayISO(),
        leverancier: f.leverancier.trim(),
        omschrijving: f.omschrijving.trim(),
        totaal,
        categorie: f.categorie,
        factuurnummer: f.factuurnummer.trim() || null,
        notitie: f.notitie.trim() || null
      };
      try {
        if (file) {
          if (file.type === "application/pdf") {
            const up = await d.act.upload(file, "application/pdf");
            let thumb = null;
            try {
              const pv = await pdfToImages(file, { maxPages: 1, width: 900 });
              if (pv.images[0]) thumb = (await d.act.upload(pv.images[0], "image/jpeg")).id;
            } catch (e) {
            }
            data.file = { id: up.id, type: "pdf", naam: file.name || null, thumb };
          } else {
            const blob = await downscale(file, 2200, 0.86);
            const up = await d.act.upload(blob, "image/jpeg");
            data.file = { id: up.id, type: "image", naam: file.name || null };
          }
          if (d.settings.driveSync) data.driveStatus = "open";
        }
      } catch (e) {
        setSaving(false);
        return setErr(e && e.code === "too_large" ? "Dit bestand is te groot (max 20 MB)." : "Het bestand kon niet worden opgeslagen.");
      }
      let ok;
      if (existing) {
        if (existing.carId !== data.carId) {
          if (existing.ai) data.ai = Object.assign({}, existing.ai, { gekozen: "zelf" });
          if (existing.driveStatus === "klaar") data.driveStatus = "verplaatsen";
        }
        if (confirmSuggestion) data.voorstel = false;
        ok = await d.act.patch("invoices", existing.id, data);
        if (ok && IS_TEST && (data.driveStatus === "open" || data.driveStatus === "verplaatsen")) d.act.queue({ type: "drive", invoiceId: existing.id });
        if (ok && data.file && existing.file) {
          d.act.deleteAsset(existing.file.id);
          d.act.deleteAsset(existing.file.thumb);
        }
      } else {
        const nid = await d.act.add("invoices", "f", Object.assign(data, { bron: "handmatig", driveStatus: data.driveStatus || (d.settings.driveSync ? "open" : null) }));
        ok = !!nid;
        if (nid && linkPartId) await linkInvoiceToPart(d, linkPartId, nid, data, linkOnderweg);
        if (nid && IS_TEST && data.file && data.driveStatus === "open") d.act.queue({ type: "drive", invoiceId: nid });
      }
      setSaving(false);
      if (ok) {
        onClose();
        ui.toast(existing ? "Opgeslagen" : linkPart ? "Factuur toegevoegd en gekoppeld aan " + (linkPart.naam || "het onderdeel") : "Kosten toegevoegd");
      }
    }
    function askDelete() {
      ui.openSheet("confirm", {
        title: "Factuur verwijderen",
        text: (existing.omschrijving || existing.leverancier || "Deze factuur") + " (" + eur(existing.totaal) + ") wordt verwijderd, met het bestand in de app. Een kopie in je Google Drive blijft staan.",
        confirmLabel: "Verwijderen",
        danger: true,
        onConfirm: async () => {
          const ok = await d.act.remove("invoices", existing.id);
          if (ok) for (const p of linkedParts) d.act.patch("parts", p.id, { facturen: (p.facturen || []).filter((x) => x !== existing.id) }, true);
          if (ok && existing.file) {
            d.act.deleteAsset(existing.file.id);
            d.act.deleteAsset(existing.file.thumb);
          }
          onClose();
        }
      });
    }
    const ef = existing && existing.file;
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: existing ? "Factuur" : "Kosten toevoegen",
        onClose,
        right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: () => save(false), disabled: saving }, "Bewaar")
      },
      linkPart && /* @__PURE__ */ React.createElement("p", { className: "small muted sheet-context" }, "Wordt gekoppeld aan ", linkPart.naam || "het onderdeel"),
      existing && existing.voorstel && /* @__PURE__ */ React.createElement("div", { className: "banner banner-info" }, /* @__PURE__ */ React.createElement(Icon, { name: "mail", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Voorgesteld uit je mail", existing.ai && existing.ai.reden ? ": " + existing.ai.reden : "", ". Klopt het?")),
      ef && /* @__PURE__ */ React.createElement("div", { className: "file-card" }, ef.type === "image" ? /* @__PURE__ */ React.createElement(Photo, { id: ef.id, className: "file-img" }) : ef.thumb ? /* @__PURE__ */ React.createElement(Photo, { id: ef.thumb, className: "file-img" }) : /* @__PURE__ */ React.createElement(Icon, { name: "doc", size: 32 }), IS_WEB ? /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "secondary", icon: "out", onClick: () => ui.openSheet("viewer", { id: ef.id, type: ef.type, naam: ef.naam }) }, ef.type === "pdf" ? "Pdf openen" : "Foto openen") : /* @__PURE__ */ React.createElement("a", { className: "btn btn-secondary btn-small", href: assetUrl(ef.id), target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement(Icon, { name: "out", size: 16 }), /* @__PURE__ */ React.createElement("span", null, ef.type === "pdf" ? "Pdf openen" : "Foto openen"))),
      /* @__PURE__ */ React.createElement(Field, { label: "Auto", id: "inv-car" }, /* @__PURE__ */ React.createElement(CarSelect, { id: "inv-car", value: f.carId, onChange: set("carId") })),
      /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: "Bedrag incl. btw", id: "inv-totaal" }, /* @__PURE__ */ React.createElement("input", { id: "inv-totaal", "data-autofocus": !existing ? true : void 0, className: "input", inputMode: "decimal", value: f.totaal, onChange: set("totaal"), placeholder: "€" })), /* @__PURE__ */ React.createElement(Field, { label: "Datum", id: "inv-datum" }, /* @__PURE__ */ React.createElement("input", { id: "inv-datum", className: "input", type: "date", value: f.datum, onChange: set("datum") }))),
      /* @__PURE__ */ React.createElement(Field, { label: "Omschrijving", id: "inv-oms" }, /* @__PURE__ */ React.createElement("input", { id: "inv-oms", className: "input", value: f.omschrijving, onChange: set("omschrijving"), placeholder: "Bijv. grote beurt" })),
      /* @__PURE__ */ React.createElement(Field, { label: "Leverancier", id: "inv-lev" }, /* @__PURE__ */ React.createElement("input", { id: "inv-lev", className: "input", value: f.leverancier, onChange: set("leverancier"), placeholder: "Garage of winkel" })),
      /* @__PURE__ */ React.createElement(Field, { label: "Categorie", id: "inv-cat" }, /* @__PURE__ */ React.createElement("select", { id: "inv-cat", className: "input", value: f.categorie, onChange: set("categorie") }, CATS.map(([v, l]) => /* @__PURE__ */ React.createElement("option", { key: v, value: v }, l)))),
      /* @__PURE__ */ React.createElement(Field, { label: "Factuurnummer", id: "inv-nr" }, /* @__PURE__ */ React.createElement("input", { id: "inv-nr", className: "input", value: f.factuurnummer, onChange: set("factuurnummer") })),
      /* @__PURE__ */ React.createElement(Field, { label: "Notitie", id: "inv-not" }, /* @__PURE__ */ React.createElement("textarea", { id: "inv-not", className: "input", rows: 2, value: f.notitie, onChange: set("notitie") })),
      existing && existing.regels && existing.regels.length > 0 && /* @__PURE__ */ React.createElement("div", { className: "lines" }, /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => setShowLines(!showLines), "aria-expanded": showLines }, existing.regels.length, " regels op de factuur"), showLines && /* @__PURE__ */ React.createElement("ul", { className: "line-list" }, existing.regels.map((r, i) => /* @__PURE__ */ React.createElement("li", { key: i }, /* @__PURE__ */ React.createElement("span", null, r.omschrijving), /* @__PURE__ */ React.createElement("span", { className: "num" }, typeof r.bedrag === "number" ? eur(r.bedrag) : ""))))),
      existing && existing.ai && existing.ai.gekozen === "assistent" && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Auto gekozen door de assistent · ", existing.ai.zekerheid, existing.ai.reden ? " · " + existing.ai.reden : ""),
      existing && existing.driveStatus && existing.driveStatus !== "klaar" && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, existing.driveStatus === "fout" ? "Naar Drive zetten lukte niet; de assistent probeert het opnieuw." : IS_TEST ? "Gaat zo naar Google Drive" : "Gaat bij de volgende ronde naar Google Drive"),
      existing && (existing.driveFileId || existing.mailId) && /* @__PURE__ */ React.createElement("div", { className: "link-list" }, existing.driveFileId && /* @__PURE__ */ React.createElement("a", { className: "link-row", href: "https://drive.google.com/file/d/" + existing.driveFileId + "/view", target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement(Icon, { name: "doc", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Factuur in Google Drive"), /* @__PURE__ */ React.createElement(Icon, { name: "out", size: 16 })), existing.driveFileId2 && /* @__PURE__ */ React.createElement("a", { className: "link-row", href: "https://drive.google.com/file/d/" + existing.driveFileId2 + "/view", target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement(Icon, { name: "doc", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "Tweede pdf in Google Drive"), /* @__PURE__ */ React.createElement(Icon, { name: "out", size: 16 })), existing.mailId && /* @__PURE__ */ React.createElement("a", { className: "link-row", href: "https://mail.google.com/mail/u/0/#all/" + existing.mailId, target: "_blank", rel: "noopener noreferrer" }, /* @__PURE__ */ React.createElement(Icon, { name: "mail", size: 18 }), /* @__PURE__ */ React.createElement("span", null, "E-mail openen"), /* @__PURE__ */ React.createElement(Icon, { name: "out", size: 16 }))),
      linkedParts.length > 0 && /* @__PURE__ */ React.createElement("div", { className: "link-list" }, /* @__PURE__ */ React.createElement("p", { className: "eyebrow" }, "Hoort bij"), linkedParts.map((p) => /* @__PURE__ */ React.createElement("button", { key: p.id, type: "button", className: "link-row", onClick: () => ui.openSheet("part", { id: p.id }) }, /* @__PURE__ */ React.createElement(Icon, { name: "tag", size: 18 }), /* @__PURE__ */ React.createElement("span", null, p.naam || "Onderdeel"), /* @__PURE__ */ React.createElement(Icon, { name: "next", size: 16 })))),
      note && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, note),
      d.caps.assets && /* @__PURE__ */ React.createElement(FilePick, { accept: "image/*,application/pdf", onFile: pickFile, className: "btn btn-secondary btn-block", id: "inv-file" }, /* @__PURE__ */ React.createElement(Icon, { name: "file", size: 18 }), /* @__PURE__ */ React.createElement("span", null, file ? clip(file.name || "Bestand gekozen", 30) : ef ? "Ander bestand kiezen" : "Foto of pdf toevoegen")),
      err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err),
      existing && existing.voorstel ? /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, busy: saving, onClick: () => save(true) }, "Klopt, bewaar") : /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, busy: saving, onClick: () => save(false) }, "Bewaar"),
      existing && /* @__PURE__ */ React.createElement(Btn, { kind: "danger-ghost", block: true, icon: "trash", onClick: askDelete }, existing.voorstel ? "Hoort er niet bij" : "Verwijderen")
    );
  }
  function ModSheet({ id, carId, onClose }) {
    const d = useData();
    const ui = useUi();
    const existing = id ? findIn(d, "modsOf", id) : null;
    const cid = existing ? existing.carId : carId;
    const [f, set] = useForm(() => ({ titel: existing ? existing.titel || "" : "", datum: existing ? (existing.datum || "").slice(0, 7) : todayISO().slice(0, 7), notitie: existing ? existing.notitie || "" : "" }));
    const [err, setErr] = React.useState("");
    async function save() {
      if (!f.titel.trim()) return setErr("Wat is er aangepast?");
      const data = { carId: cid, titel: f.titel.trim(), datum: f.datum ? f.datum.slice(0, 7) + "-01" : null, notitie: f.notitie.trim() || null };
      const ok = existing ? await d.act.patch("mods", existing.id, data) : !!await d.act.add("mods", "m", data);
      if (ok) onClose();
    }
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: "Aanpassing",
        onClose,
        right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: save }, "Bewaar")
      },
      /* @__PURE__ */ React.createElement(Field, { label: "Wat is er aangepast?", id: "mod-titel" }, /* @__PURE__ */ React.createElement("input", { id: "mod-titel", "data-autofocus": true, className: "input", value: f.titel, onChange: set("titel"), placeholder: "Bijv. schroefset 12/10 kg" })),
      /* @__PURE__ */ React.createElement(Field, { label: "Sinds", id: "mod-datum" }, /* @__PURE__ */ React.createElement("input", { id: "mod-datum", className: "input", type: "month", value: f.datum, onChange: set("datum") })),
      /* @__PURE__ */ React.createElement(Field, { label: "Notitie", id: "mod-notitie" }, /* @__PURE__ */ React.createElement("textarea", { id: "mod-notitie", className: "input", rows: 2, value: f.notitie, onChange: set("notitie"), placeholder: "Merk, maat of waar hij vandaan komt" })),
      err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err),
      /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, onClick: save }, "Bewaar"),
      existing && /* @__PURE__ */ React.createElement(
        Btn,
        {
          kind: "danger-ghost",
          block: true,
          icon: "trash",
          onClick: () => ui.openSheet("confirm", {
            title: "Aanpassing verwijderen",
            text: "“" + existing.titel + "” wordt verwijderd.",
            confirmLabel: "Verwijderen",
            danger: true,
            onConfirm: async () => {
              await d.act.remove("mods", existing.id);
              onClose();
            }
          })
        },
        "Verwijderen"
      )
    );
  }
  function findIn(d, fn, id) {
    const list = fn === "modsOf" ? d.mods : d.apkNotes;
    return (list || []).find((y) => y.id === id) || null;
  }
  function ApkSheet({ id, carId, soort, datum, onClose }) {
    const d = useData();
    const ui = useUi();
    const existing = id ? findIn(d, "apkNotesOf", id) : null;
    const cid = existing ? existing.carId : carId;
    const kind = existing ? existing.soort : soort || "bevinding";
    const [f, set] = useForm(() => ({
      datum: existing ? existing.datum : datum || todayISO(),
      resultaat: existing ? existing.resultaat || "goedgekeurd" : "goedgekeurd",
      km: existing && existing.km ? String(existing.km) : "",
      tekst: existing ? existing.tekst || "" : ""
    }));
    const [err, setErr] = React.useState("");
    async function save() {
      if (kind === "bevinding" && !f.tekst.trim()) return setErr("Wat kwam er naar voren?");
      if (!f.datum) return setErr("Vul de datum van de keuring in.");
      const data = { carId: cid, soort: kind, datum: f.datum, tekst: f.tekst.trim() || null };
      if (kind === "keuring") Object.assign(data, { resultaat: f.resultaat, km: parseNum(f.km) });
      const ok = existing ? await d.act.patch("apkNotes", existing.id, data) : !!await d.act.add("apkNotes", "a", data);
      if (ok) onClose();
    }
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: kind === "keuring" ? "Keuring" : "Eigen bevinding",
        onClose,
        right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: save }, "Bewaar")
      },
      /* @__PURE__ */ React.createElement(Field, { label: "Datum van de keuring", id: "apk-datum" }, /* @__PURE__ */ React.createElement("input", { id: "apk-datum", className: "input", type: "date", value: f.datum, onChange: set("datum") })),
      kind === "keuring" && /* @__PURE__ */ React.createElement(React.Fragment, null, /* @__PURE__ */ React.createElement(Field, { label: "Uitslag" }, /* @__PURE__ */ React.createElement(
        Seg,
        {
          label: "Uitslag",
          options: [
            ["goedgekeurd", "Goedgekeurd"],
            ["afgekeurd", "Afgekeurd"],
            ["herkeuring", "Herkeuring"]
          ],
          value: f.resultaat,
          onChange: set("resultaat")
        }
      )), /* @__PURE__ */ React.createElement(Field, { label: "Km-stand bij de keuring", id: "apk-km" }, /* @__PURE__ */ React.createElement("input", { id: "apk-km", className: "input", inputMode: "numeric", value: f.km, onChange: set("km") }))),
      /* @__PURE__ */ React.createElement(Field, { label: kind === "keuring" ? "Opmerkingen" : "Wat kwam er naar voren?", id: "apk-tekst", hint: kind === "bevinding" ? "Bijv. een advies of gebrek dat niet in het register staat." : null }, /* @__PURE__ */ React.createElement("textarea", { id: "apk-tekst", "data-autofocus": true, className: "input", rows: 3, value: f.tekst, onChange: set("tekst") })),
      err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err),
      /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, onClick: save }, "Bewaar"),
      existing && /* @__PURE__ */ React.createElement(
        Btn,
        {
          kind: "danger-ghost",
          block: true,
          icon: "trash",
          onClick: () => ui.openSheet("confirm", {
            title: "Verwijderen",
            text: "Deze notitie bij de keuring van " + fmtDate(existing.datum) + " wordt verwijderd.",
            confirmLabel: "Verwijderen",
            danger: true,
            onConfirm: async () => {
              await d.act.remove("apkNotes", existing.id);
              onClose();
            }
          })
        },
        "Verwijderen"
      )
    );
  }
  function SpecSheet({ carId, onClose }) {
    const d = useData();
    const car = d.car(carId);
    const [f, setF] = React.useState(() => {
      const s = car && car.specs || {};
      return Object.fromEntries(SPECS.map(([k]) => [k, { fabriek: s[k] && s[k].fabriek || "", nu: s[k] && s[k].nu || "", notitie: s[k] && s[k].notitie || "" }]));
    });
    const upd = (k, field) => (e) => {
      const v = e.target.value;
      setF((p) => ({ ...p, [k]: { ...p[k], [field]: v } }));
    };
    async function save() {
      const specs = {};
      for (const [k] of SPECS) {
        const prev = (car.specs || {})[k] || {};
        specs[k] = { fabriek: f[k].fabriek.trim() || null, nu: f[k].nu.trim() || null, notitie: f[k].notitie.trim() || null, bron: f[k].fabriek.trim() !== (prev.fabriek || "") ? "eigen" : prev.bron || null };
      }
      const ok = await d.act.patch("cars", carId, { specs });
      if (ok) onClose();
    }
    if (!car) return null;
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: "Prestaties",
        onClose,
        right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: save }, "Bewaar")
      },
      /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Fabriek is de fabrieksopgave. Vul bij Nu in wat er veranderd is, bijvoorbeeld een gemeten 0–100-tijd."),
      SPECS.map(([k, l]) => /* @__PURE__ */ React.createElement("fieldset", { className: "spec-edit", key: k }, /* @__PURE__ */ React.createElement("legend", null, l), /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: "Fabriek", id: "spec-" + k + "-f" }, /* @__PURE__ */ React.createElement("input", { id: "spec-" + k + "-f", className: "input", value: f[k].fabriek, onChange: upd(k, "fabriek") })), /* @__PURE__ */ React.createElement(Field, { label: "Nu", id: "spec-" + k + "-n" }, /* @__PURE__ */ React.createElement("input", { id: "spec-" + k + "-n", className: "input", value: f[k].nu, onChange: upd(k, "nu") }))), f[k].nu && /* @__PURE__ */ React.createElement(Field, { label: "Toelichting", id: "spec-" + k + "-t" }, /* @__PURE__ */ React.createElement("input", { id: "spec-" + k + "-t", className: "input", value: f[k].notitie, onChange: upd(k, "notitie"), placeholder: "Bijv. gemeten met GPS, nov 2026" })))),
      /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, onClick: save }, "Bewaar")
    );
  }
  function BudgetSheet({ carId, onClose }) {
    const d = useData();
    const car = d.car(carId);
    const [f, set] = useForm(() => ({ planNaam: car && car.planNaam || "", budget: car ? toInput(car.budget) : "" }));
    async function save() {
      const ok = await d.act.patch("cars", carId, { planNaam: f.planNaam.trim() || null, budget: parseNum(f.budget) });
      if (ok) onClose();
    }
    return /* @__PURE__ */ React.createElement(
      Sheet,
      {
        title: "Plan en budget",
        onClose,
        right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: save }, "Bewaar")
      },
      /* @__PURE__ */ React.createElement(Field, { label: "Naam van het plan", id: "plan-naam", hint: "Bijv. de ombouw of het project waar deze lijst voor is." }, /* @__PURE__ */ React.createElement("input", { id: "plan-naam", "data-autofocus": true, className: "input", value: f.planNaam, onChange: set("planNaam"), placeholder: "Plannen" })),
      /* @__PURE__ */ React.createElement(Field, { label: "Budget", id: "plan-budget", hint: "Leeg laten als je geen budget wilt bijhouden." }, /* @__PURE__ */ React.createElement("input", { id: "plan-budget", className: "input", inputMode: "decimal", value: f.budget, onChange: set("budget"), placeholder: "€" })),
      /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, onClick: save }, "Bewaar")
    );
  }
  function ConfirmSheet({ title, text: text2, confirmLabel, danger, onConfirm, onClose }) {
    const [busy, setBusy] = React.useState(false);
    return /* @__PURE__ */ React.createElement(Sheet, { title, onClose, size: "small" }, /* @__PURE__ */ React.createElement("p", { className: "body-text" }, text2), /* @__PURE__ */ React.createElement(
      Btn,
      {
        kind: danger ? "danger" : "primary",
        block: true,
        busy,
        onClick: async () => {
          setBusy(true);
          try {
            await onConfirm();
          } finally {
            setBusy(false);
          }
          onClose();
        }
      },
      confirmLabel || "Doorgaan"
    ), /* @__PURE__ */ React.createElement(Btn, { kind: "secondary", block: true, onClick: onClose, disabled: busy }, "Annuleer"));
  }
  function MenuSheet({ title, items, onClose }) {
    return /* @__PURE__ */ React.createElement(Sheet, { title, onClose, left: null, right: /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: onClose }, "Klaar") }, /* @__PURE__ */ React.createElement("div", { className: "list-card" }, items.map(([icon, label, fn]) => /* @__PURE__ */ React.createElement(
      "button",
      {
        type: "button",
        key: label,
        className: "list-row",
        onClick: () => {
          onClose();
          setTimeout(fn, 0);
        }
      },
      /* @__PURE__ */ React.createElement(Icon, { name: icon, size: 20 }),
      /* @__PURE__ */ React.createElement("span", { className: "list-row-main" }, /* @__PURE__ */ React.createElement("span", { className: "list-row-title" }, label)),
      /* @__PURE__ */ React.createElement(Icon, { name: "next", size: 18, className: "chev" })
    ))));
  }

  // src/screens/scan.jsx
  var empty = () => ({ leverancier: "", datum: todayISO(), totaal: "", omschrijving: "", categorie: "onderhoud", factuurnummer: "", km: "", kmBijwerken: false, valuta: "EUR" });
  function ScanSheet({ carId: presetCar, linkPartId, linkOnderweg, onClose }) {
    const d = useData();
    const ui = useUi();
    const [step, setStep] = React.useState("kies");
    const [src, setSrc] = React.useState(null);
    const [form, setForm] = React.useState(empty);
    const [ai, setAi] = React.useState(null);
    const startChoice = () => presetCar || (d.cars.length === 1 ? d.cars[0].id : ui.ai.sample && !ui.ai.off ? "assistent" : null);
    const [choice, setChoice] = React.useState(startChoice);
    const [err, setErr] = React.useState("");
    const [busy, setBusy] = React.useState(false);
    const [saved, setSaved] = React.useState(null);
    const [showLines, setShowLines] = React.useState(false);
    const [drag, setDrag] = React.useState(false);
    const [note, setNote] = React.useState("");
    const [guess, setGuess] = React.useState(null);
    const ctl = React.useRef(null);
    const aiOn = !!(ui.ai.sample && !ui.ai.off);
    const imgLimits = ui.ai.limits && ui.ai.limits.images;
    React.useEffect(() => () => ctl.current && ctl.current.abort(), []);
    const setF = (k) => (e) => {
      const v = e && e.target ? e.target.type === "checkbox" ? e.target.checked : e.target.value : e;
      setForm((p) => ({ ...p, [k]: v }));
    };
    function reset() {
      setStep("kies");
      setSrc(null);
      setForm(empty());
      setAi(null);
      setErr("");
      setSaved(null);
      setNote("");
      setGuess(null);
      setChoice(startChoice());
    }
    async function pick(file) {
      setErr("");
      const isPdf = file.type === "application/pdf" || /\.pdf$/i.test(file.name || "");
      if (!isPdf && file.type && !file.type.startsWith("image/")) {
        setErr("Kies een foto of een pdf.");
        return;
      }
      setStep("lezen");
      try {
        let s;
        if (isPdf) {
          const max = imgLimits ? Math.max(1, Math.min(3, imgLimits.maxCount || 1)) : 1;
          const pv = await pdfToImages(file, { maxPages: max, width: 1600 });
          s = { kind: "pdf", file, images: pv.images, text: pv.text, preview: pv.images[0] ? URL.createObjectURL(pv.images[0]) : null, pages: pv.pages };
        } else {
          const blob = await downscale(file, 2200, 0.86);
          s = { kind: "image", file, blob, images: [blob], preview: URL.createObjectURL(blob) };
        }
        setSrc(s);
        await read(s);
      } catch (e) {
        setStep("kies");
        setErr("Dit bestand kon niet worden geopend. Kies een foto (jpg, png) of een pdf.");
      }
    }
    async function read(s) {
      if (!aiOn || !imgLimits) {
        setAi(null);
        const g = s.text ? parseInvoiceText(s.text, d.cars) : null;
        const f0 = empty();
        if (g) Object.assign(f0, { leverancier: g.leverancier || "", datum: g.datum || f0.datum, totaal: g.totaal != null ? toInput(g.totaal) : "", factuurnummer: g.factuurnummer || "", categorie: g.categorie || f0.categorie, valuta: g.valuta || "EUR" });
        setForm(f0);
        setGuess(g);
        if (g && g.carId && !presetCar) setChoice(g.carId);
        setErr("");
        const found = g && (g.totaal != null || g.datum);
        setNote(found ? "Gegevens uit de pdf gehaald. Controleer ze voor je bewaart." : ui.ai.disabled ? "Vul de gegevens zelf in." : aiOn ? "Foto’s lezen kan in deze weergave niet. Vul de gegevens zelf in." : "De assistent is hier niet beschikbaar. Vul de gegevens zelf in.");
        setStep("controle");
        return;
      }
      ctl.current = new AbortController();
      try {
        const maxBytes = imgLimits.maxInputBytes || 5e6;
        const imgs = s.images.filter((b) => b && b.size <= maxBytes).slice(0, Math.max(1, imgLimits.maxCount || 1));
        if (!imgs.length) throw { code: "image_rejected" };
        const r = await readInvoice(ui.ai.sample, { images: imgs, pdfText: s.text, d, signal: ctl.current.signal });
        setAi(r);
        setForm({
          leverancier: r.leverancier || "",
          datum: r.datum || todayISO(),
          totaal: toInput(r.totaal),
          omschrijving: r.omschrijving || "",
          categorie: r.categorie || "overig",
          factuurnummer: r.factuurnummer || "",
          km: r.km ? String(Math.round(r.km)) : "",
          kmBijwerken: !!r.km,
          valuta: r.valuta || "EUR"
        });
        if (!r.leesbaar) setErr("De assistent kon deze factuur niet goed lezen. Controleer alles of maak een scherpere foto.");
        if (!presetCar && r.auto && r.auto.id && d.cars.length > 1) setChoice("assistent");
        setStep("controle");
      } catch (e) {
        if (e && e.code === "cancelled") {
          setStep("kies");
          return;
        }
        if (aiHidden(e)) ui.setAiOff();
        setAi(null);
        setForm(empty());
        setErr((aiErrorCopy(e) || "Lezen lukte niet.") + " Je kunt de gegevens ook zelf invullen.");
        setStep("controle");
      }
    }
    const carOfChoice = () => {
      if (!choice) return null;
      if (choice === "assistent") return ai && ai.auto ? ai.auto.id : null;
      if (choice === "geen") return null;
      return choice;
    };
    function toCar() {
      setErr("");
      if (parseNum(form.totaal) === null) return setErr("Vul het totaalbedrag in.");
      if (!form.leverancier.trim() && !form.omschrijving.trim()) return setErr("Vul de leverancier of een omschrijving in.");
      if (presetCar || d.cars.length === 0) return save();
      setStep("auto");
    }
    async function save() {
      setErr("");
      if (!choice && !presetCar && d.cars.length > 0) return setErr("Kies bij welke auto deze factuur hoort.");
      const totaal = parseNum(form.totaal);
      if (totaal === null) return setErr("Vul het totaalbedrag in.");
      setBusy(true);
      const carId = carOfChoice();
      let file = null;
      if (src && d.caps.assets) {
        try {
          if (src.kind === "pdf") {
            const up = await d.act.upload(src.file, "application/pdf");
            let thumb = null;
            if (src.images[0]) {
              try {
                thumb = (await d.act.upload(await downscale(src.images[0], 900, 0.8), "image/jpeg")).id;
              } catch (e) {
              }
            }
            file = { id: up.id, type: "pdf", naam: src.file.name || null, thumb };
          } else {
            const up = await d.act.upload(src.blob, "image/jpeg");
            file = { id: up.id, type: "image", naam: src.file.name || null };
          }
        } catch (e) {
          setBusy(false);
          return setErr(e && e.code === "too_large" ? "Dit bestand is te groot (max 20 MB)." : "Het bestand kon niet worden opgeslagen. Probeer het opnieuw.");
        }
      }
      const km = parseNum(form.km);
      const data = {
        carId: carId || null,
        datum: form.datum || todayISO(),
        leverancier: form.leverancier.trim(),
        omschrijving: form.omschrijving.trim(),
        totaal,
        btw: ai ? ai.btw : null,
        valuta: form.valuta && form.valuta !== "EUR" ? form.valuta : null,
        categorie: form.categorie,
        factuurnummer: form.factuurnummer.trim() || null,
        kenteken: ai && ai.kenteken ? normPlate(ai.kenteken) : guess && guess.kenteken ? guess.kenteken : null,
        km: km || null,
        regels: ai ? ai.regels : [],
        file,
        bron: "scan",
        ai: ai ? { zekerheid: ai.auto.zekerheid, reden: ai.auto.reden, voorstel: ai.auto.id, gekozen: choice === "assistent" ? "assistent" : "zelf" } : null,
        driveStatus: d.settings.driveSync && file ? "open" : null
      };
      const id = await d.act.add("invoices", "f", data);
      if (!id) {
        setBusy(false);
        return;
      }
      if (IS_TEST && data.driveStatus) d.act.queue({ type: "drive", invoiceId: id });
      if (linkPartId) await linkInvoiceToPart(d, linkPartId, id, data, linkOnderweg);
      if (carId && km && form.kmBijwerken) {
        const car = d.car(carId);
        if (car && (!car.km || km > car.km)) await d.act.patch("cars", carId, { km, kmDatum: data.datum }, true);
      }
      setBusy(false);
      setSaved({ id, carId: carId || null, totaal, leverancier: data.leverancier, categorie: data.categorie, datum: data.datum, drive: !!data.driveStatus });
      setStep("klaar");
    }
    const kentekenRaw = ai && ai.kenteken || guess && guess.kenteken || null;
    const kentekenCar = kentekenRaw ? d.cars.find((c) => c.kenteken && c.kenteken === normPlate(kentekenRaw)) : null;
    const linkPart = linkPartId ? d.parts.find((p) => p.id === linkPartId) : null;
    const suggested = ai && ai.auto && ai.auto.id ? d.car(ai.auto.id) : null;
    const twijfel = choice === "assistent" && ai && ai.auto && ai.auto.zekerheid === "gok" && d.settings.bijTwijfelVragen;
    const titles = { kies: "Factuur scannen", lezen: "Factuur lezen", controle: "Klopt dit?", auto: "Bij welke auto?", klaar: "Bewaard" };
    const left = step === "controle" || step === "auto" ? /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => step === "auto" ? setStep("controle") : reset() }, step === "auto" ? "Terug" : "Opnieuw") : void 0;
    return /* @__PURE__ */ React.createElement(Sheet, { title: titles[step], onClose, left, right: step === "klaar" ? /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn strong", onClick: onClose }, "Klaar") : null }, step === "kies" && /* @__PURE__ */ React.createElement(
      "div",
      {
        className: "scan-start" + (drag ? " is-drag" : ""),
        onDragOver: (e) => {
          e.preventDefault();
          setDrag(true);
        },
        onDragLeave: () => setDrag(false),
        onDrop: (e) => {
          e.preventDefault();
          setDrag(false);
          const f = e.dataTransfer.files && e.dataTransfer.files[0];
          if (f) pick(f);
        }
      },
      /* @__PURE__ */ React.createElement("div", { className: "scan-art", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement(Icon, { name: "file", size: 44, stroke: 1.4 })),
      /* @__PURE__ */ React.createElement("p", { className: "body-text center" }, presetCar && d.car(presetCar) ? "Voor " + (d.car(presetCar).naam || "je auto") + ". " : "", "Maak een foto van een papieren factuur of kies een pdf. ", aiOn ? "De assistent leest bedrag, datum en regels" + (presetCar ? "." : " en zoekt uit bij welke auto hij hoort.") : "Bij een pdf van een webshop vult de app bedrag en datum alvast in."),
      !ui.wide && /* @__PURE__ */ React.createElement(FilePick, { accept: "image/*", capture: "environment", onFile: pick, className: "btn btn-primary btn-block btn-large", id: "scan-camera" }, /* @__PURE__ */ React.createElement(Icon, { name: "camera", size: 22 }), /* @__PURE__ */ React.createElement("span", null, "Foto maken")),
      /* @__PURE__ */ React.createElement(FilePick, { accept: "image/*,application/pdf", onFile: pick, className: "btn btn-block " + (ui.wide ? "btn-primary btn-large" : "btn-secondary"), id: "scan-file" }, /* @__PURE__ */ React.createElement(Icon, { name: "image", size: 20 }), /* @__PURE__ */ React.createElement("span", null, ui.wide ? "Foto of pdf kiezen" : "Kies foto of pdf")),
      ui.wide && /* @__PURE__ */ React.createElement("p", { className: "small muted center" }, "Of sleep een bestand hierheen."),
      /* @__PURE__ */ React.createElement(
        "button",
        {
          type: "button",
          className: "link-btn center",
          onClick: () => {
            onClose();
            setTimeout(() => ui.openSheet("invoice", { carId: presetCar }), 0);
          }
        },
        "Zonder foto invoeren"
      ),
      err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err)
    ), step === "lezen" && /* @__PURE__ */ React.createElement("div", { className: "scan-reading" }, /* @__PURE__ */ React.createElement("div", { className: "scan-paper" }, src && src.preview ? /* @__PURE__ */ React.createElement("img", { src: src.preview, alt: "De gekozen factuur" }) : /* @__PURE__ */ React.createElement(Spinner, { size: 28 })), /* @__PURE__ */ React.createElement("p", { className: "body-text center thinking" }, /* @__PURE__ */ React.createElement(Spinner, null), " ", src && aiOn ? "De assistent leest de factuur…" : "Bestand openen…"), aiOn && /* @__PURE__ */ React.createElement("p", { className: "small muted center" }, "Dit duurt meestal 10 tot 40 seconden."), /* @__PURE__ */ React.createElement(Btn, { kind: "ghost", block: true, onClick: () => ctl.current && ctl.current.abort() }, "Stop")), step === "controle" && /* @__PURE__ */ React.createElement("div", { className: "stack" }, !presetCar && d.cars.length > 1 && /* @__PURE__ */ React.createElement("p", { className: "eyebrow" }, "Stap 1 van 2"), /* @__PURE__ */ React.createElement("div", { className: "scan-summary" }, src && src.preview && /* @__PURE__ */ React.createElement("img", { className: "scan-thumb", src: src.preview, alt: "" }), /* @__PURE__ */ React.createElement("div", { className: "scan-summary-text" }, /* @__PURE__ */ React.createElement("span", { className: "small muted" }, ai ? "Gelezen door de assistent" : guess && (guess.totaal != null || guess.datum) ? "Uit de pdf gehaald" : "Zelf invullen"), /* @__PURE__ */ React.createElement("strong", { className: "scan-supplier" }, form.leverancier || "Leverancier"), /* @__PURE__ */ React.createElement("span", { className: "scan-total" }, parseNum(form.totaal) !== null ? eur(parseNum(form.totaal)) : "€ –"))), err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err), note && !err && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, note), /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: "Totaal incl. btw", id: "scan-totaal" }, /* @__PURE__ */ React.createElement("input", { id: "scan-totaal", className: "input", inputMode: "decimal", value: form.totaal, onChange: setF("totaal"), placeholder: "€" })), /* @__PURE__ */ React.createElement(Field, { label: "Datum", id: "scan-datum" }, /* @__PURE__ */ React.createElement("input", { id: "scan-datum", className: "input", type: "date", value: form.datum, onChange: setF("datum") }))), form.valuta && form.valuta !== "EUR" && /* @__PURE__ */ React.createElement("p", { className: "small tone-warn" }, "Bedrag in ", form.valuta, ". Reken het om naar euro’s als je dat wilt."), /* @__PURE__ */ React.createElement(Field, { label: "Leverancier", id: "scan-lev" }, /* @__PURE__ */ React.createElement("input", { id: "scan-lev", className: "input", value: form.leverancier, onChange: setF("leverancier") })), /* @__PURE__ */ React.createElement(Field, { label: "Omschrijving", id: "scan-oms" }, /* @__PURE__ */ React.createElement("input", { id: "scan-oms", className: "input", value: form.omschrijving, onChange: setF("omschrijving"), placeholder: "Bijv. 4-wiel uitlijnen" })), /* @__PURE__ */ React.createElement(Field, { label: "Categorie", id: "scan-cat" }, /* @__PURE__ */ React.createElement("select", { id: "scan-cat", className: "input", value: form.categorie, onChange: setF("categorie") }, CATS.map(([v, l]) => /* @__PURE__ */ React.createElement("option", { key: v, value: v }, l)))), /* @__PURE__ */ React.createElement(Field, { label: "Factuurnummer", id: "scan-nr" }, /* @__PURE__ */ React.createElement("input", { id: "scan-nr", className: "input", value: form.factuurnummer, onChange: setF("factuurnummer") })), kentekenRaw && /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Kenteken op de bon: ", /* @__PURE__ */ React.createElement("strong", null, kentekenRaw), kentekenCar ? " · herkend als " + (kentekenCar.naam || "je auto") : ""), form.km !== "" && /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: "Kilometerstand", id: "scan-km" }, /* @__PURE__ */ React.createElement("input", { id: "scan-km", className: "input", inputMode: "numeric", value: form.km, onChange: setF("km") })), /* @__PURE__ */ React.createElement("label", { className: "check-row", htmlFor: "scan-kmup" }, /* @__PURE__ */ React.createElement("input", { id: "scan-kmup", type: "checkbox", checked: form.kmBijwerken, onChange: setF("kmBijwerken") }), /* @__PURE__ */ React.createElement("span", null, "Km-stand van de auto bijwerken"))), ai && ai.regels && ai.regels.length > 0 && /* @__PURE__ */ React.createElement("div", { className: "lines" }, /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", "aria-expanded": showLines, onClick: () => setShowLines(!showLines) }, ai.regels.length, " regels op de factuur"), showLines && /* @__PURE__ */ React.createElement("ul", { className: "line-list" }, ai.regels.map((r, i) => /* @__PURE__ */ React.createElement("li", { key: i }, /* @__PURE__ */ React.createElement("span", null, r.omschrijving), /* @__PURE__ */ React.createElement("span", { className: "num" }, typeof r.bedrag === "number" ? eur(r.bedrag) : ""))))), /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, busy, onClick: toCar }, presetCar ? "Bewaar bij " + ((d.car(presetCar) || {}).naam || "deze auto") : d.cars.length ? "Volgende: bij welke auto?" : "Bewaar")), step === "auto" && /* @__PURE__ */ React.createElement("div", { className: "stack" }, /* @__PURE__ */ React.createElement("p", { className: "eyebrow" }, "Stap 2 van 2"), /* @__PURE__ */ React.createElement("p", { className: "body-text" }, "In welk paspoort moet deze factuur komen?"), /* @__PURE__ */ React.createElement("div", { className: "choice-list", role: "radiogroup", "aria-label": "Auto" }, ai && /* @__PURE__ */ React.createElement("button", { type: "button", role: "radio", "aria-checked": choice === "assistent", className: "choice" + (choice === "assistent" ? " is-on" : ""), onClick: () => setChoice("assistent") }, /* @__PURE__ */ React.createElement("span", { className: "choice-icon accent", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement(Icon, { name: "spark", size: 20 })), /* @__PURE__ */ React.createElement("span", { className: "choice-main" }, /* @__PURE__ */ React.createElement("span", { className: "choice-title" }, "Laat de assistent kiezen"), /* @__PURE__ */ React.createElement("span", { className: "choice-sub" }, suggested ? suggested.naam || "Je auto" : "Geen van je auto’s", " · ", /* @__PURE__ */ React.createElement("span", { className: "zek zek-" + ai.auto.zekerheid }, ai.auto.zekerheid), ai.auto.reden ? " · " + ai.auto.reden : "")), /* @__PURE__ */ React.createElement("span", { className: "radio", "aria-hidden": "true" })), twijfel && /* @__PURE__ */ React.createElement("p", { className: "small tone-warn" }, "De assistent twijfelt. Kies liever zelf de auto."), /* @__PURE__ */ React.createElement("p", { className: "eyebrow" }, ai ? "Of kies zelf" : "Kies de auto"), d.active.concat(d.previous).map((c) => /* @__PURE__ */ React.createElement("button", { type: "button", key: c.id, role: "radio", "aria-checked": choice === c.id, className: "choice" + (choice === c.id ? " is-on" : ""), onClick: () => setChoice(c.id) }, /* @__PURE__ */ React.createElement(CarThumb, { car: c, size: 40 }), /* @__PURE__ */ React.createElement("span", { className: "choice-main" }, /* @__PURE__ */ React.createElement("span", { className: "choice-title" }, c.naam || "Auto"), /* @__PURE__ */ React.createElement("span", { className: "choice-sub" }, c.status === "verkocht" ? "Eerder gehad" : [c.bouwjaar, c.ondertitel].filter(Boolean).join(" · ") || " ")), /* @__PURE__ */ React.createElement(Plate, { kenteken: c.kenteken, size: "sm" }), /* @__PURE__ */ React.createElement("span", { className: "radio", "aria-hidden": "true" }))), /* @__PURE__ */ React.createElement("button", { type: "button", role: "radio", "aria-checked": choice === "geen", className: "choice" + (choice === "geen" ? " is-on" : ""), onClick: () => setChoice("geen") }, /* @__PURE__ */ React.createElement("span", { className: "choice-icon", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement(Icon, { name: "close", size: 20 })), /* @__PURE__ */ React.createElement("span", { className: "choice-main" }, /* @__PURE__ */ React.createElement("span", { className: "choice-title" }, "Niet voor een van je auto’s"), /* @__PURE__ */ React.createElement("span", { className: "choice-sub" }, "Bijv. gereedschap of een auto van iemand anders")), /* @__PURE__ */ React.createElement("span", { className: "radio", "aria-hidden": "true" }))), err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err), /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, busy, onClick: save }, twijfel && suggested ? "Toch bewaren bij " + (suggested.naam || "deze auto") : "Bewaar in paspoort")), step === "klaar" && saved && /* @__PURE__ */ React.createElement("div", { className: "stack center-stack" }, /* @__PURE__ */ React.createElement("span", { className: "done-icon", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement(Icon, { name: "check", size: 30, stroke: 2.6 })), /* @__PURE__ */ React.createElement("h3", { className: "h2" }, saved.carId ? "Bewaard bij " + ((d.car(saved.carId) || {}).naam || "je auto") : "Bewaard zonder auto"), /* @__PURE__ */ React.createElement("p", { className: "body-text center" }, [saved.leverancier, fmtDate(saved.datum), eur(saved.totaal)].filter(Boolean).join(" · "), /* @__PURE__ */ React.createElement("br", null), /* @__PURE__ */ React.createElement("span", { className: "muted" }, catLabel(saved.categorie))), linkPart && /* @__PURE__ */ React.createElement("p", { className: "small muted center" }, "Gekoppeld aan ", linkPart.naam || "het onderdeel", ", dat nu op gekocht staat."), saved.drive && /* @__PURE__ */ React.createElement("p", { className: "small muted center" }, IS_TEST ? "De assistent zet hem zo in je Drive-map." : "Gaat bij de volgende ronde van de assistent naar je Google Drive."), /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, icon: "camera", onClick: reset }, "Nog een factuur"), saved.carId && /* @__PURE__ */ React.createElement(
      Btn,
      {
        kind: "secondary",
        block: true,
        onClick: () => {
          onClose();
          ui.navigate({ name: "car", carId: saved.carId, tab: "kosten" });
        }
      },
      "Naar het paspoort"
    )));
  }

  // src/screens/verkoop.jsx
  function VerkoopSheet({ carId, onClose }) {
    const d = useData();
    const ui = useUi();
    const car = d.car(carId);
    const saved = car && car.verkoop || {};
    const [secties, setSecties] = React.useState(() => Object.assign(defaultSections(), saved.secties || {}));
    const [vraagprijs, setVraagprijs] = React.useState(toInput(saved.vraagprijs));
    const [contact, setContact] = React.useState(saved.contact || "");
    const [tekst, setTekst] = React.useState(saved.tekst || "");
    const [writing, setWriting] = React.useState(false);
    const [building, setBuilding] = React.useState(false);
    const [pdf, setPdf] = React.useState(null);
    const [err, setErr] = React.useState("");
    const ctl = React.useRef(null);
    const aiOn = !!(ui.ai.sample && !ui.ai.off);
    React.useEffect(() => () => ctl.current && ctl.current.abort(), []);
    if (!car) return null;
    const has = {
      foto: !!car.foto,
      tekst: true,
      gegevens: true,
      aanpassingen: d.modsOf(carId).length > 0,
      kosten: d.invoicesOf(carId).length > 0,
      facturen: d.invoicesOf(carId).length > 0,
      apk: mergeApk(car, d.apkNotesOf(carId)).length > 0,
      bouw: d.stepsOf(carId).length > 0 || !!(car.samenvatting && car.samenvatting.tekst)
    };
    function remember2(extra) {
      const v = Object.assign({ secties, vraagprijs: parseNum(vraagprijs), contact: contact.trim() || null, tekst: tekst.trim() || null }, extra || {});
      d.act.patch("cars", carId, { verkoop: v }, true);
    }
    async function write2() {
      setWriting(true);
      setErr("");
      ctl.current = new AbortController();
      try {
        const t = await salesText(ui.ai.sample, { d, car, signal: ctl.current.signal });
        setTekst(t);
        remember2({ tekst: t });
        setPdf(null);
      } catch (e) {
        if (aiHidden(e)) ui.setAiOff();
        const m = aiErrorCopy(e);
        if (m) setErr(m);
      } finally {
        setWriting(false);
      }
    }
    async function make() {
      setBuilding(true);
      setErr("");
      remember2();
      try {
        const res = await buildSalesPdf(d, car, { secties, vraagprijs: parseNum(vraagprijs), contact: contact.trim(), tekst: tekst.trim() });
        let previews = [];
        try {
          previews = await renderPdfPages(res.bytes.slice(0), ui.wide ? 520 : Math.min(window.innerWidth - 48, 520));
        } catch (e) {
          previews = [];
        }
        setPdf({ bytes: res.bytes, pages: res.pages, previews });
      } catch (e) {
        setErr("De pdf kon niet worden gemaakt. Controleer je verbinding en probeer het opnieuw.");
      } finally {
        setBuilding(false);
      }
    }
    async function download() {
      if (!d.caps.downloads) return setErr("Downloaden kan in deze weergave niet.");
      const name = ("Verkoopsamenvatting " + (car.naam || "auto") + " " + todayISO()).replace(/[\\/:*?"<>|]+/g, " ");
      try {
        await d.caps.downloads.save({ filename: name + ".pdf", data: new Blob([pdf.bytes], { type: "application/pdf" }) });
        ui.toast("Pdf opgeslagen");
      } catch (e) {
        if (e && e.code === "declined") return;
        setErr(e && e.code === "rate_limited" ? "Er staat al een download klaar. Rond die eerst af." : "Downloaden lukte niet in deze weergave.");
      }
    }
    return /* @__PURE__ */ React.createElement(Sheet, { title: "Verkoopsamenvatting", onClose, size: "wide" }, !pdf ? /* @__PURE__ */ React.createElement("div", { className: "stack" }, /* @__PURE__ */ React.createElement("p", { className: "body-text" }, "Het paspoort van ", car.naam || "je auto", " als nette A4 voor een koper. Bij een lang bouwproces worden het meer pagina’s."), /* @__PURE__ */ React.createElement("div", { className: "list-card switch-list" }, SALE_SECTIONS.map(([k, l, sub]) => /* @__PURE__ */ React.createElement(
      Switch,
      {
        key: k,
        id: "sale-sw-" + k,
        checked: secties[k] && has[k],
        disabled: !has[k],
        onChange: (v) => {
          setSecties((p) => ({ ...p, [k]: v }));
        },
        label: l,
        sub: has[k] ? sub : k === "foto" ? "Nog geen foto" : "Nog niets ingevuld"
      }
    ))), secties.tekst && /* @__PURE__ */ React.createElement(Field, { label: "Verkooptekst", id: "sale-tekst" }, /* @__PURE__ */ React.createElement("textarea", { id: "sale-tekst", className: "input", rows: 6, value: tekst, onChange: (e) => setTekst(e.target.value), placeholder: "Een paar zinnen over de auto: staat, onderhoud, wat er recent gedaan is." }), aiOn ? /* @__PURE__ */ React.createElement(Btn, { small: true, kind: "secondary", icon: "spark", busy: writing, onClick: write2 }, tekst ? "Opnieuw laten schrijven" : "Laat de assistent schrijven") : /* @__PURE__ */ React.createElement(
      Btn,
      {
        small: true,
        kind: "secondary",
        icon: "file",
        onClick: () => {
          const t = localSalesText(d, car);
          setTekst(t);
          remember2({ tekst: t });
          setPdf(null);
        }
      },
      tekst ? "Opnieuw invullen uit je gegevens" : "Invullen uit je gegevens"
    )), /* @__PURE__ */ React.createElement("div", { className: "field-row" }, /* @__PURE__ */ React.createElement(Field, { label: "Vraagprijs", id: "sale-prijs" }, /* @__PURE__ */ React.createElement("input", { id: "sale-prijs", className: "input", inputMode: "decimal", value: vraagprijs, onChange: (e) => setVraagprijs(e.target.value), placeholder: "Optioneel" })), /* @__PURE__ */ React.createElement(Field, { label: "Contact", id: "sale-contact" }, /* @__PURE__ */ React.createElement("input", { id: "sale-contact", className: "input", value: contact, onChange: (e) => setContact(e.target.value), placeholder: "Naam en telefoon" }))), err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err), /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, icon: "doc", busy: building, onClick: make }, "Pdf maken")) : /* @__PURE__ */ React.createElement("div", { className: "stack" }, /* @__PURE__ */ React.createElement("div", { className: "row-between" }, /* @__PURE__ */ React.createElement("span", { className: "body-text" }, pdf.pages, " ", pdf.pages === 1 ? "pagina" : "pagina’s", " A4"), /* @__PURE__ */ React.createElement("button", { type: "button", className: "link-btn", onClick: () => setPdf(null) }, "Aanpassen")), pdf.previews.length > 0 ? /* @__PURE__ */ React.createElement("div", { className: "pdf-pages" }, pdf.previews.map((u, i) => /* @__PURE__ */ React.createElement("img", { key: i, className: "pdf-page", src: u, alt: "Pagina " + (i + 1) }))) : /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Voorbeeld kon niet worden getoond; de pdf zelf is wel klaar."), err && /* @__PURE__ */ React.createElement("p", { className: "form-error" }, err), d.caps.downloads ? /* @__PURE__ */ React.createElement(Btn, { kind: "primary", block: true, icon: "download", onClick: download }, "Pdf downloaden") : /* @__PURE__ */ React.createElement("p", { className: "small muted" }, "Downloaden kan in deze weergave niet."), /* @__PURE__ */ React.createElement("p", { className: "small muted center" }, "Printen of delen doe je vanuit de gedownloade pdf.")));
  }

  // src/main.jsx
  var SHEETS = {
    car: CarSheet,
    part: PartSheet,
    step: StepSheet,
    invoice: InvoiceSheet,
    mod: ModSheet,
    apk: ApkSheet,
    specs: SpecSheet,
    budget: BudgetSheet,
    confirm: ConfirmSheet,
    menu: MenuSheet,
    scan: ScanSheet,
    verkoop: VerkoopSheet,
    bought: BoughtSheet
  };
  function App() {
    const caps = useCaps();
    return /* @__PURE__ */ React.createElement(ToastHost, null, /* @__PURE__ */ React.createElement(Boot, { caps }));
  }
  function Boot({ caps }) {
    const toast = useToast();
    if (!caps.booted) return /* @__PURE__ */ React.createElement(Loading, { text: "Garage openen…" });
    if (!caps.db) return /* @__PURE__ */ React.createElement(NoStore, null);
    return /* @__PURE__ */ React.createElement(DataProvider, { caps, toast }, /* @__PURE__ */ React.createElement(Shell, null));
  }
  function Loading({ text: text2 }) {
    return /* @__PURE__ */ React.createElement("div", { className: "loading" }, /* @__PURE__ */ React.createElement("span", { className: "brand-plate", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement("span", null)), /* @__PURE__ */ React.createElement(Spinner, { size: 22 }), /* @__PURE__ */ React.createElement("span", { className: "muted" }, text2));
  }
  function NoStore() {
    if (IS_WEB) {
      return /* @__PURE__ */ React.createElement("div", { className: "loading" }, /* @__PURE__ */ React.createElement(CarShape, { width: 120 }), /* @__PURE__ */ React.createElement("h1", { className: "h2" }, "Autopaspoort"), /* @__PURE__ */ React.createElement("p", { className: "body-text center muted", style: { maxWidth: 380 } }, "Deze browser laat de app niets bewaren. Open Autopaspoort in Safari, Chrome of Edge, en niet in een privévenster. Staat opslaan voor websites uit? Zet het dan aan in de instellingen van je browser."), /* @__PURE__ */ React.createElement(Btn, { kind: "secondary", icon: "refresh", onClick: () => window.location.reload() }, "Opnieuw proberen"));
    }
    return /* @__PURE__ */ React.createElement("div", { className: "loading" }, /* @__PURE__ */ React.createElement(CarShape, { width: 120 }), /* @__PURE__ */ React.createElement("h1", { className: "h2" }, "Mijn Autopaspoort"), /* @__PURE__ */ React.createElement("p", { className: "body-text center muted", style: { maxWidth: 360 } }, "Open deze app via claude.ai of de Claude-app. Daar staan je auto’s, facturen en plannen."));
  }
  function Shell() {
    const d = useData();
    const toast = useToast();
    const wide = useMedia("(min-width: 960px)");
    const [route, setRoute] = React.useState(() => parseHash(window.location.hash));
    const [sheets, setSheets] = React.useState([]);
    const [busy, setBusyState] = React.useState({});
    const [aiOff, setAiOff] = React.useState(false);
    const busyRef = React.useRef({});
    const dataRef = React.useRef(d);
    dataRef.current = d;
    const mainRef = React.useRef(null);
    useRdwAutoUpdate(d);
    React.useEffect(() => {
      const onHash = () => setRoute(parseHash(window.location.hash));
      window.addEventListener("hashchange", onHash);
      return () => window.removeEventListener("hashchange", onHash);
    }, []);
    React.useEffect(() => {
      if (mainRef.current) mainRef.current.scrollTop = 0;
    }, [route.name, route.carId, route.tab]);
    React.useEffect(() => {
      document.body.classList.toggle("has-sheet", sheets.length > 0);
    }, [sheets.length]);
    const ui = React.useMemo(() => {
      const navigate = (r) => {
        setRoute(r);
        const h = routeHash(r);
        if (window.location.hash !== h) {
          try {
            window.location.hash = h;
          } catch (e) {
          }
        }
      };
      return {
        wide,
        route,
        navigate,
        toast,
        openSheet: (type, props) => setSheets((s) => s.concat({ key: Math.random().toString(36).slice(2), type, props: props || {} })),
        closeSheet: () => setSheets((s) => s.slice(0, -1)),
        busy,
        isBusy: (k) => !!busyRef.current[k],
        setBusy: (k, v) => {
          busyRef.current = { ...busyRef.current, [k]: v };
          setBusyState(busyRef.current);
        },
        latest: () => dataRef.current,
        ai: { sample: d.aiEnabled ? d.caps.sample || null : null, limits: d.caps.sampleLimits || null, off: aiOff, disabled: !d.aiEnabled },
        setAiOff: () => setAiOff(true)
      };
    }, [wide, route, toast, busy, aiOff, d.caps.sample, d.caps.sampleLimits, d.aiEnabled]);
    let screen;
    if (!d.ready) screen = /* @__PURE__ */ React.createElement(Loading, { text: "Gegevens laden…" });
    else if (route.name === "car") screen = /* @__PURE__ */ React.createElement(CarPage, { carId: route.carId, tab: route.tab });
    else if (route.name === "plannen") screen = /* @__PURE__ */ React.createElement(PlannenAll, null);
    else if (route.name === "kosten") screen = /* @__PURE__ */ React.createElement(KostenAll, null);
    else if (route.name === "meer") screen = /* @__PURE__ */ React.createElement(Meer, null);
    else screen = /* @__PURE__ */ React.createElement(Garage, null);
    return /* @__PURE__ */ React.createElement(UiCtx.Provider, { value: ui }, /* @__PURE__ */ React.createElement("div", { className: "shell" + (wide ? " is-wide" : "") }, wide && /* @__PURE__ */ React.createElement(Sidebar, null), /* @__PURE__ */ React.createElement("main", { className: "main", ref: mainRef, id: "main" }, d.dbError && d.dbError.code === "revoked" ? /* @__PURE__ */ React.createElement("div", { className: "banner banner-warn" }, "De app heeft geen toegang meer tot je gegevens. Herlaad de pagina.") : null, screen), !wide && /* @__PURE__ */ React.createElement(TabBar, null), sheets.map((s, i) => {
      const C = SHEETS[s.type] || EXTRA.sheets[s.type];
      if (!C) return null;
      return /* @__PURE__ */ React.createElement("div", { key: s.key, className: "sheet-layer", style: { zIndex: 50 + i }, hidden: false }, /* @__PURE__ */ React.createElement(C, { ...s.props, onClose: () => setSheets((x) => x.filter((y) => y.key !== s.key)) }));
    })));
  }
  function TabBar() {
    const ui = useUi();
    const d = useData();
    const r = ui.route;
    const deals = d.openAlerts.filter((a) => a.soort === "korting").length;
    const item = (name, label, icon, badge) => /* @__PURE__ */ React.createElement(
      "a",
      {
        href: "#" + name,
        className: "tabbar-item" + (r.name === name || name === "garage" && r.name === "car" ? " is-on" : ""),
        "aria-current": r.name === name ? "page" : void 0,
        onClick: (e) => {
          e.preventDefault();
          ui.navigate({ name });
        }
      },
      /* @__PURE__ */ React.createElement("span", { className: "tabbar-icon" }, /* @__PURE__ */ React.createElement(Icon, { name: icon, size: 24, stroke: 1.8 }), badge ? /* @__PURE__ */ React.createElement("span", { className: "badge" }, badge) : null),
      /* @__PURE__ */ React.createElement("span", null, label)
    );
    return /* @__PURE__ */ React.createElement("nav", { className: "tabbar", "aria-label": "Hoofdmenu" }, item("garage", "Garage", "home"), item("plannen", "Plannen", "list", deals), /* @__PURE__ */ React.createElement(
      "button",
      {
        type: "button",
        className: "tabbar-item tabbar-scan",
        "aria-label": "Factuur scannen",
        onClick: () => d.cars.length ? ui.openSheet("scan", {}) : ui.openSheet("car", {})
      },
      /* @__PURE__ */ React.createElement("span", { className: "scan-fab" }, /* @__PURE__ */ React.createElement(Icon, { name: "camera", size: 26, stroke: 2 })),
      /* @__PURE__ */ React.createElement("span", null, "Scan")
    ), item("kosten", "Kosten", "chart", d.suggestions.length), item("meer", "Meer", "dots"));
  }
  function Sidebar() {
    const ui = useUi();
    const d = useData();
    const r = ui.route;
    const deals = d.openAlerts.filter((a) => a.soort === "korting").length;
    const nav = (name, label, icon, badge) => /* @__PURE__ */ React.createElement(
      "a",
      {
        href: "#" + name,
        className: "side-link" + (r.name === name ? " is-on" : ""),
        "aria-current": r.name === name ? "page" : void 0,
        onClick: (e) => {
          e.preventDefault();
          ui.navigate({ name });
        }
      },
      /* @__PURE__ */ React.createElement(Icon, { name: icon, size: 20 }),
      /* @__PURE__ */ React.createElement("span", null, label),
      badge ? /* @__PURE__ */ React.createElement("span", { className: "badge inline" }, badge) : null
    );
    return /* @__PURE__ */ React.createElement("aside", { className: "sidebar", "aria-label": "Navigatie" }, /* @__PURE__ */ React.createElement(
      "a",
      {
        className: "brand",
        href: "#garage",
        onClick: (e) => {
          e.preventDefault();
          ui.navigate({ name: "garage" });
        }
      },
      /* @__PURE__ */ React.createElement("span", { className: "brand-plate", "aria-hidden": "true" }, /* @__PURE__ */ React.createElement("span", null)),
      /* @__PURE__ */ React.createElement("span", { className: "brand-name" }, "Autopaspoort"),
      (IS_TEST || IS_NOAI) && !IS_WEB && /* @__PURE__ */ React.createElement("span", { className: "test-tag" }, IS_NOAI ? "Zonder AI" : "Test")
    ), /* @__PURE__ */ React.createElement(Btn, { kind: "primary", icon: "file", block: true, onClick: () => d.cars.length ? ui.openSheet("scan", {}) : ui.openSheet("car", {}) }, "Factuur toevoegen"), /* @__PURE__ */ React.createElement("nav", { "aria-label": "Garage", className: "side-group" }, /* @__PURE__ */ React.createElement("span", { className: "side-label" }, "Garage"), d.active.map((c) => /* @__PURE__ */ React.createElement(
      "a",
      {
        key: c.id,
        href: "#auto-" + c.id,
        className: "side-car" + (r.name === "car" && r.carId === c.id ? " is-on" : ""),
        "aria-current": r.name === "car" && r.carId === c.id ? "page" : void 0,
        onClick: (e) => {
          e.preventDefault();
          ui.navigate({ name: "car", carId: c.id });
        }
      },
      /* @__PURE__ */ React.createElement("span", { className: "side-car-name" }, c.naam || "Nieuwe auto"),
      /* @__PURE__ */ React.createElement(Plate, { kenteken: c.kenteken, size: "xs" })
    )), /* @__PURE__ */ React.createElement("button", { type: "button", className: "side-add", onClick: () => ui.openSheet("car", {}) }, /* @__PURE__ */ React.createElement(Icon, { name: "plus", size: 16, stroke: 2.4 }), "Auto toevoegen"), d.previous.length > 0 && /* @__PURE__ */ React.createElement("span", { className: "side-label" }, "Eerder gehad"), d.previous.map((c) => /* @__PURE__ */ React.createElement(
      "a",
      {
        key: c.id,
        href: "#auto-" + c.id,
        className: "side-prev" + (r.name === "car" && r.carId === c.id ? " is-on" : ""),
        onClick: (e) => {
          e.preventDefault();
          ui.navigate({ name: "car", carId: c.id });
        }
      },
      c.naam || "Auto"
    ))), /* @__PURE__ */ React.createElement("nav", { "aria-label": "Overal", className: "side-group" }, /* @__PURE__ */ React.createElement("span", { className: "side-label" }, "Overal"), nav("garage", "Overzicht", "home"), nav("plannen", "Plannen", "list", deals), nav("kosten", "Kosten", "chart", d.suggestions.length), nav("meer", d.aiEnabled ? "Koppelingen en meer" : "Meer", "dots")));
  }
  ReactDOM.createRoot(document.getElementById("root")).render(/* @__PURE__ */ React.createElement(App, null));
})();
