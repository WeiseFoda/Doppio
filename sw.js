/* Doppio Service Worker — cache, push, notificações mobile */
const CACHE = 'doppio-v11'; // ao mudar a versão, o 'activate' apaga os caches antigos (inclusive qualquer conteúdo privado que versões anteriores tenham guardado)
const ASSETS = ['./', './Doppio.html', './voice.js', './manifest.webmanifest', './icon.svg'];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE).then(cache => cache.addAll(ASSETS).catch(() => {})).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const req = event.request;
  if (req.method !== 'GET') return;
  // Mesma normalização do servidor (decodifica e ignora barras vazias): "/%66iles/x", "/files//x" e "/%61pi/..." chegam às mesmas rotas.
  let segs; try { segs = new URL(req.url).pathname.split('/').filter(Boolean).map(decodeURIComponent); } catch { return; }
  if (['api', 'files', 'ws'].includes(segs[0])) return; // dados do servidor (privados) nunca vêm do cache
  event.respondWith(
    caches.match(req).then(cached => {
      const net = fetch(req).then(res => {
        if (res && res.ok && req.url.startsWith(self.location.origin)) {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(req, clone)).catch(() => {});
        }
        return res;
      }).catch(() => cached);
      return cached || net;
    })
  );
});

self.addEventListener('push', event => {
  let data = { title: 'Doppio', body: 'Nova notificação', tag: 'doppio-push' };
  try {
    if (event.data) {
      const json = event.data.json();
      data = { ...data, ...json };
    } else if (event.data) {
      data.body = event.data.text();
    }
  } catch {
    try { data.body = event.data.text(); } catch {}
  }
  event.waitUntil(
    self.registration.showNotification(data.title || 'Doppio', {
      body: data.body || '',
      icon: data.icon || undefined,
      badge: data.badge || undefined,
      tag: data.tag || 'doppio-push',
      renotify: true,
      data: data.data || data,
      vibrate: data.vibrate || [120, 60, 120],
      requireInteraction: !!data.requireInteraction,
      actions: data.actions || [
        { action: 'open', title: 'Abrir' },
        { action: 'dismiss', title: 'Dispensar' }
      ]
    })
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  if (event.action === 'dismiss') return;
  const url = (event.notification.data && event.notification.data.url) || './Doppio.html';
  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
      for (const client of list) {
        if (client.url.includes('Doppio') || client.url.includes('Discord2')) {
          client.postMessage({ type: 'notification-click', data: event.notification.data });
          return client.focus();
        }
      }
      if (self.clients.openWindow) return self.clients.openWindow(url);
    })
  );
});

self.addEventListener('notificationclose', () => {});

self.addEventListener('message', event => {
  const msg = event.data || {};
  if (msg.type === 'notify') {
    event.waitUntil(
      self.registration.showNotification(msg.title || 'Doppio', {
        body: msg.body || '',
        tag: msg.tag || 'doppio',
        renotify: true,
        vibrate: [100, 50, 100],
        data: msg.data || {}
      })
    );
  }
  if (msg.type === 'skipWaiting') self.skipWaiting();
});
