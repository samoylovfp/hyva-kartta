var cacheName = 'hyka-pwa';
var filesToCache = [
  './',
  './index.html',
  './hyka.js',
  './hyka_bg.wasm',
];

/* Start the service worker and cache all of the app's content */
self.addEventListener('install', function (e) {
  console.log("Installed worker");
  e.waitUntil(
    caches.open(cacheName).then(function (cache) {
      return cache.addAll(filesToCache);
    })
  );
});

/* Serve cached content when offline */
self.addEventListener('fetch', function (e) {
  e.respondWith(
    caches.match(e.request).then(function (response) {
      return response || fetch(e.request);
    })
  );
});
