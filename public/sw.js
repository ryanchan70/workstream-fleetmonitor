// Minimal service worker for notifications only.
//
// It deliberately does NOT cache anything. Its whole reason to exist is that
// two mobile platforms refuse notifications without one:
//
//   Chrome on Android throws on `new Notification(...)` and only allows
//   ServiceWorkerRegistration.showNotification(). The dashboard already had
//   that fallback path, but it called getRegistration() when nothing was ever
//   registered, so it always failed through to the "in-page alerts are being
//   used instead" message.
//
//   Safari on iOS exposes the Notification API only to a PWA installed to the
//   Home Screen (iOS 16.4+), and installation requires a manifest and a
//   service worker.
//
// A caching service worker would also happily serve a stale dashboard after a
// deploy, which is not a trade worth making here.

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));

self.addEventListener('notificationclick', (event) => {
    event.notification.close();
    event.waitUntil(
        self.clients.matchAll({ type: 'window', includeUncontrolled: true })
            .then((tabs) => {
                for (const tab of tabs) {
                    if ('focus' in tab) return tab.focus();
                }
                return self.clients.openWindow ? self.clients.openWindow('/') : undefined;
            })
    );
});
