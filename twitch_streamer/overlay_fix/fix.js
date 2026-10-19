// Bambuddy's overlay replaces its camera <img> with a new MJPEG connection
// every minute, but the removed image keeps streaming. Chromium allows six
// connections per host, so after about six minutes every request the page
// makes (camera and print data alike) waits forever and the overlay freezes
// with a blank camera. Dropping a removed image's src closes its connection.
new MutationObserver((records) => {
  for (const record of records) {
    for (const node of record.removedNodes) {
      if (node.nodeType !== Node.ELEMENT_NODE) continue;
      const images = node.tagName === "IMG" ? [node] : node.querySelectorAll("img");
      for (const img of images) {
        if (!img.isConnected) img.removeAttribute("src");
      }
    }
  }
}).observe(document, { childList: true, subtree: true });
