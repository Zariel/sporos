#!/bin/sh
set -eu

mkdir -p \
  /downloads/qbittorrent \
  /downloads/rtorrent \
  /qbittorrent-config/qBittorrent \
  /rtorrent-data \
  /rtorrent-passwd

cp -R /fixtures/media/qbittorrent/. /downloads/qbittorrent/
cp -R /fixtures/media/rtorrent/. /downloads/rtorrent/
cp /templates/qBittorrent.conf /qbittorrent-config/qBittorrent/qBittorrent.conf

chown -R 1000:1000 \
  /downloads/qbittorrent \
  /downloads/rtorrent \
  /qbittorrent-config \
  /rtorrent-data \
  /rtorrent-passwd

chmod -R u+rwX,go+rX /downloads
chmod -R u+rwX,go-rwx /qbittorrent-config /rtorrent-data /rtorrent-passwd
