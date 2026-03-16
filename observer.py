#!/usr/bin/env python3
"""
Observador del pipeline dlt Facebook Ads.
Muestra si el pipeline está ejecutándose y el resultado de la última carrera.

Uso:
  python observer.py           # Estado actual
  python observer.py --serve   # Servidor HTTP en puerto 8080
  python observer.py --watch   # Actualización cada 5 segundos
"""
import argparse
import json
import os
import sys
from pathlib import Path


def _status_path():
    return Path(os.environ.get("PIPELINE_STATUS_PATH", "/app/.pipeline_status.json"))


def get_status():
    """Lee el estado actual del pipeline."""
    p = _status_path()
    if not p.exists():
        return {"running": False, "status": "unknown", "message": "Nunca ejecutado o sin estado"}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data
    except Exception as e:
        return {"running": False, "status": "error", "message": str(e)}


def print_status():
    """Imprime el estado de forma legible."""
    s = get_status()
    running = s.get("running", False)

    print()
    print("=" * 50)
    print("  PIPELINE dlt Facebook Ads - Observador")
    print("=" * 50)
    print()

    if running:
        print("  Estado:   EJECUTÁNDOSE")
        print(f"  Inicio:   {s.get('started_at', '?')}")
        print(f"  Clientes: {', '.join(s.get('clients', []) or ['?'])}")
        print(f"  Actual:   {s.get('current_client', '-') or '-'}")
    else:
        print("  Estado:   INACTIVO")
        print(f"  Último:   {s.get('status', '?')}")
        if s.get("duration_seconds") is not None:
            mins = s["duration_seconds"] // 60
            secs = s["duration_seconds"] % 60
            print(f"  Duración: {mins}m {secs}s")
        if s.get("error"):
            print(f"  Error:    {s['error']}")
        print(f"  Actualizado: {s.get('updated_at', '?')}")

    print()
    print("=" * 50)
    print()


def serve_http(port: int = 8080):
    """Servidor HTTP mínimo que devuelve el estado como JSON."""
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
    except ImportError:
        print("Error: se requiere Python estándar", file=sys.stderr)
        sys.exit(1)

    class StatusHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/" or self.path == "/status":
                s = get_status()
                body = json.dumps(s, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", len(body))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # Silenciar logs

    print(f"Observador HTTP en http://0.0.0.0:{port}/status")
    print("Ctrl+C para detener")
    server = HTTPServer(("0.0.0.0", port), StatusHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDetenido.")
        server.shutdown()


def watch(interval: int = 5):
    """Muestra el estado y actualiza cada N segundos."""
    import time
    while True:
        try:
            print("\033[2J\033[H", end="")  # Clear screen
            print_status()
            print(f"Actualizando cada {interval}s... Ctrl+C para salir")
            time.sleep(interval)
        except KeyboardInterrupt:
            print("\nDetenido.")
            break


def main():
    parser = argparse.ArgumentParser(description="Observador del pipeline dlt Facebook Ads")
    parser.add_argument("--serve", action="store_true", help="Iniciar servidor HTTP en puerto 8080")
    parser.add_argument("--port", type=int, default=8080, help="Puerto para --serve")
    parser.add_argument("--watch", action="store_true", help="Actualizar estado cada 5 segundos")
    parser.add_argument("--interval", type=int, default=5, help="Intervalo en segundos para --watch")
    parser.add_argument("--json", action="store_true", help="Salida JSON")
    args = parser.parse_args()

    if args.serve:
        serve_http(args.port)
    elif args.watch:
        watch(args.interval)
    elif args.json:
        print(json.dumps(get_status(), indent=2))
    else:
        print_status()


if __name__ == "__main__":
    main()
